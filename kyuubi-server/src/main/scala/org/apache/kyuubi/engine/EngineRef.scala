/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine

import java.util.concurrent.{Semaphore, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Random

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_SUBMIT_TIME_KEY
import org.apache.kyuubi.engine.EngineType._
import org.apache.kyuubi.engine.ShareLevel.{CONNECTION, GROUP, SERVER, ShareLevel}
import org.apache.kyuubi.engine.chat.ChatProcessBuilder
import org.apache.kyuubi.engine.flink.FlinkProcessBuilder
import org.apache.kyuubi.engine.hive.HiveProcessBuilder
import org.apache.kyuubi.engine.jdbc.JdbcProcessBuilder
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.engine.trino.TrinoProcessBuilder
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ENGINE_REF_ID, HA_NAMESPACE}
import org.apache.kyuubi.ha.client.{DiscoveryClient, DiscoveryClientProvider, DiscoveryPaths, ServiceNodeInfo}
import org.apache.kyuubi.metrics.MetricsConstants.{ENGINE_FAIL, ENGINE_TIMEOUT, ENGINE_TOTAL}
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.plugin.GroupProvider
import org.apache.kyuubi.server.KyuubiServer

/**
 * The description and functionality of an engine at server side
 *
 * @param conf Engine configuration
 * @param sessionUser Caller of the engine
 * @param engineRefId Id of the corresponding session in which the engine is created
 */
private[kyuubi] class EngineRef(
    conf: KyuubiConf,
    sessionUser: String,
    doAsEnabled: Boolean,
    groupProvider: GroupProvider,
    engineRefId: String,
    engineManager: KyuubiApplicationManager,
    startupProcessSemaphore: Option[Semaphore] = None)
  extends Logging {
  // The corresponding ServerSpace where the engine belongs to
  private val serverSpace: String = conf.get(HA_NAMESPACE)

  private val timeout: Long = conf.get(ENGINE_INIT_TIMEOUT)

  // Share level of the engine
  private val shareLevel: ShareLevel = ShareLevel.withName(conf.get(ENGINE_SHARE_LEVEL))

  private val engineType: EngineType = EngineType.withName(conf.get(ENGINE_TYPE))

  // Server-side engine pool size threshold
  private val poolThreshold: Int = Option(KyuubiServer.kyuubiServer).map(_.getConf)
    .getOrElse(KyuubiConf()).get(ENGINE_POOL_SIZE_THRESHOLD)

  private val clientPoolSize: Int = conf.get(ENGINE_POOL_SIZE)

  private val clientPoolName: String = conf.get(ENGINE_POOL_NAME)

  private val enginePoolIgnoreSubdomain: Boolean = conf.get(ENGINE_POOL_IGNORE_SUBDOMAIN)

  private val enginePoolSelectPolicy: String = conf.get(ENGINE_POOL_SELECT_POLICY)

  // In case the multi kyuubi instances have the small gap of timeout, here we add
  // a small amount of time for timeout
  private val LOCK_TIMEOUT_SPAN_FACTOR = if (Utils.isTesting) 0.5 else 0.1

  private var builder: ProcBuilder = _

  private[kyuubi] def getEngineRefId: String = engineRefId

  // user for routing session to the engine
  private[kyuubi] val routingUser: String = shareLevel match {
    case SERVER => Utils.currentUser
    case GROUP => groupProvider.primaryGroup(sessionUser, conf.getAll.asJava)
    case _ => sessionUser
  }

  // user for launching engine
  private[kyuubi] val appUser: String = if (doAsEnabled) routingUser else Utils.currentUser

  @VisibleForTesting
  private[kyuubi] val subdomain: String = conf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN) match {
    case subdomain if clientPoolSize > 0 && (subdomain.isEmpty || enginePoolIgnoreSubdomain) =>
      val poolSize = math.min(clientPoolSize, poolThreshold)
      if (poolSize < clientPoolSize) {
        warn(s"Request engine pool size($clientPoolSize) exceeds, fallback to " +
          s"system threshold $poolThreshold")
      }
      val seqNum = enginePoolSelectPolicy match {
        case "POLLING" =>
          val snPath =
            DiscoveryPaths.makePath(
              s"${serverSpace}_${KYUUBI_VERSION}_${shareLevel}_${engineType}_seqNum",
              routingUser,
              clientPoolName)
          DiscoveryClientProvider.withDiscoveryClient(conf) { client =>
            client.getAndIncrement(snPath)
          }
        case "RANDOM" =>
          Random.nextInt(poolSize)
      }
      s"$clientPoolName-${seqNum % poolSize}"
    case Some(_subdomain) => _subdomain
    case _ => "default" // [KYUUBI #1293]
  }

  /**
   * The default engine name, used as default `spark.app.name` if not set
   */
  @VisibleForTesting
  private[kyuubi] val defaultEngineName: String = {
    val commonNamePrefix = s"kyuubi_${shareLevel}_${engineType}_${routingUser}"
    shareLevel match {
      case CONNECTION => s"${commonNamePrefix}_$engineRefId"
      case _ => s"${commonNamePrefix}_${subdomain}_$engineRefId"
    }
  }

  /**
   * The EngineSpace used to expose itself to the KyuubiServers in `serverSpace`
   *
   * For `CONNECTION` share level:
   *   /`serverSpace_version_CONNECTION_engineType`/`user`/`engineRefId`
   * For `USER` share level:
   *   /`serverSpace_version_USER_engineType`/`user`[/`subdomain`]
   * For `GROUP` share level:
   *   /`serverSpace_version_GROUP_engineType`/`primary group name`[/`subdomain`]
   * For `SERVER` share level:
   *   /`serverSpace_version_SERVER_engineType`/`kyuubi server user`[/`subdomain`]
   */
  @VisibleForTesting
  private[kyuubi] lazy val engineSpace: String = {
    val commonParent = s"${serverSpace}_${KYUUBI_VERSION}_${shareLevel}_$engineType"
    shareLevel match {
      case CONNECTION => DiscoveryPaths.makePath(commonParent, routingUser, engineRefId)
      case _ => DiscoveryPaths.makePath(commonParent, routingUser, subdomain)
    }
  }

  /**
   * The distributed lock path used to ensure only once engine being created for non-CONNECTION
   * share level.
   */
  private def tryWithLock[T](discoveryClient: DiscoveryClient)(f: => T): T =
    shareLevel match {
      case CONNECTION => f
      case _ =>
        val lockPath =
          DiscoveryPaths.makePath(
            s"${serverSpace}_${KYUUBI_VERSION}_${shareLevel}_${engineType}_lock",
            routingUser,
            subdomain)
        discoveryClient.tryWithLock(
          lockPath,
          timeout + (LOCK_TIMEOUT_SPAN_FACTOR * timeout).toLong)(f)
    }

  private def create(
      discoveryClient: DiscoveryClient,
      extraEngineLog: Option[OperationLog]): (String, Int) = tryWithLock(discoveryClient) {
    // Get the engine address ahead if another process has succeeded
    // 获取引擎服务
    var engineRef = discoveryClient.getServerHost(engineSpace)
    // 如果不为空那么就直接返回能够获取的引擎信息
    if (engineRef.nonEmpty) return engineRef.get
    // 配置HA_NAMESPACE, 高可用的根目录
    conf.set(HA_NAMESPACE, engineSpace)
    // 配置HA_ENGINE_REF_ID, 高可用的引用ID
    conf.set(HA_ENGINE_REF_ID, engineRefId)
    // 开启时间
    val started = System.currentTimeMillis()
    // 引擎提交时间
    conf.set(KYUUBI_ENGINE_SUBMIT_TIME_KEY, String.valueOf(started))
    // 构建引擎类型
    builder = engineType match {
      // Spark
      case SPARK_SQL =>
        conf.setIfMissing(SparkProcessBuilder.APP_KEY, defaultEngineName)
        new SparkProcessBuilder(appUser, doAsEnabled, conf, engineRefId, extraEngineLog)
      // Flink
      case FLINK_SQL =>
        conf.setIfMissing(FlinkProcessBuilder.APP_KEY, defaultEngineName)
        new FlinkProcessBuilder(appUser, doAsEnabled, conf, engineRefId, extraEngineLog)
      // TRINO
      case TRINO =>
        new TrinoProcessBuilder(appUser, doAsEnabled, conf, engineRefId, extraEngineLog)
      // HiveSQL
      case HIVE_SQL =>
        conf.setIfMissing(HiveProcessBuilder.HIVE_ENGINE_NAME, defaultEngineName)
        // 构建一个Hive进程
        HiveProcessBuilder(
          appUser,
          doAsEnabled,
          conf,
          engineRefId,
          extraEngineLog,
          defaultEngineName)
      // JDBC
      case JDBC =>
        new JdbcProcessBuilder(appUser, doAsEnabled, conf, engineRefId, extraEngineLog)
      // CHAT
      case CHAT =>
        new ChatProcessBuilder(appUser, doAsEnabled, conf, engineRefId, extraEngineLog)
    }
    // 获取总共构建时间
    MetricsSystem.tracing(_.incCount(ENGINE_TOTAL))
    // 请求许可
    var acquiredPermit = false
    try {
      // 如果在指定的超时时间内无法获取所有许可, 增加一个指标计数器
      if (!startupProcessSemaphore.forall(_.tryAcquire(timeout, TimeUnit.MILLISECONDS))) {
        MetricsSystem.tracing(_.incCount(MetricRegistry.name(ENGINE_TIMEOUT, appUser)))
        throw KyuubiSQLException(
          s"Timeout($timeout ms, you can modify ${ENGINE_INIT_TIMEOUT.key} to change it) to" +
            s" acquires a permit from engine builder semaphore.")
      }
      // 请求许可通过
      acquiredPermit = true
      // 启动引擎
      val redactedCmd = builder.toString
      info(s"Launching engine:\n$redactedCmd")
      builder.validateConf()
      // 进行引擎的启动
      val process = builder.start
      // 退出值
      var exitValue: Option[Int] = None
      // 最后应用信息
      var lastApplicationInfo: Option[ApplicationInfo] = None
      // 如果引擎
      while (engineRef.isEmpty) {
        if (exitValue.isEmpty && process.waitFor(1, TimeUnit.SECONDS)) {
          exitValue = Some(process.exitValue())
          if (!exitValue.contains(0)) {
            val error = builder.getError
            MetricsSystem.tracing { ms =>
              ms.incCount(MetricRegistry.name(ENGINE_FAIL, appUser))
              ms.incCount(MetricRegistry.name(ENGINE_FAIL, error.getClass.getSimpleName))
            }
            throw error
          }
        }

        if (started + timeout <= System.currentTimeMillis()) {
          val killMessage =
            engineManager.killApplication(builder.appMgrInfo(), engineRefId, Some(appUser))
          builder.close(true)
          MetricsSystem.tracing(_.incCount(MetricRegistry.name(ENGINE_TIMEOUT, appUser)))
          throw KyuubiSQLException(
            s"Timeout($timeout ms, you can modify ${ENGINE_INIT_TIMEOUT.key} to change it) to" +
              s" launched $engineType engine with $redactedCmd. $killMessage",
            builder.getError)
        }
        engineRef = discoveryClient.getEngineByRefId(engineSpace, engineRefId)

        // even the submit process succeeds, the application might meet failure when initializing,
        // check the engine application state from engine manager and fast fail on engine terminate
        if (engineRef.isEmpty && exitValue.contains(0)) {
          Option(engineManager).foreach { engineMgr =>
            if (lastApplicationInfo.isDefined) {
              TimeUnit.SECONDS.sleep(1)
            }

            val applicationInfo = engineMgr.getApplicationInfo(
              builder.appMgrInfo(),
              engineRefId,
              Some(appUser),
              Some(started))

            applicationInfo.foreach { appInfo =>
              if (ApplicationState.isTerminated(appInfo.state)) {
                MetricsSystem.tracing { ms =>
                  ms.incCount(MetricRegistry.name(ENGINE_FAIL, appUser))
                  ms.incCount(MetricRegistry.name(ENGINE_FAIL, "ENGINE_TERMINATE"))
                }
                throw new KyuubiSQLException(
                  s"""
                     |The engine application has been terminated. Please check the engine log.
                     |ApplicationInfo: ${appInfo.toMap.mkString("(\n", ",\n", "\n)")}
                     |""".stripMargin,
                  builder.getError)
              }
            }

            lastApplicationInfo = applicationInfo
          }
        }
      }
      engineRef.get
    } finally {
      if (acquiredPermit) startupProcessSemaphore.foreach(_.release())
      val waitCompletion = conf.get(KyuubiConf.SESSION_ENGINE_STARTUP_WAIT_COMPLETION)
      val destroyProcess = !waitCompletion && builder.isClusterMode()
      if (destroyProcess) {
        info("Destroy the builder process because waitCompletion is false" +
          " and the engine is running in cluster mode.")
      }
      // we must close the process builder whether session open is success or failure since
      // we have a log capture thread in process builder.
      builder.close(destroyProcess)
    }
  }

  /**
   * 从引擎进程中获取一个引擎实例或者创建一个引擎实例
   * Get the engine ref from engine space first or create a new one
   *  用于获取或创建引擎实例的zookeeper客户端
   * @param discoveryClient the zookeeper client to get or create engine instance
   * @param extraEngineLog the launch engine operation log, used to inject engine log into it
   * @return engine host and port 引擎的host和port
   */
  def getOrCreate(
      discoveryClient: DiscoveryClient,
      extraEngineLog: Option[OperationLog] = None): (String, Int) = {
    // 这里如果为空就进行重新创建
    discoveryClient.getServerHost(engineSpace)
      .getOrElse {
        create(discoveryClient, extraEngineLog)
      }
  }

  /**
   * Deregister the engine from engine space with the given host and port on connection failure.
   *
   * @param discoveryClient the zookeeper client to get or create engine instance
   * @param hostPort the existing engine host and port
   * @return deregister result and message
   */
  def deregister(discoveryClient: DiscoveryClient, hostPort: (String, Int)): (Boolean, String) =
    tryWithLock(discoveryClient) {
      // refer the DiscoveryClient::getServerHost implementation
      discoveryClient.getServiceNodesInfo(engineSpace, Some(1), silent = true) match {
        case Seq(sn) =>
          if ((sn.host, sn.port) == hostPort) {
            val msg = s"Deleting engine node:$sn"
            info(msg)
            discoveryClient.delete(s"$engineSpace/${sn.nodeName}")
            (true, msg)
          } else {
            val msg = s"Engine node:$sn is not matched with host&port[$hostPort]"
            warn(msg)
            (false, msg)
          }
        case _ =>
          val msg = s"No engine node found in $engineSpace"
          warn(msg)
          (false, msg)
      }
    }

  def getServiceNode(
      discoveryClient: DiscoveryClient,
      hostPort: (String, Int)): Option[ServiceNodeInfo] = {
    val serviceNodes = discoveryClient.getServiceNodesInfo(engineSpace)
    serviceNodes.find { sn => (sn.host, sn.port) == hostPort }
  }

  def close(): Unit = {
    if (shareLevel == CONNECTION && builder != null) {
      try {
        val appMgrInfo = builder.appMgrInfo()
        builder.close(true)
        engineManager.killApplication(appMgrInfo, engineRefId, Some(appUser))
      } catch {
        case e: Exception =>
          warn(s"Error closing engine builder, engineRefId: $engineRefId", e)
      }
    }
  }
}

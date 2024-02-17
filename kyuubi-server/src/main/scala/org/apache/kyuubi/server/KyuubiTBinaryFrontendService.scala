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

package org.apache.kyuubi.server

import java.util.Base64

import org.apache.hadoop.conf.Configuration

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.cli.Handle
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.ha.client.{KyuubiServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.service.{Serverable, Service, TBinaryFrontendService}
import org.apache.kyuubi.service.TFrontendService.{CURRENT_SERVER_CONTEXT, FeServiceServerContext, OK_STATUS}
import org.apache.kyuubi.session.KyuubiSessionImpl
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.thrift.protocol.TProtocol
import org.apache.kyuubi.shaded.thrift.server.ServerContext

final class KyuubiTBinaryFrontendService(
    override val serverable: Serverable)
  extends TBinaryFrontendService("KyuubiTBinaryFrontend") {

  override protected def hadoopConf: Configuration = KyuubiServer.getHadoopConf()

  override lazy val discoveryService: Option[Service] = {
    if (ServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new KyuubiServiceDiscovery(this))
    } else {
      None
    }
  }

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    super.initialize(conf)

    server.foreach(_.setServerEventHandler(new FeTServerEventHandler() {
      override def createContext(input: TProtocol, output: TProtocol): ServerContext = {
        MetricsSystem.tracing { ms =>
          ms.incCount(THRIFT_BINARY_CONN_OPEN)
          ms.incCount(THRIFT_BINARY_CONN_TOTAL)
        }
        new FeServiceServerContext()
      }

      override def deleteContext(
          serverContext: ServerContext,
          input: TProtocol,
          output: TProtocol): Unit = {
        super.deleteContext(serverContext, input, output)
        MetricsSystem.tracing { ms =>
          ms.decCount(THRIFT_BINARY_CONN_OPEN)
        }
      }
    }))
  }

  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    debug(req.toString)
    info("Client protocol version: " + req.getClient_protocol)
    // 构建返回响应请求
    val resp = new TOpenSessionResp
    try {
      // 构建SessionHandle(协议版本, 会话用户, 密码, ip地址以及配置信息)
      val sessionHandle = getSessionHandle(req, resp)
      //  构建一个响应配置存储的HashMap
      val respConfiguration = new java.util.HashMap[String, String]()
      // 获取启动引擎操作
      // 通过sessionHandle获取Session
      // 将Session转换成KyuubiSessionImpl类型
      // 获取当中的launchEngineOp属性
      val launchEngineOp = be.sessionManager.getSession(sessionHandle)
        .asInstanceOf[KyuubiSessionImpl].launchEngineOp
      // operationHandler
      val opHandleIdentifier = Handle.toTHandleIdentifier(launchEngineOp.getHandle.identifier)
      // 添加新的响应配置
      // Kyuubi会话引擎句柄GUID
      respConfiguration.put(
        KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_GUID,
        Base64.getMimeEncoder.encodeToString(opHandleIdentifier.getGuid))
      // Kyuubi会话引擎句柄密钥
      respConfiguration.put(
        KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_SECRET,
        Base64.getMimeEncoder.encodeToString(opHandleIdentifier.getSecret))
      // Kyuubi会话引擎支持结果
      respConfiguration.put(KYUUBI_SESSION_ENGINE_LAUNCH_SUPPORT_RESULT, true.toString)
      // 设置SessionHandle
      resp.setSessionHandle(sessionHandle.toTSessionHandle)
      // 设置Reponse配置
      resp.setConfiguration(respConfiguration)
      // 设置状态, 这里是成功状态
      resp.setStatus(OK_STATUS)
      // 将SessionHandle设置为当前服务器的会话句柄
      Option(CURRENT_SERVER_CONTEXT.get()).foreach(_.setSessionHandle(sessionHandle))
    } catch {
      case e: Exception =>
        error("Error opening session: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e, verbose = true))
    }
    // 返回响应信息
    resp
  }

  override protected def isServer(): Boolean = true

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    debug(req.toString)
    val resp = new TRenewDelegationTokenResp
    resp.setStatus(notSupportTokenErrorStatus)
    resp
  }
}

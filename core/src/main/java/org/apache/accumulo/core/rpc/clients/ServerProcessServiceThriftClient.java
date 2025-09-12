/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.rpc.clients;

import java.io.UncheckedIOException;
import java.net.UnknownHostException;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.process.thrift.ServerProcessService.Client;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;

public class ServerProcessServiceThriftClient extends ThriftClientTypes<Client> {

  protected ServerProcessServiceThriftClient(String serviceName) {
    super(serviceName, new Client.Factory());
  }

  public Client getServerProcessConnection(ClientContext context, Logger log,
      ServerId serverProcess) {
    try {
      // Manager requests can take a long time: don't ever time out
      return ThriftUtil.getClientNoTimeout(this, serverProcess, context);
    } catch (TTransportException tte) {
      Throwable cause = tte.getCause();
      if (cause instanceof UnknownHostException) {
        // do not expect to recover from this
        throw new UncheckedIOException((UnknownHostException) cause);
      }
      log.debug("Failed to connect to process at " + serverProcess + ", will retry... ", tte);
      return null;
    }

  }
}

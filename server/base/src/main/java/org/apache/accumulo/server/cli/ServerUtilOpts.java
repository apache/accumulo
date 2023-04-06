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
package org.apache.accumulo.server.cli;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.server.ServerContext;

public class ServerUtilOpts extends ClientOpts {

  private ServerContext context;

  public synchronized ServerContext getServerContext() {
    if (context == null) {
      if (getClientConfigFile() == null) {
        context = new ServerContext(SiteConfiguration.auto());
      } else {
        ClientInfo info = ClientInfo.from(getClientProps());
        context = ServerContext.override(SiteConfiguration.auto(), info.getInstanceName(),
            info.getZooKeepers(), info.getZooKeepersSessionTimeOut());
      }
    }
    return context;
  }
}

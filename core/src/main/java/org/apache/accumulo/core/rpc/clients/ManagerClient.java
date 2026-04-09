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

import static com.google.common.base.Preconditions.checkArgument;

import java.net.UnknownHostException;
import java.util.Optional;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;

import com.google.common.net.HostAndPort;

public interface ManagerClient<C extends TServiceClient> {

  default C getManagerConnection(Logger log, ThriftClientTypes<C> type, ClientContext context) {
    checkArgument(context != null, "context is null");

    // obtain the primary manager location
    String managerLocation = null;
    ServiceLockPaths.ServiceLockPath m = context.getServerPaths().getManager(true);
    if (m != null) {
      Optional<ServiceLockData> sld = context.getZooCache().getLockData(m);
      if (sld.isPresent()) {
        managerLocation = sld.orElseThrow().getAddressString(ServiceLockData.ThriftService.MANAGER);
      }
    }

    if (managerLocation == null) {
      log.debug("No managers...");
      return null;
    }

    if (managerLocation.equals("0.0.0.0:0")) {
      // The Manager creates the lock with an initial address of 0.0.0.0:0, then
      // later updates the lock contents with the actual address after everything
      // is started.
      log.debug("Manager is up and lock acquired, waiting for address...");
      return null;
    }
    HostAndPort manager = HostAndPort.fromString(managerLocation);
    try {
      // Manager requests can take a long time: don't ever time out
      return ThriftUtil.getClientNoTimeout(type, manager, context);
    } catch (TTransportException tte) {
      Throwable cause = tte.getCause();
      if (cause != null && cause instanceof UnknownHostException) {
        // do not expect to recover from this
        throw new IllegalStateException(tte);
      }
      log.debug("Failed to connect to manager=" + manager + ", will retry... ", tte);
      return null;
    }
  }

}

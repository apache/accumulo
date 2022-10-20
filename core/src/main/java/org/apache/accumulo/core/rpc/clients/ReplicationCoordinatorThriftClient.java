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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator.Client;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationCoordinatorThriftClient extends ThriftClientTypes<Client> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicationCoordinatorThriftClient.class);

  ReplicationCoordinatorThriftClient(String serviceName) {
    super(serviceName, new Client.Factory());
  }

  @Override
  public Client getConnection(ClientContext context) {

    List<String> locations = context.getManagerLocations();

    if (locations.isEmpty()) {
      LOG.debug("No managers for replication to instance {}", context.getInstanceName());
      return null;
    }

    // This is the manager thrift service, we just want the hostname, not the port
    String managerThriftService = locations.get(0);
    if (managerThriftService.endsWith(":0")) {
      LOG.warn("Manager found for {} did not have real location {}", context.getInstanceName(),
          managerThriftService);
      return null;
    }

    String zkPath = context.getZooKeeperRoot() + Constants.ZMANAGER_REPLICATION_COORDINATOR_ADDR;
    String replCoordinatorAddr;

    LOG.debug("Using ZooKeeper quorum at {} with path {} to find peer Manager information",
        context.getZooKeepers(), zkPath);

    // Get the coordinator port for the manager we're trying to connect to
    try {
      ZooReader reader = context.getZooReader();
      replCoordinatorAddr = new String(reader.getData(zkPath), UTF_8);
    } catch (KeeperException | InterruptedException e) {
      LOG.error("Could not fetch remote coordinator port", e);
      return null;
    }

    // Throw the hostname and port through HostAndPort to get some normalization
    HostAndPort coordinatorAddr = HostAndPort.fromString(replCoordinatorAddr);

    LOG.debug("Connecting to manager at {}", coordinatorAddr);

    try {
      // Manager requests can take a long time: don't ever time out
      return ThriftUtil.getClientNoTimeout(ThriftClientTypes.REPLICATION_COORDINATOR,
          coordinatorAddr, context);
    } catch (TTransportException tte) {
      LOG.debug("Failed to connect to manager coordinator service ({})", coordinatorAddr, tte);
      return null;
    }
  }

  @Override
  public Client getConnectionWithRetry(ClientContext context) {
    requireNonNull(context);

    for (int attempts = 1; attempts <= 10; attempts++) {

      Client result = getConnection(context);
      if (result != null)
        return result;
      LOG.debug("Could not get ReplicationCoordinator connection to {}, will retry",
          context.getInstanceName());
      try {
        Thread.sleep(attempts * 250L);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    throw new RuntimeException(
        "Timed out trying to communicate with manager from " + context.getInstanceName());
  }

  @Override
  public <R> R execute(ClientContext context, Exec<R,Client> exec)
      throws AccumuloException, AccumuloSecurityException {
    Client client = null;
    for (int i = 0; i < 10; i++) {
      try {
        client = getConnectionWithRetry(context);
        return exec.execute(client);
      } catch (TTransportException tte) {
        LOG.debug("ReplicationClient coordinator request failed, retrying ... ", tte);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new AccumuloException(e);
        }
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          ThriftUtil.close(client, context);
      }
    }

    throw new AccumuloException(
        "Could not connect to ReplicationCoordinator at " + context.getInstanceName());
  }

}

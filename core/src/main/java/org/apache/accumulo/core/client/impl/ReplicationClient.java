/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.client.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationClient {
  private static final Logger log = LoggerFactory.getLogger(ReplicationClient.class);

  /**
   * @param context
   *          the client session for the peer replicant
   * @return Client to the ReplicationCoordinator service
   */
  public static ReplicationCoordinator.Client getCoordinatorConnectionWithRetry(ClientContext context) throws AccumuloException {
    requireNonNull(context);
    Instance instance = context.getInstance();

    for (int attempts = 1; attempts <= 10; attempts++) {

      ReplicationCoordinator.Client result = getCoordinatorConnection(context);
      if (result != null)
        return result;
      log.debug("Could not get ReplicationCoordinator connection to {}, will retry", instance.getInstanceName());
      try {
        Thread.sleep(attempts * 250);
      } catch (InterruptedException e) {
        throw new AccumuloException(e);
      }
    }

    throw new AccumuloException("Timed out trying to communicate with master from " + instance.getInstanceName());
  }

  public static ReplicationCoordinator.Client getCoordinatorConnection(ClientContext context) {
    Instance instance = context.getInstance();
    List<String> locations = instance.getMasterLocations();

    if (locations.size() == 0) {
      log.debug("No masters for replication to instance {}", instance.getInstanceName());
      return null;
    }

    // This is the master thrift service, we just want the hostname, not the port
    String masterThriftService = locations.get(0);
    if (masterThriftService.endsWith(":0")) {
      log.warn("Master found for {} did not have real location {}", instance.getInstanceName(), masterThriftService);
      return null;
    }

    String zkPath = ZooUtil.getRoot(instance) + Constants.ZMASTER_REPLICATION_COORDINATOR_ADDR;
    String replCoordinatorAddr;

    log.debug("Using ZooKeeper quorum at {} with path {} to find peer Master information", instance.getZooKeepers(), zkPath);

    // Get the coordinator port for the master we're trying to connect to
    try {
      ZooReader reader = new ZooReader(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
      replCoordinatorAddr = new String(reader.getData(zkPath, null), UTF_8);
    } catch (KeeperException | InterruptedException e) {
      log.error("Could not fetch remote coordinator port", e);
      return null;
    }

    // Throw the hostname and port through HostAndPort to get some normalization
    HostAndPort coordinatorAddr = HostAndPort.fromString(replCoordinatorAddr);

    log.debug("Connecting to master at {}", coordinatorAddr);

    try {
      // Master requests can take a long time: don't ever time out
      ReplicationCoordinator.Client client = ThriftUtil.getClientNoTimeout(new ReplicationCoordinator.Client.Factory(), coordinatorAddr, context);
      return client;
    } catch (TTransportException tte) {
      log.debug("Failed to connect to master coordinator service ({})", coordinatorAddr, tte);
      return null;
    }
  }

  /**
   * Attempt a single time to create a ReplicationServicer client to the given host
   *
   * @param context
   *          The client session for the peer replicant
   * @param server
   *          Server to connect to
   * @param timeout
   *          RPC timeout in milliseconds
   * @return A ReplicationServicer client to the given host in the given instance
   */
  public static ReplicationServicer.Client getServicerConnection(ClientContext context, HostAndPort server, long timeout) throws TTransportException {
    requireNonNull(context);
    requireNonNull(server);

    try {
      return ThriftUtil.getClient(new ReplicationServicer.Client.Factory(), server, context, timeout);
    } catch (TTransportException tte) {
      log.debug("Failed to connect to servicer ({}), will retry...", server, tte);
      throw tte;
    }
  }

  public static void close(ReplicationCoordinator.Iface iface) {
    TServiceClient client = (TServiceClient) iface;
    if (client != null && client.getInputProtocol() != null && client.getInputProtocol().getTransport() != null) {
      ThriftTransportPool.getInstance().returnTransport(client.getInputProtocol().getTransport());
    } else {
      log.debug("Attempt to close null connection to the remote system", new Exception());
    }
  }

  public static void close(ReplicationServicer.Iface iface) {
    TServiceClient client = (TServiceClient) iface;
    if (client != null && client.getInputProtocol() != null && client.getInputProtocol().getTransport() != null) {
      ThriftTransportPool.getInstance().returnTransport(client.getInputProtocol().getTransport());
    } else {
      log.debug("Attempt to close null connection to the remote system", new Exception());
    }
  }

  public static <T> T executeCoordinatorWithReturn(ClientContext context, ClientExecReturn<T,ReplicationCoordinator.Client> exec) throws AccumuloException,
      AccumuloSecurityException {
    ReplicationCoordinator.Client client = null;
    for (int i = 0; i < 10; i++) {
      try {
        client = getCoordinatorConnectionWithRetry(context);
        return exec.execute(client);
      } catch (TTransportException tte) {
        log.debug("ReplicationClient coordinator request failed, retrying ... ", tte);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new AccumuloException(e);
        }
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (AccumuloException e) {
        throw e;
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          close(client);
      }
    }

    throw new AccumuloException("Could not connect to ReplicationCoordinator at " + context.getInstance().getInstanceName());
  }

  public static <T> T executeServicerWithReturn(ClientContext context, HostAndPort tserver, ClientExecReturn<T,ReplicationServicer.Client> exec, long timeout)
      throws AccumuloException, AccumuloSecurityException, TTransportException {
    ReplicationServicer.Client client = null;
    while (true) {
      try {
        client = getServicerConnection(context, tserver, timeout);
        return exec.execute(client);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (AccumuloException e) {
        throw e;
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          close(client);
      }
    }
  }

}

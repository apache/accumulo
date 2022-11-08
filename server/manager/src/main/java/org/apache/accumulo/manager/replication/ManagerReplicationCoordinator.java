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
package org.apache.accumulo.manager.replication;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.SecureRandom;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinatorErrorCode;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinatorException;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Choose a tserver to service a replication task
 */
@Deprecated
public class ManagerReplicationCoordinator implements ReplicationCoordinator.Iface {
  private static final SecureRandom random = new SecureRandom();
  private static final Logger log = LoggerFactory.getLogger(ManagerReplicationCoordinator.class);

  private final Manager manager;
  private final ZooReader reader;
  private final SecurityOperation security;

  public ManagerReplicationCoordinator(Manager manager) {
    this(manager, manager.getContext().getZooReader());
  }

  protected ManagerReplicationCoordinator(Manager manager, ZooReader reader) {
    this.manager = manager;
    this.reader = reader;
    this.security = manager.getContext().getSecurityOperation();
  }

  @Override
  public String getServicerAddress(String remoteTableId, TCredentials creds)
      throws ReplicationCoordinatorException, TException {
    try {
      security.authenticateUser(manager.getContext().rpcCreds(), creds);
    } catch (ThriftSecurityException e) {
      log.error("{} failed to authenticate for replication to {}", creds.getPrincipal(),
          remoteTableId);
      throw new ReplicationCoordinatorException(ReplicationCoordinatorErrorCode.CANNOT_AUTHENTICATE,
          "Could not authenticate " + creds.getPrincipal());
    }

    Set<TServerInstance> tservers = manager.onlineTabletServers();
    if (tservers.isEmpty()) {
      throw new ReplicationCoordinatorException(
          ReplicationCoordinatorErrorCode.NO_AVAILABLE_SERVERS,
          "No tservers are available for replication");
    }

    TServerInstance tserver = getRandomTServer(tservers, random.nextInt(tservers.size()));
    String replServiceAddr;
    try {
      replServiceAddr = new String(reader.getData(manager.getZooKeeperRoot()
          + ReplicationConstants.ZOO_TSERVERS + "/" + tserver.getHostPort()), UTF_8);
    } catch (KeeperException | InterruptedException e) {
      log.error("Could not fetch replication service port for tserver", e);
      throw new ReplicationCoordinatorException(
          ReplicationCoordinatorErrorCode.SERVICE_CONFIGURATION_UNAVAILABLE,
          "Could not determine port for replication service running at " + tserver.getHostPort());
    }

    return replServiceAddr;
  }

  protected TServerInstance getRandomTServer(Set<TServerInstance> tservers, int offset) {
    checkArgument(tservers.size() > offset, "Must provide an offset less than the size of the set");
    Iterator<TServerInstance> iter = tservers.iterator();
    while (offset > 0) {
      iter.next();
      offset--;
    }

    return iter.next();
  }

}

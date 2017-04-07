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
package org.apache.accumulo.master.replication;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinatorErrorCode;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinatorException;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Choose a tserver to service a replication task
 */
public class MasterReplicationCoordinator implements ReplicationCoordinator.Iface {
  private static final Logger log = LoggerFactory.getLogger(MasterReplicationCoordinator.class);

  private final Master master;
  private final Instance inst;
  private final Random rand;
  private final ZooReader reader;
  private final SecurityOperation security;

  public MasterReplicationCoordinator(Master master) {
    this(master, new ZooReader(master.getInstance().getZooKeepers(), master.getInstance().getZooKeepersSessionTimeOut()));
  }

  protected MasterReplicationCoordinator(Master master, ZooReader reader) {
    this.master = master;
    this.rand = new Random(358923462l);
    this.inst = master.getInstance();
    this.reader = reader;
    this.security = SecurityOperation.getInstance(master, false);
  }

  @Override
  public String getServicerAddress(String remoteTableId, TCredentials creds) throws ReplicationCoordinatorException, TException {
    try {
      security.authenticateUser(master.rpcCreds(), creds);
    } catch (ThriftSecurityException e) {
      log.error("{} failed to authenticate for replication to {}", creds.getPrincipal(), remoteTableId);
      throw new ReplicationCoordinatorException(ReplicationCoordinatorErrorCode.CANNOT_AUTHENTICATE, "Could not authenticate " + creds.getPrincipal());
    }

    Set<TServerInstance> tservers = master.onlineTabletServers();
    if (tservers.isEmpty()) {
      throw new ReplicationCoordinatorException(ReplicationCoordinatorErrorCode.NO_AVAILABLE_SERVERS, "No tservers are available for replication");
    }

    TServerInstance tserver = getRandomTServer(tservers, rand.nextInt(tservers.size()));
    String replServiceAddr;
    try {
      replServiceAddr = new String(reader.getData(ZooUtil.getRoot(inst) + ReplicationConstants.ZOO_TSERVERS + "/" + tserver.hostPort(), null), UTF_8);
    } catch (KeeperException | InterruptedException e) {
      log.error("Could not fetch repliation service port for tserver", e);
      throw new ReplicationCoordinatorException(ReplicationCoordinatorErrorCode.SERVICE_CONFIGURATION_UNAVAILABLE,
          "Could not determine port for replication service running at " + tserver.hostPort());
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

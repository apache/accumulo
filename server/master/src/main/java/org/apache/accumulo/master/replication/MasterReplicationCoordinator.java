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

import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.replication.thrift.NoServersAvailableException;
import org.apache.accumulo.core.replication.thrift.RemoteReplicationCoordinator;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;

/**
 * Choose a tserver to service a replication task
 */
public class MasterReplicationCoordinator implements RemoteReplicationCoordinator.Iface {

  public static enum NoServersAvailable {
    NO_ONLINE_SERVERS
  }

  private final Master master;
  private final AccumuloConfiguration conf;
  private final Random rand;
  private final int port;

  public MasterReplicationCoordinator(Master master, AccumuloConfiguration conf) {
    this.master = master;
    this.conf = conf;
    this.port = conf.getPort(Property.REPLICATION_RECEIPT_SERVICE_PORT);
    this.rand = new Random(358923462l);
  }

  @Override
  public String getServicerAddress(int remoteTableId) throws NoServersAvailableException, TException {
    Set<TServerInstance> tservers = master.onlineTabletServers();
    if (tservers.isEmpty()) {
      throw new NoServersAvailableException(NoServersAvailable.NO_ONLINE_SERVERS.ordinal(), "No tservers are online");
    }

    TServerInstance tserver = getRandomTServer(tservers, rand.nextInt(tservers.size()));
    return tserver.host() + ":" + port;
  }

  protected TServerInstance getRandomTServer(Set<TServerInstance> tservers, int offset) {
    Preconditions.checkArgument(tservers.size() > offset, "Must provide an offset less than the size of the set");
    Iterator<TServerInstance> iter = tservers.iterator();
    while (offset > 0) {
      iter.next();
      offset--;
    }

    return iter.next();
  }

}

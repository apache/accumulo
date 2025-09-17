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
package org.apache.accumulo.core.metadata;

import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Objects;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.clientImpl.ServerIdUtil;
import org.apache.accumulo.core.data.ResourceGroupId;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

/**
 * A tablet is assigned to a tablet server at the given address as long as it is alive and well.
 * When the tablet server is restarted, the instance information it advertises will change.
 * Therefore tablet assignments can be considered out-of-date if the tablet server instance
 * information has been changed.
 */
public class TServerInstance implements Comparable<TServerInstance>, Serializable {

  private static final long serialVersionUID = 1L;

  public static record TServerInstanceInfo(ServerIdUtil.ServerIdInfo server, String session) {
    public TServerInstance getTSI() {
      return new TServerInstance(server.getServerId(), session);
    }
  }

  public static TServerInstance deserialize(String json) {
    return GSON.get().fromJson(json, TServerInstanceInfo.class).getTSI();
  }

  public static TServerInstance fromZooKeeperPathString(String zkPath) {

    // TODO: WAL marker serializations using the old format could present a
    // problem. If we change the code here to handle the old format, then
    // we have to make a guess at the resource group, which could affect
    // callers of this method (GcWalsFilter, WalStateManager, LiveTServerSet)

    String parts[] = zkPath.split("\\+");
    Preconditions.checkArgument(parts.length == 3,
        "Invalid tserver instance in zk path: " + zkPath);
    var rgid = ResourceGroupId.of(parts[0]);
    var hostAndPort = HostAndPort.fromString(parts[1]);
    var session = parts[2];
    return new TServerInstance(
        ServerIdUtil.tserver(rgid, hostAndPort.getHost(), hostAndPort.getPort()), session);
  }

  private final ServerId server;
  private final String session;
  private transient String hostPortSession;

  public TServerInstance(ServerId address, String session) {
    Preconditions.checkArgument(address.getType() == Type.TABLET_SERVER,
        "ServerId type must be TABLET_SERVER");
    this.server = address;
    this.session = session;
    setZooKeeperPathString();
  }

  public TServerInstance(ServerId address, long session) {
    Preconditions.checkArgument(address.getType() == Type.TABLET_SERVER,
        "ServerId type must be TABLET_SERVER");
    this.server = address;
    this.session = Long.toHexString(session);
    setZooKeeperPathString();
  }

  public TServerInstance(String json) {
    var partial = GSON.get().fromJson(json, TServerInstanceInfo.class).getTSI();
    this.server = partial.server;
    this.session = partial.session;
    setZooKeeperPathString();
  }

  private void setZooKeeperPathString() {
    this.hostPortSession = server.getResourceGroup().canonical() + "+" + server.toHostPortString()
        + "+" + this.session;
  }

  @Override
  public int compareTo(TServerInstance other) {
    if (this == other) {
      return 0;
    }
    int result = this.getServer().compareTo(other.getServer());
    if (result == 0) {
      return this.getSession().compareTo(other.getSession());
    }
    return result;
  }

  @Override
  public int hashCode() {
    return Objects.hash(server, session);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TServerInstance) {
      return compareTo((TServerInstance) obj) == 0;
    }
    return false;
  }

  public String toZooKeeperPathString() {
    return hostPortSession;
  }

  @Override
  public String toString() {
    return toZooKeeperPathString();
  }

  public String getSession() {
    return session;
  }

  public ServerId getServer() {
    return server;
  }

  public TServerInstanceInfo getTServerInstanceInfo() {
    return new TServerInstanceInfo(ServerIdUtil.toServerIdInfo(server), session);
  }

  public String serialize() {
    return GSON.get().toJson(getTServerInstanceInfo());
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    setZooKeeperPathString();
  }
}

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
package org.apache.accumulo.core.client.admin.servers;

import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Objects;

import org.apache.accumulo.core.conf.PropertyType.PortRange;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.core.util.cache.Caches.CacheName;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

/**
 * Object representing the type, resource group, and address of a server process.
 *
 * @since 4.0.0
 */
public final class ServerId implements Comparable<ServerId>, Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Server process type names.
   *
   * @since 4.0.0
   */
  public enum Type {
    MANAGER, MINI, MONITOR, GARBAGE_COLLECTOR, COMPACTOR, SCAN_SERVER, TABLET_SERVER;
  }

  public static record ServerIdInfo(String type, String resourceGroup, String host, int port) {
    public ServerId getServerId() {
      return new ServerId(Type.valueOf(type), ResourceGroupId.of(resourceGroup), host, port);
    }
  }

  // cache is for canonicalization/deduplication of created objects,
  // to limit the number of ServerId objects in the JVM at any given moment
  // WeakReferences are used because we don't need them to stick around any longer than they need to
  private static final Cache<ServerIdInfo,ServerId> cache =
      Caches.getInstance().createNewBuilder(CacheName.SERVER_ID, false).weakValues().build();

  private static ServerId resolve(ServerIdInfo info) {
    return cache.get(info, k -> info.getServerId());
  }

  public static ServerId compactor(String host, int port) {
    return resolve(
        new ServerIdInfo(Type.COMPACTOR.name(), ResourceGroupId.DEFAULT.canonical(), host, port));
  }

  public static ServerId compactor(ResourceGroupId rgid, String host, int port) {
    return resolve(new ServerIdInfo(Type.COMPACTOR.name(), rgid.canonical(), host, port));
  }

  public static ServerId gc(String host, int port) {
    return resolve(new ServerIdInfo(Type.GARBAGE_COLLECTOR.name(),
        ResourceGroupId.DEFAULT.canonical(), host, port));
  }

  public static ServerId manager(String host, int port) {
    return resolve(
        new ServerIdInfo(Type.MANAGER.name(), ResourceGroupId.DEFAULT.canonical(), host, port));
  }

  public static ServerId mini(String host, int port) {
    return resolve(
        new ServerIdInfo(Type.MINI.name(), ResourceGroupId.DEFAULT.canonical(), host, port));
  }

  public static ServerId monitor(String host, int port) {
    return resolve(
        new ServerIdInfo(Type.MONITOR.name(), ResourceGroupId.DEFAULT.canonical(), host, port));
  }

  public static ServerId sserver(String host, int port) {
    return resolve(
        new ServerIdInfo(Type.SCAN_SERVER.name(), ResourceGroupId.DEFAULT.canonical(), host, port));
  }

  public static ServerId sserver(ResourceGroupId rgid, String host, int port) {
    return resolve(new ServerIdInfo(Type.SCAN_SERVER.name(), rgid.canonical(), host, port));
  }

  public static ServerId tserver(String host, int port) {
    return resolve(new ServerIdInfo(Type.TABLET_SERVER.name(), ResourceGroupId.DEFAULT.canonical(),
        host, port));
  }

  public static ServerId tserver(ResourceGroupId rgid, String host, int port) {
    return resolve(new ServerIdInfo(Type.TABLET_SERVER.name(), rgid.canonical(), host, port));
  }

  public static ServerId dynamic(Type type, ResourceGroupId rgid, String host, int port) {
    return resolve(new ServerIdInfo(type.name(), rgid.canonical(), host, port));
  }

  public static ServerId fromWalFileName(String name) {
    String parts[] = name.split("\\+");
    Preconditions.checkArgument(parts.length == 3, "Invalid server id in wal file: " + name);
    // return an uncached tserver object
    return ServerId.tserver(ResourceGroupId.of(parts[0]), parts[1], Integer.parseInt(parts[2]));
  }

  public static final ServerId deserialize(String json) {
    return GSON.get().fromJson(json, ServerIdInfo.class).getServerId();
  }

  private final Type type;
  private final ResourceGroupId resourceGroup;
  private final String host;
  private final int port;
  private transient HostAndPort hostPort;

  private ServerId(Type type, ResourceGroupId resourceGroup, String host, int port) {
    super();
    Preconditions.checkArgument(port == 0 || PortRange.VALID_RANGE.contains(port),
        "invalid server port value: " + port);
    this.type = Objects.requireNonNull(type);
    this.resourceGroup = Objects.requireNonNull(resourceGroup);
    this.host = Objects.requireNonNull(host);
    this.port = port;
    this.hostPort = HostAndPort.fromParts(host, port);
  }

  public Type getType() {
    return type;
  }

  public ResourceGroupId getResourceGroup() {
    return this.resourceGroup;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  private synchronized HostAndPort getHostPort() {
    if (hostPort == null) {
      hostPort = HostAndPort.fromParts(host, port);
    }
    return hostPort;
  }

  @Override
  public int compareTo(ServerId other) {
    if (this == other) {
      return 0;
    }
    int result = this.getType().compareTo(other.getType());
    if (result == 0) {
      result = this.getResourceGroup().compareTo(other.getResourceGroup());
      if (result == 0) {
        result = this.getHost().compareTo(other.getHost());
        if (result == 0) {
          result = Integer.compare(this.getPort(), other.getPort());
        }
      }
    }
    return result;
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port, type, resourceGroup);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ServerId other = (ServerId) obj;
    return 0 == compareTo(other);
  }

  @Override
  public String toString() {
    return "Server [type= " + type + ", resource group= " + resourceGroup + ", host= " + host
        + ", port= " + port + "]";
  }

  public String toWalFileName() {
    return resourceGroup + "+" + host + "+" + port;
  }

  public String toHostPortString() {
    return getHostPort().toString();
  }

  public ServerIdInfo toServerIdInfo() {
    return new ServerIdInfo(getType().name(), getResourceGroup().canonical(), getHost(), getPort());
  }

  public String serialize() {
    return GSON.get().toJson(toServerIdInfo());
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.hostPort = HostAndPort.fromParts(host, port);
  }
}

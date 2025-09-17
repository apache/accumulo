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
package org.apache.accumulo.core.clientImpl;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.core.util.cache.Caches.CacheName;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.base.Preconditions;

public class ServerIdUtil {

  public static record ServerIdInfo(String type, String resourceGroup, String host, int port) {
    public ServerId getServerId() {
      return new ServerId(ServerId.Type.valueOf(type), ResourceGroupId.of(resourceGroup), host,
          port);
    }
  }

  // cache is for canonicalization/deduplication of created objects,
  // to limit the number of ServerId objects in the JVM at any given moment
  // WeakReferences are used because we don't need them to stick around any longer than they need to
  private static final Cache<ServerIdInfo,ServerId> cache =
      Caches.getInstance().createNewBuilder(CacheName.SERVER_ID, false).weakValues().build();

  public static ServerIdInfo toServerIdInfo(ServerId server) {
    return new ServerIdInfo(server.getType().name(), server.getResourceGroup().canonical(),
        server.getHost(), server.getPort());
  }

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
    return tserver(ResourceGroupId.of(parts[0]), parts[1], Integer.parseInt(parts[2]));
  }

  public static String toWalFileName(ServerId server) {
    return server.getResourceGroup() + "+" + server.getHost() + "+" + server.getPort();
  }

}

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
package org.apache.accumulo.core.lock;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooCache.ZcStat;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

/**
 * Class for creating and retrieving ServiceLockPath objects
 */
public class ServiceLockPaths {

  public static class ServiceLockPath {
    private final String type;
    private final String resourceGroup;
    private final String server;
    private final String path;

    /**
     * Exists for ServiceLockIt
     */
    protected ServiceLockPath(String path) {
      this.type = null;
      this.resourceGroup = null;
      this.server = null;
      this.path = path;
    }

    /**
     * Create a ServiceLockPath for a management process
     */
    private ServiceLockPath(String root, String type) {
      Objects.requireNonNull(root);
      this.type = Objects.requireNonNull(type);
      Preconditions.checkArgument(this.type.equals(Constants.ZGC_LOCK)
          || this.type.equals(Constants.ZMANAGER_LOCK) || this.type.equals(Constants.ZMONITOR_LOCK)
          || this.type.equals(Constants.ZTABLE_LOCKS), "Unsupported type: " + type);
      // These server types support only one active instance, so they use a lock at
      // a known path, not the server's address.
      this.resourceGroup = null;
      this.server = null;
      this.path = root + this.type;
    }

    /**
     * Create a ServiceLockPath for ZTABLE_LOCKS
     */
    private ServiceLockPath(String root, String type, String content) {
      Objects.requireNonNull(root);
      this.type = Objects.requireNonNull(type);
      Preconditions.checkArgument(
          this.type.equals(Constants.ZTABLE_LOCKS) || this.type.equals(Constants.ZMINI_LOCK),
          "Unsupported type: " + type);
      this.resourceGroup = null;
      this.server = Objects.requireNonNull(content);
      this.path = root + this.type + "/" + this.server;
    }

    /**
     * Create a ServiceLockPath for a worker process
     */
    private ServiceLockPath(String root, String type, String resourceGroup, String server) {
      Objects.requireNonNull(root);
      this.type = Objects.requireNonNull(type);
      Preconditions.checkArgument(
          this.type.equals(Constants.ZCOMPACTORS) || this.type.equals(Constants.ZSSERVERS)
              || this.type.equals(Constants.ZTSERVERS) || this.type.equals(Constants.ZDEADTSERVERS),
          "Unsupported type: " + type);
      this.resourceGroup = Objects.requireNonNull(resourceGroup);
      this.server = Objects.requireNonNull(server);
      this.path = root + this.type + "/" + this.resourceGroup + "/" + this.server;
    }

    public String getType() {
      return type;
    }

    public String getResourceGroup() {
      return resourceGroup;
    }

    public String getServer() {
      return server;
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
      ServiceLockPath other = (ServiceLockPath) obj;
      if (path == null) {
        if (other.path != null) {
          return false;
        }
      } else if (!path.equals(other.path)) {
        return false;
      }
      if (resourceGroup == null) {
        if (other.resourceGroup != null) {
          return false;
        }
      } else if (!resourceGroup.equals(other.resourceGroup)) {
        return false;
      }
      if (server == null) {
        if (other.server != null) {
          return false;
        }
      } else if (!server.equals(other.server)) {
        return false;
      }
      if (type == null) {
        if (other.type != null) {
          return false;
        }
      } else if (!type.equals(other.type)) {
        return false;
      }
      return true;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((path == null) ? 0 : path.hashCode());
      result = prime * result + ((resourceGroup == null) ? 0 : resourceGroup.hashCode());
      result = prime * result + ((server == null) ? 0 : server.hashCode());
      result = prime * result + ((type == null) ? 0 : type.hashCode());
      return result;
    }

    @Override
    public String toString() {
      return this.path;
    }

  }

  private final ClientContext ctx;

  public ServiceLockPaths(ClientContext context) {
    this.ctx = context;
  }

  private static String determineServerType(final String path) {
    if (path.contains(Constants.ZGC_LOCK)) {
      return Constants.ZGC_LOCK;
    } else if (path.contains(Constants.ZMANAGER_LOCK)) {
      return Constants.ZMANAGER_LOCK;
    } else if (path.contains(Constants.ZMONITOR_LOCK)) {
      return Constants.ZMONITOR_LOCK;
    } else if (path.contains(Constants.ZMINI_LOCK)) {
      return Constants.ZMINI_LOCK;
    } else if (path.contains(Constants.ZCOMPACTORS)) {
      return Constants.ZCOMPACTORS;
    } else if (path.contains(Constants.ZSSERVERS)) {
      return Constants.ZSSERVERS;
    } else if (path.contains(Constants.ZDEADTSERVERS)) {
      // This has to be before TSERVERS
      return Constants.ZDEADTSERVERS;
    } else if (path.contains(Constants.ZTSERVERS)) {
      return Constants.ZTSERVERS;
    } else {
      throw new IllegalArgumentException("Unhandled to determine server type from path: " + path);
    }
  }

  /**
   * Parse a ZooKeeper path string and return a ServiceLockPath
   */
  public static ServiceLockPath parse(Optional<String> serverType, String path) {
    Objects.requireNonNull(serverType);
    Objects.requireNonNull(path);

    final String type = serverType.orElseGet(() -> determineServerType(path));

    switch (type) {
      case Constants.ZGC_LOCK:
      case Constants.ZMANAGER_LOCK:
      case Constants.ZMONITOR_LOCK:
        return new ServiceLockPath(path.substring(0, path.indexOf(type)), type);
      default: {
        final String[] pathParts = path.replaceFirst("/", "").split("/");
        Preconditions.checkArgument(pathParts.length >= 4,
            "Unhandled zookeeper service path : " + path);
        final String server = pathParts[pathParts.length - 1];
        final String resourceGroup = pathParts[pathParts.length - 2];
        switch (type) {
          case Constants.ZMINI_LOCK:
            return new ServiceLockPath(path.substring(0, path.indexOf(type)), type, server);
          case Constants.ZCOMPACTORS:
          case Constants.ZSSERVERS:
          case Constants.ZTSERVERS:
          case Constants.ZDEADTSERVERS:
            return new ServiceLockPath(path.substring(0, path.indexOf(type)), type, resourceGroup,
                server);
          default:
            throw new IllegalArgumentException("Unhandled zookeeper service path : " + path);
        }
      }
    }

  }

  public ServiceLockPath createGarbageCollectorPath() {
    return new ServiceLockPath(ctx.getZooKeeperRoot(), Constants.ZGC_LOCK);
  }

  public ServiceLockPath createManagerPath() {
    return new ServiceLockPath(ctx.getZooKeeperRoot(), Constants.ZMANAGER_LOCK);
  }

  public ServiceLockPath createMiniPath(String miniUUID) {
    return new ServiceLockPath(ctx.getZooKeeperRoot(), Constants.ZMINI_LOCK, miniUUID);
  }

  public ServiceLockPath createMonitorPath() {
    return new ServiceLockPath(ctx.getZooKeeperRoot(), Constants.ZMONITOR_LOCK);
  }

  public ServiceLockPath createCompactorPath(String resourceGroup, HostAndPort serverAddress) {
    return new ServiceLockPath(ctx.getZooKeeperRoot(), Constants.ZCOMPACTORS, resourceGroup,
        serverAddress.toString());
  }

  public ServiceLockPath createScanServerPath(String resourceGroup, HostAndPort serverAddress) {
    return new ServiceLockPath(ctx.getZooKeeperRoot(), Constants.ZSSERVERS, resourceGroup,
        serverAddress.toString());
  }

  public ServiceLockPath createTableLocksPath() {
    return new ServiceLockPath(ctx.getZooKeeperRoot(), Constants.ZTABLE_LOCKS);
  }

  public ServiceLockPath createTableLocksPath(String tableId) {
    return new ServiceLockPath(ctx.getZooKeeperRoot(), Constants.ZTABLE_LOCKS, tableId);
  }

  public ServiceLockPath createTabletServerPath(String resourceGroup, HostAndPort serverAddress) {
    return new ServiceLockPath(ctx.getZooKeeperRoot(), Constants.ZTSERVERS, resourceGroup,
        serverAddress.toString());
  }

  public ServiceLockPath createDeadTabletServerPath(String resourceGroup,
      HostAndPort serverAddress) {
    return new ServiceLockPath(ctx.getZooKeeperRoot(), Constants.ZDEADTSERVERS, resourceGroup,
        serverAddress.toString());
  }

  public Set<ServiceLockPath> getCompactor(Optional<String> resourceGroup,
      Optional<HostAndPort> address, boolean withLock) {
    return get(Constants.ZCOMPACTORS, resourceGroup, address, withLock);
  }

  public ServiceLockPath getGarbageCollector(boolean withLock) {
    Set<ServiceLockPath> results =
        get(Constants.ZGC_LOCK, Optional.empty(), Optional.empty(), withLock);
    if (results.isEmpty()) {
      return null;
    } else {
      return results.iterator().next();
    }
  }

  public ServiceLockPath getManager(boolean withLock) {
    Set<ServiceLockPath> results =
        get(Constants.ZMANAGER_LOCK, Optional.empty(), Optional.empty(), withLock);
    if (results.isEmpty()) {
      return null;
    } else {
      return results.iterator().next();
    }
  }

  public ServiceLockPath getMonitor(boolean withLock) {
    Set<ServiceLockPath> results =
        get(Constants.ZMONITOR_LOCK, Optional.empty(), Optional.empty(), withLock);
    if (results.isEmpty()) {
      return null;
    } else {
      return results.iterator().next();
    }
  }

  public Set<ServiceLockPath> getScanServer(Optional<String> resourceGroup,
      Optional<HostAndPort> address, boolean withLock) {
    return get(Constants.ZSSERVERS, resourceGroup, address, withLock);
  }

  public Set<ServiceLockPath> getTabletServer(Optional<String> resourceGroup,
      Optional<HostAndPort> address, boolean withLock) {
    return get(Constants.ZTSERVERS, resourceGroup, address, withLock);
  }

  public Set<ServiceLockPath> getDeadTabletServer(Optional<String> resourceGroup,
      Optional<HostAndPort> address, boolean withLock) {
    return get(Constants.ZDEADTSERVERS, resourceGroup, address, withLock);
  }

  /**
   * Find paths in ZooKeeper based on the input arguments and return a set of ServiceLockPath
   * objects.
   *
   * @param serverType type of lock, should be something like Constants.ZTSERVERS or
   *        Constants.ZMANAGER_LOCK
   * @param resourceGroup name of resource group, if empty will return all resource groups
   * @param address address of server (host:port), if empty will return all addresses
   * @param withLock supply true if you only want to return servers that have an active lock. Not
   *        applicable for types that don't use a lock (e.g. dead tservers)
   * @return set of ServiceLockPath objects for the paths found based on the search criteria
   */
  private Set<ServiceLockPath> get(final String serverType, Optional<String> resourceGroup,
      Optional<HostAndPort> address, boolean withLock) {

    Objects.requireNonNull(serverType);
    Objects.requireNonNull(resourceGroup);
    Objects.requireNonNull(address);

    final Set<ServiceLockPath> results = new HashSet<>();
    final String typePath = ctx.getZooKeeperRoot() + serverType;
    final ZooCache cache = ctx.getZooCache();

    if (serverType.equals(Constants.ZGC_LOCK) || serverType.equals(Constants.ZMANAGER_LOCK)
        || serverType.equals(Constants.ZMONITOR_LOCK)) {
      final ZcStat stat = new ZcStat();
      final ServiceLockPath slp = parse(Optional.of(serverType), typePath);
      if (!withLock) {
        results.add(slp);
      } else {
        Optional<ServiceLockData> sld = ServiceLock.getLockData(cache, slp, stat);
        if (!sld.isEmpty()) {
          results.add(slp);
        }
      }
    } else if (serverType.equals(Constants.ZCOMPACTORS) || serverType.equals(Constants.ZSSERVERS)
        || serverType.equals(Constants.ZTSERVERS) || serverType.equals(Constants.ZDEADTSERVERS)) {
      final List<String> resourceGroups = cache.getChildren(typePath);
      for (final String group : resourceGroups) {
        if (resourceGroup.isEmpty() || resourceGroup.orElseThrow().equals(group)) {
          final List<String> servers = cache.getChildren(typePath + "/" + group);
          for (final String server : servers) {
            final ZcStat stat = new ZcStat();
            final ServiceLockPath slp =
                parse(Optional.of(serverType), typePath + "/" + group + "/" + server);
            if (address.isEmpty() || address.orElseThrow().toString().equals(server)) {
              if (!withLock || slp.getType().equals(Constants.ZDEADTSERVERS)) {
                // Dead TServers don't have lock data
                results.add(slp);
              } else {
                Optional<ServiceLockData> sld = ServiceLock.getLockData(cache, slp, stat);
                if (!sld.isEmpty()
                    && (address.isEmpty() || address.orElseThrow().toString().equals(server))) {
                  results.add(slp);
                }
              }
            }
          }
        }
      }
    } else {
      throw new IllegalArgumentException("Unhandled zookeeper service path");
    }
    return results;
  }

}

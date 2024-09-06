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
      Preconditions.checkArgument(
          this.type.equals(Constants.ZGC_LOCK) || this.type.equals(Constants.ZMANAGER_LOCK)
              || this.type.equals(Constants.ZMONITOR_LOCK)
              || this.type.equals(Constants.ZTABLE_LOCKS) || this.type.equals(Constants.ZMINI_LOCK),
          "Unsupported type: " + type);
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
      Preconditions.checkArgument(this.type.equals(Constants.ZTABLE_LOCKS),
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

  /**
   * Parse a ZooKeeper path string and return a ServiceLockPath
   */
  public static ServiceLockPath parse(String path) {
    Objects.requireNonNull(path);
    if (path.contains(Constants.ZGC_LOCK)) {
      return new ServiceLockPath(path.substring(0, path.indexOf(Constants.ZGC_LOCK)),
          Constants.ZGC_LOCK);
    } else if (path.contains(Constants.ZMANAGER_LOCK)) {
      return new ServiceLockPath(path.substring(0, path.indexOf(Constants.ZMANAGER_LOCK)),
          Constants.ZMANAGER_LOCK);
    } else if (path.contains(Constants.ZMONITOR_LOCK)) {
      return new ServiceLockPath(path.substring(0, path.indexOf(Constants.ZMONITOR_LOCK)),
          Constants.ZMONITOR_LOCK);
    } else {
      String[] pathParts = path.split("/");
      Preconditions.checkArgument(pathParts.length >= 4,
          "Unhandled zookeeper service path : " + path);
      String server = pathParts[pathParts.length - 1];
      String resourceGroup = pathParts[pathParts.length - 2];
      if (path.contains(Constants.ZCOMPACTORS)) {
        return new ServiceLockPath(path.substring(0, path.indexOf(Constants.ZCOMPACTORS)),
            Constants.ZCOMPACTORS, resourceGroup, server);
      } else if (path.contains(Constants.ZSSERVERS)) {
        return new ServiceLockPath(path.substring(0, path.indexOf(Constants.ZSSERVERS)),
            Constants.ZSSERVERS, resourceGroup, server);
      } else if (path.contains(Constants.ZDEADTSERVERS)) {
        // This has to be before TSERVERS
        return new ServiceLockPath(path.substring(0, path.indexOf(Constants.ZDEADTSERVERS)),
            Constants.ZDEADTSERVERS, resourceGroup, server);
      } else if (path.contains(Constants.ZTSERVERS)) {
        return new ServiceLockPath(path.substring(0, path.indexOf(Constants.ZTSERVERS)),
            Constants.ZTSERVERS, resourceGroup, server);
      } else {
        throw new IllegalArgumentException("Unhandled zookeeper service path : " + path);
      }
    }
  }

  public static ServiceLockPath createGarbageCollectorPath(ClientContext context) {
    return new ServiceLockPath(Objects.requireNonNull(context).getZooKeeperRoot(),
        Constants.ZGC_LOCK);
  }

  public static ServiceLockPath createManagerPath(ClientContext context) {
    return new ServiceLockPath(Objects.requireNonNull(context).getZooKeeperRoot(),
        Constants.ZMANAGER_LOCK);
  }

  public static ServiceLockPath createMiniPath(ClientContext context) {
    return new ServiceLockPath(Objects.requireNonNull(context).getZooKeeperRoot(),
        Constants.ZMINI_LOCK);
  }

  public static ServiceLockPath createMonitorPath(ClientContext context) {
    return new ServiceLockPath(Objects.requireNonNull(context).getZooKeeperRoot(),
        Constants.ZMONITOR_LOCK);
  }

  public static ServiceLockPath createCompactorPath(ClientContext context, String resourceGroup,
      HostAndPort serverAddress) {
    return new ServiceLockPath(Objects.requireNonNull(context).getZooKeeperRoot(),
        Constants.ZCOMPACTORS, resourceGroup, serverAddress.toString());
  }

  public static ServiceLockPath createScanServerPath(ClientContext context, String resourceGroup,
      HostAndPort serverAddress) {
    return new ServiceLockPath(Objects.requireNonNull(context).getZooKeeperRoot(),
        Constants.ZSSERVERS, resourceGroup, serverAddress.toString());
  }

  public static ServiceLockPath createTableLocksPath(ClientContext context) {
    return new ServiceLockPath(Objects.requireNonNull(context).getZooKeeperRoot(),
        Constants.ZTABLE_LOCKS);
  }

  public static ServiceLockPath createTableLocksPath(ClientContext context, String tableId) {
    return new ServiceLockPath(Objects.requireNonNull(context).getZooKeeperRoot(),
        Constants.ZTABLE_LOCKS, tableId);
  }

  public static ServiceLockPath createTabletServerPath(ClientContext context, String resourceGroup,
      HostAndPort serverAddress) {
    return new ServiceLockPath(Objects.requireNonNull(context).getZooKeeperRoot(),
        Constants.ZTSERVERS, resourceGroup, serverAddress.toString());
  }

  public static ServiceLockPath createDeadTabletServerPath(ClientContext context,
      String resourceGroup, HostAndPort serverAddress) {
    return new ServiceLockPath(Objects.requireNonNull(context).getZooKeeperRoot(),
        Constants.ZDEADTSERVERS, resourceGroup, serverAddress.toString());
  }

  public static Set<ServiceLockPath> getCompactor(ClientContext context,
      Optional<String> resourceGroup, Optional<HostAndPort> address) {
    return get(context, Constants.ZCOMPACTORS, resourceGroup, address);
  }

  public static ServiceLockPath getGarbageCollector(ClientContext context) {
    Set<ServiceLockPath> results =
        get(context, Constants.ZGC_LOCK, Optional.empty(), Optional.empty());
    if (results.isEmpty()) {
      return null;
    } else {
      return results.iterator().next();
    }
  }

  public static ServiceLockPath getManager(ClientContext context) {
    Set<ServiceLockPath> results =
        get(context, Constants.ZMANAGER_LOCK, Optional.empty(), Optional.empty());
    if (results.isEmpty()) {
      return null;
    } else {
      return results.iterator().next();
    }
  }

  public static ServiceLockPath getMonitor(ClientContext context) {
    Set<ServiceLockPath> results =
        get(context, Constants.ZMONITOR_LOCK, Optional.empty(), Optional.empty());
    if (results.isEmpty()) {
      return null;
    } else {
      return results.iterator().next();
    }
  }

  public static Set<ServiceLockPath> getScanServer(ClientContext context,
      Optional<String> resourceGroup, Optional<HostAndPort> address) {
    return get(context, Constants.ZSSERVERS, resourceGroup, address);
  }

  public static Set<ServiceLockPath> getTabletServer(ClientContext context,
      Optional<String> resourceGroup, Optional<HostAndPort> address) {
    return get(context, Constants.ZTSERVERS, resourceGroup, address);
  }

  public static Set<ServiceLockPath> getDeadTabletServer(ClientContext context,
      Optional<String> resourceGroup, Optional<HostAndPort> address) {
    return get(context, Constants.ZDEADTSERVERS, resourceGroup, address);
  }

  /**
   * Find paths in ZooKeeper based on the input arguments and return a set of ServiceLockPath
   * objects at those paths that have valid locks.
   */
  private static Set<ServiceLockPath> get(final ClientContext context, final String serverType,
      Optional<String> resourceGroup, Optional<HostAndPort> address) {

    Objects.requireNonNull(context);
    Objects.requireNonNull(serverType);
    Objects.requireNonNull(resourceGroup);
    Objects.requireNonNull(address);

    final Set<ServiceLockPath> results = new HashSet<>();
    final ZooCache cache = context.getZooCache();
    final String typePath = context.getZooKeeperRoot() + serverType;

    if (serverType.equals(Constants.ZGC_LOCK) || serverType.equals(Constants.ZMANAGER_LOCK)
        || serverType.equals(Constants.ZMONITOR_LOCK)) {
      final ZcStat stat = new ZcStat();
      final ServiceLockPath slp = parse(typePath);
      Optional<ServiceLockData> sld = ServiceLock.getLockData(cache, slp, stat);
      if (!sld.isEmpty()) {
        results.add(slp);
      }
    } else if (serverType.equals(Constants.ZCOMPACTORS) || serverType.equals(Constants.ZSSERVERS)
        || serverType.equals(Constants.ZTSERVERS) || serverType.equals(Constants.ZDEADTSERVERS)) {
      final List<String> resourceGroups = cache.getChildren(typePath);
      for (final String group : resourceGroups) {
        if (resourceGroup.isEmpty() || resourceGroup.orElseThrow().equals(group)) {
          final List<String> servers = cache.getChildren(typePath + "/" + group);
          for (final String server : servers) {
            final ZcStat stat = new ZcStat();
            final ServiceLockPath slp = parse(typePath + "/" + group + "/" + server);
            Optional<ServiceLockData> sld = ServiceLock.getLockData(cache, slp, stat);
            if (!sld.isEmpty()
                && (address.isEmpty() || address.orElseThrow().toString().equals(server))) {
              results.add(slp);
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

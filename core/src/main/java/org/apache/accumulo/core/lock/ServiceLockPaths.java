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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.core.util.threads.ThreadPoolNames;
import org.apache.accumulo.core.util.threads.ThreadPools;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.MoreExecutors;

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
      requireNonNull(root);
      this.type = requireNonNull(type);
      Preconditions.checkArgument(this.type.equals(Constants.ZGC_LOCK)
          || this.type.equals(Constants.ZMANAGER_LOCK) || this.type.equals(Constants.ZMONITOR_LOCK)
          || this.type.equals(Constants.ZTABLE_LOCKS) || this.type.equals(Constants.ZADMIN_LOCK)
          || this.type.equals(Constants.ZTEST_LOCK), "Unsupported type: " + type);
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
      requireNonNull(root);
      this.type = requireNonNull(type);
      Preconditions.checkArgument(
          this.type.equals(Constants.ZTABLE_LOCKS) || this.type.equals(Constants.ZMINI_LOCK),
          "Unsupported type: " + type);
      this.resourceGroup = null;
      this.server = requireNonNull(content);
      this.path = root + this.type + "/" + this.server;
    }

    /**
     * Create a ServiceLockPath for a worker process
     */
    private ServiceLockPath(String root, String type, String resourceGroup, String server) {
      requireNonNull(root);
      this.type = requireNonNull(type);
      Preconditions.checkArgument(
          this.type.equals(Constants.ZCOMPACTORS) || this.type.equals(Constants.ZSSERVERS)
              || this.type.equals(Constants.ZTSERVERS) || this.type.equals(Constants.ZDEADTSERVERS),
          "Unsupported type: " + type);
      this.resourceGroup = requireNonNull(resourceGroup);
      this.server = requireNonNull(server);
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

  private final ExecutorService fetchExectuor;

  private final String zkRoot;
  private final ZooCache zooCache;

  public ServiceLockPaths(String zkRoot, ZooCache zc) {
    this.zkRoot = requireNonNull(zkRoot);
    this.zooCache = requireNonNull(zc);
    this.fetchExectuor = ThreadPools.getServerThreadPools()
        .getPoolBuilder(ThreadPoolNames.SERVICE_LOCK_POOL).numCoreThreads(16).build();
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
    } else if (path.contains(Constants.ZADMIN_LOCK)) {
      return Constants.ZADMIN_LOCK;
    } else if (path.contains(Constants.ZTEST_LOCK)) {
      return Constants.ZTEST_LOCK;
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
    requireNonNull(serverType);
    requireNonNull(path);

    final String type = serverType.orElseGet(() -> determineServerType(path));

    switch (type) {
      case Constants.ZGC_LOCK:
      case Constants.ZMANAGER_LOCK:
      case Constants.ZMONITOR_LOCK:
      case Constants.ZADMIN_LOCK:
      case Constants.ZTEST_LOCK:
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
    return new ServiceLockPath(zkRoot, Constants.ZGC_LOCK);
  }

  public ServiceLockPath createManagerPath() {
    return new ServiceLockPath(zkRoot, Constants.ZMANAGER_LOCK);
  }

  public ServiceLockPath createMiniPath(String miniUUID) {
    return new ServiceLockPath(zkRoot, Constants.ZMINI_LOCK, miniUUID);
  }

  public ServiceLockPath createMonitorPath() {
    return new ServiceLockPath(zkRoot, Constants.ZMONITOR_LOCK);
  }

  public ServiceLockPath createCompactorPath(String resourceGroup, HostAndPort serverAddress) {
    return new ServiceLockPath(zkRoot, Constants.ZCOMPACTORS, resourceGroup,
        serverAddress.toString());
  }

  public ServiceLockPath createScanServerPath(String resourceGroup, HostAndPort serverAddress) {
    return new ServiceLockPath(zkRoot, Constants.ZSSERVERS, resourceGroup,
        serverAddress.toString());
  }

  public ServiceLockPath createTableLocksPath() {
    return new ServiceLockPath(zkRoot, Constants.ZTABLE_LOCKS);
  }

  public ServiceLockPath createTableLocksPath(String tableId) {
    return new ServiceLockPath(zkRoot, Constants.ZTABLE_LOCKS, tableId);
  }

  public ServiceLockPath createTabletServerPath(String resourceGroup, HostAndPort serverAddress) {
    return new ServiceLockPath(zkRoot, Constants.ZTSERVERS, resourceGroup,
        serverAddress.toString());
  }

  public ServiceLockPath createDeadTabletServerPath(String resourceGroup,
      HostAndPort serverAddress) {
    return new ServiceLockPath(zkRoot, Constants.ZDEADTSERVERS, resourceGroup,
        serverAddress.toString());
  }

  public ServiceLockPath createAdminLockPath() {
    return new ServiceLockPath(zkRoot, Constants.ZADMIN_LOCK);
  }

  public ServiceLockPath createTestLockPath() {
    return new ServiceLockPath(zkRoot, Constants.ZTEST_LOCK);
  }

  public Set<ServiceLockPath> getCompactor(ResourceGroupPredicate resourceGroupPredicate,
      AddressSelector address, boolean withLock) {
    return get(Constants.ZCOMPACTORS, resourceGroupPredicate, address, withLock);
  }

  /**
   * Note that the ServiceLockPath object returned by this method does not populate the server
   * attribute. To get the location of the GarbageCollector you will need to parse the lock data at
   * the ZooKeeper path.
   */
  public ServiceLockPath getGarbageCollector(boolean withLock) {
    Set<ServiceLockPath> results =
        get(Constants.ZGC_LOCK, rg -> true, AddressSelector.all(), withLock);
    if (results.isEmpty()) {
      return null;
    } else {
      return results.iterator().next();
    }
  }

  /**
   * Note that the ServiceLockPath object returned by this method does not populate the server
   * attribute. The location of the Manager is not in the ZooKeeper path. Instead, use
   * InstanceOperations.getServers(ServerId.Type.MANAGER) to get the location.
   */
  public ServiceLockPath getManager(boolean withLock) {
    Set<ServiceLockPath> results =
        get(Constants.ZMANAGER_LOCK, rg -> true, AddressSelector.all(), withLock);
    if (results.isEmpty()) {
      return null;
    } else {
      return results.iterator().next();
    }
  }

  /**
   * Note that the ServiceLockPath object returned by this method does not populate the server
   * attribute. To get the location of the GarbageCollector you will need to parse the lock data at
   * the ZooKeeper path.
   */
  public ServiceLockPath getMonitor(boolean withLock) {
    Set<ServiceLockPath> results =
        get(Constants.ZMONITOR_LOCK, rg -> true, AddressSelector.all(), withLock);
    if (results.isEmpty()) {
      return null;
    } else {
      return results.iterator().next();
    }
  }

  public Set<ServiceLockPath> getScanServer(ResourceGroupPredicate resourceGroupPredicate,
      AddressSelector address, boolean withLock) {
    return get(Constants.ZSSERVERS, resourceGroupPredicate, address, withLock);
  }

  public Set<ServiceLockPath> getTabletServer(ResourceGroupPredicate resourceGroupPredicate,
      AddressSelector address, boolean withLock) {
    return get(Constants.ZTSERVERS, resourceGroupPredicate, address, withLock);
  }

  public Set<ServiceLockPath> getDeadTabletServer(ResourceGroupPredicate resourceGroupPredicate,
      AddressSelector address, boolean withLock) {
    return get(Constants.ZDEADTSERVERS, resourceGroupPredicate, address, withLock);
  }

  public interface ResourceGroupPredicate extends Predicate<String> {

  }

  public static class AddressSelector {
    private final Predicate<String> predicate;
    private final HostAndPort exactAddress;

    private AddressSelector(Predicate<String> predicate, HostAndPort exactAddress) {
      Preconditions.checkArgument((predicate == null && exactAddress != null)
          || (predicate != null && exactAddress == null));
      if (predicate == null) {
        String hp = exactAddress.toString();
        this.predicate = addr -> addr.equals(hp);
      } else {
        this.predicate = predicate;
      }
      this.exactAddress = exactAddress;
    }

    public static AddressSelector exact(HostAndPort hostAndPort) {
      return new AddressSelector(null, hostAndPort);
    }

    public static AddressSelector matching(Predicate<String> predicate) {
      return new AddressSelector(predicate, null);
    }

    private static AddressSelector ALL = new AddressSelector(s -> true, null);

    public static AddressSelector all() {
      return ALL;
    }

    public HostAndPort getExactAddress() {
      return exactAddress;
    }

    public Predicate<String> getPredicate() {
      return predicate;
    }
  }

  /**
   * Find paths in ZooKeeper based on the input arguments and return a set of ServiceLockPath
   * objects.
   *
   * @param serverType type of lock, should be something like Constants.ZTSERVERS or
   *        Constants.ZMANAGER_LOCK
   * @param resourceGroupPredicate only returns servers in resource groups that pass this predicate
   * @param addressSelector only return servers that meet this criteria
   * @param withLock supply true if you only want to return servers that have an active lock. Not
   *        applicable for types that don't use a lock (e.g. dead tservers)
   * @return set of ServiceLockPath objects for the paths found based on the search criteria
   */
  private Set<ServiceLockPath> get(final String serverType,
      ResourceGroupPredicate resourceGroupPredicate, AddressSelector addressSelector,
      boolean withLock) {

    requireNonNull(serverType);
    requireNonNull(resourceGroupPredicate);
    requireNonNull(addressSelector);

    final Set<ServiceLockPath> results = ConcurrentHashMap.newKeySet();
    final String typePath = zkRoot + serverType;

    if (serverType.equals(Constants.ZGC_LOCK) || serverType.equals(Constants.ZMANAGER_LOCK)
        || serverType.equals(Constants.ZMONITOR_LOCK)) {
      final ZcStat stat = new ZcStat();
      final ServiceLockPath slp = parse(Optional.of(serverType), typePath);
      if (!withLock) {
        results.add(slp);
      } else {
        Optional<ServiceLockData> sld = ServiceLock.getLockData(zooCache, slp, stat);
        if (!sld.isEmpty()) {
          results.add(slp);
        }
      }
    } else if (serverType.equals(Constants.ZCOMPACTORS) || serverType.equals(Constants.ZSSERVERS)
        || serverType.equals(Constants.ZTSERVERS) || serverType.equals(Constants.ZDEADTSERVERS)) {
      final List<String> resourceGroups = zooCache.getChildren(typePath);
      for (final String group : resourceGroups) {
        if (resourceGroupPredicate.test(group)) {
          final Collection<String> servers;
          final Predicate<String> addressPredicate;

          if (addressSelector.getExactAddress() != null) {
            var server = addressSelector.getExactAddress().toString();
            if (withLock || zooCache.get(typePath + "/" + group + "/" + server) != null) {
              // When withLock is true the server in the list may not exist in zookeeper, if it does
              // not exist then no lock will be found later when looking for a lock in zookeeper.
              servers = List.of(server);
            } else {
              servers = List.of();
            }
            addressPredicate = s -> true;
          } else {
            servers = zooCache.getChildren(typePath + "/" + group);
            addressPredicate = addressSelector.getPredicate();
          }

          // For lots of servers use a thread pool and for a small number of servers use this
          // thread.
          Executor executor = servers.size() > 64 ? fetchExectuor : MoreExecutors.directExecutor();

          List<Future<?>> futures = new ArrayList<>();

          for (final String server : servers) {
            if (addressPredicate.test(server)) {
              final ServiceLockPath slp =
                  parse(Optional.of(serverType), typePath + "/" + group + "/" + server);
              if (!withLock || slp.getType().equals(Constants.ZDEADTSERVERS)) {
                // Dead TServers don't have lock data
                results.add(slp);
              } else {
                // Execute reads to zookeeper to get lock info in parallel. The zookeeper client
                // has a single shared connection to a server so this will not create lots of
                // connections, it will place multiple outgoing request on that single zookeeper
                // connection at the same time though.
                var futureTask = new FutureTask<>(() -> {
                  final ZcStat stat = new ZcStat();
                  Optional<ServiceLockData> sld = ServiceLock.getLockData(zooCache, slp, stat);
                  if (sld.isPresent()) {
                    results.add(slp);
                  }
                  return null;
                });
                executor.execute(futureTask);
                futures.add(futureTask);
              }
            }
          }

          // wait for futures to complete and check for errors
          for (var future : futures) {
            try {
              future.get();
            } catch (InterruptedException | ExecutionException e) {
              throw new IllegalStateException(e);
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

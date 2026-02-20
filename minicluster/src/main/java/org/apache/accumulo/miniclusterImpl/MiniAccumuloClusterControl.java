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
package org.apache.accumulo.miniclusterImpl;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.server.conf.store.ResourceGroupPropKey;
import org.apache.accumulo.server.util.ZooZap;
import org.apache.accumulo.server.util.adminCommand.StopAll;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MiniAccumuloClusterControl implements ClusterControl {
  private static final Logger log = LoggerFactory.getLogger(MiniAccumuloClusterControl.class);

  protected MiniAccumuloClusterImpl cluster;

  Process zooKeeperProcess = null;
  Process managerProcess = null;
  Process gcProcess = null;
  Process monitor = null;
  final Map<String,List<Process>> tabletServerProcesses = new HashMap<>();
  final Map<String,List<Process>> scanServerProcesses = new HashMap<>();
  final Map<String,List<Process>> compactorProcesses = new HashMap<>();

  public MiniAccumuloClusterControl(MiniAccumuloClusterImpl cluster) {
    requireNonNull(cluster);
    this.cluster = cluster;
  }

  public void start(ServerType server) throws IOException {
    start(server, null);
  }

  @Override
  public int exec(Class<?> clz, String[] args) throws IOException {
    Process p = cluster.exec(clz, args).getProcess();
    int exitCode;
    try {
      exitCode = p.waitFor();
    } catch (InterruptedException e) {
      log.warn("Interrupted waiting for process to exit", e);
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    return exitCode;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input file name")
  @Override
  public Entry<Integer,String> execWithStdout(Class<?> clz, String[] args) throws IOException {
    ProcessInfo pi = cluster.exec(clz, args);
    int exitCode;
    try {
      exitCode = pi.getProcess().waitFor();
    } catch (InterruptedException e) {
      log.warn("Interrupted waiting for process to exit", e);
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }

    return Maps.immutableEntry(exitCode, pi.readStdOut());
  }

  @Override
  public void adminStopAll() throws IOException {
    Process p = cluster.exec(StopAll.class).getProcess();
    try {
      p.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    if (p.exitValue() != 0) {
      throw new IOException("Failed to run `accumulo admin stopAll`");
    }
    stopAllServers(ServerType.COMPACTOR);
    stopAllServers(ServerType.SCAN_SERVER);
  }

  @Override
  public synchronized void startAllServers(ServerType server) throws IOException {
    start(server, null);
  }

  @Override
  public synchronized void start(ServerType server, String hostname) throws IOException {
    start(server, Collections.emptyMap(), Integer.MAX_VALUE, null, new String[] {});
  }

  public synchronized void start(ServerType server, Map<String,String> configOverrides, int limit)
      throws IOException {
    start(server, configOverrides, limit, null, new String[] {});
  }

  public synchronized void start(ServerType server, Map<String,String> configOverrides, int limit,
      Class<?> classOverride, String... args) throws IOException {
    if (limit <= 0) {
      return;
    }

    switch (server) {
      case TABLET_SERVER:
        synchronized (tabletServerProcesses) {
          Map<String,Integer> tserverGroups =
              cluster.getConfig().getClusterServerConfiguration().getTabletServerConfiguration();
          for (Entry<String,Integer> e : tserverGroups.entrySet()) {
            final String rg = e.getKey();
            try {
              ResourceGroupPropKey.of(ResourceGroupId.of(rg))
                  .createZNode(cluster.getServerContext().getZooSession().asReaderWriter());
            } catch (KeeperException | InterruptedException e1) {
              throw new IllegalStateException(
                  "Unable to create resource group configuration node for " + rg);
            }
            List<Process> processes =
                tabletServerProcesses.computeIfAbsent(rg, k -> new ArrayList<>());
            Class<?> classToUse = classOverride != null ? classOverride
                : cluster.getConfig().getServerClass(server, rg);
            int count = 0;
            for (int i = processes.size(); count < limit && i < e.getValue(); i++, ++count) {
              processes.add(cluster
                  ._exec(classToUse, server, configOverrides,
                      ArrayUtils.addAll(args, "-o", Property.TSERV_GROUP_NAME.getKey() + "=" + rg))
                  .getProcess());
            }
          }
        }
        break;
      case MANAGER:
        if (managerProcess == null) {
          Class<?> classToUse = classOverride != null ? classOverride
              : cluster.getConfig().getServerClass(server, Constants.DEFAULT_RESOURCE_GROUP_NAME);
          managerProcess = cluster._exec(classToUse, server, configOverrides, args).getProcess();
        }
        break;
      case ZOOKEEPER:
        if (zooKeeperProcess == null) {
          Class<?> classToUse = classOverride != null ? classOverride
              : cluster.getConfig().getServerClass(server, Constants.DEFAULT_RESOURCE_GROUP_NAME);
          zooKeeperProcess = cluster
              ._exec(classToUse, server, configOverrides, cluster.getZooCfgFile().getAbsolutePath())
              .getProcess();
        }
        break;
      case GARBAGE_COLLECTOR:
        if (gcProcess == null) {
          Class<?> classToUse = classOverride != null ? classOverride
              : cluster.getConfig().getServerClass(server, Constants.DEFAULT_RESOURCE_GROUP_NAME);
          gcProcess = cluster._exec(classToUse, server, configOverrides, args).getProcess();
        }
        break;
      case MONITOR:
        if (monitor == null) {
          Class<?> classToUse = classOverride != null ? classOverride
              : cluster.getConfig().getServerClass(server, Constants.DEFAULT_RESOURCE_GROUP_NAME);
          monitor = cluster._exec(classToUse, server, configOverrides, args).getProcess();
        }
        break;
      case SCAN_SERVER:
        synchronized (scanServerProcesses) {
          Map<String,Integer> sserverGroups =
              cluster.getConfig().getClusterServerConfiguration().getScanServerConfiguration();
          for (Entry<String,Integer> e : sserverGroups.entrySet()) {
            final String rg = e.getKey();
            try {
              ResourceGroupPropKey.of(ResourceGroupId.of(rg))
                  .createZNode(cluster.getServerContext().getZooSession().asReaderWriter());
            } catch (KeeperException | InterruptedException e1) {
              throw new IllegalStateException(
                  "Unable to create resource group configuration node for " + rg);
            }
            List<Process> processes =
                scanServerProcesses.computeIfAbsent(rg, k -> new ArrayList<>());
            Class<?> classToUse = classOverride != null ? classOverride
                : cluster.getConfig().getServerClass(server, rg);
            int count = 0;
            for (int i = processes.size(); count < limit && i < e.getValue(); i++, ++count) {
              processes.add(cluster
                  ._exec(classToUse, server, configOverrides,
                      ArrayUtils.addAll(args, "-o", Property.SSERV_GROUP_NAME.getKey() + "=" + rg))
                  .getProcess());
            }
          }
        }
        break;
      case COMPACTOR:
        synchronized (compactorProcesses) {
          Map<String,Integer> compactorGroups =
              cluster.getConfig().getClusterServerConfiguration().getCompactorConfiguration();
          for (Entry<String,Integer> e : compactorGroups.entrySet()) {
            final String rg = e.getKey();
            try {
              ResourceGroupPropKey.of(ResourceGroupId.of(rg))
                  .createZNode(cluster.getServerContext().getZooSession().asReaderWriter());
            } catch (KeeperException | InterruptedException e1) {
              throw new IllegalStateException(
                  "Unable to create resource group configuration node for " + rg);
            }
            List<Process> processes =
                compactorProcesses.computeIfAbsent(rg, k -> new ArrayList<>());
            Class<?> classToUse = classOverride != null ? classOverride
                : cluster.getConfig().getServerClass(server, rg);
            int count = 0;
            // Override the Compactor classToUse for the default resource group. In the cases
            // where the ExternalDoNothingCompactor and MemoryConsumingCompactor are used, they
            // should be used in a non-default resource group. We need the default resource
            // group to compact normally for the root and metadata tables.
            for (int i = processes.size(); count < limit && i < e.getValue(); i++, ++count) {
              processes.add(cluster._exec(classToUse, server, configOverrides,
                  ArrayUtils.addAll(args, "-o", Property.COMPACTOR_GROUP_NAME.getKey() + "=" + rg))
                  .getProcess());
            }
          }
        }
        break;
      default:
        throw new UnsupportedOperationException("Cannot start process for " + server);
    }
  }

  @Override
  public synchronized void stopAllServers(ServerType server) throws IOException {
    stop(server);
  }

  public void stop(ServerType server) throws IOException {
    stop(server, null);
  }

  public void stopCompactorGroup(String compactorResourceGroup) {
    synchronized (compactorProcesses) {
      var group = compactorProcesses.get(compactorResourceGroup);
      if (group == null) {
        return;
      }
      cluster.stopProcessesWithTimeout(ServerType.COMPACTOR, group, 30, TimeUnit.SECONDS);
      compactorProcesses.remove(compactorResourceGroup);
    }
  }

  public void stopScanServerGroup(String sserverResourceGroup) {
    synchronized (scanServerProcesses) {
      var group = scanServerProcesses.get(sserverResourceGroup);
      if (group == null) {
        return;
      }
      cluster.stopProcessesWithTimeout(ServerType.SCAN_SERVER, group, 30, TimeUnit.SECONDS);
      scanServerProcesses.remove(sserverResourceGroup);
    }
  }

  public void stopTabletServerGroup(String tserverResourceGroup) {
    synchronized (tabletServerProcesses) {
      var group = tabletServerProcesses.get(tserverResourceGroup);
      if (group == null) {
        return;
      }
      cluster.stopProcessesWithTimeout(ServerType.TABLET_SERVER, group, 30, TimeUnit.SECONDS);
      tabletServerProcesses.remove(tserverResourceGroup);
    }
  }

  @Override
  public synchronized void stop(ServerType server, String hostname) throws IOException {
    switch (server) {
      case MANAGER:
        if (managerProcess != null) {
          try {
            cluster.stopProcessWithTimeout(managerProcess, 30, TimeUnit.SECONDS);
            try {
              System.setProperty(SiteConfiguration.ACCUMULO_PROPERTIES_PROPERTY,
                  "file://" + this.cluster.getAccumuloPropertiesPath());
              new ZooZap().execute(new String[] {"-manager"});
            } catch (Exception e) {
              log.error("Error zapping Manager zookeeper lock", e);
            }
          } catch (ExecutionException | TimeoutException e) {
            log.warn("Manager did not fully stop after 30 seconds", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            managerProcess = null;
          }
        }
        break;
      case GARBAGE_COLLECTOR:
        if (gcProcess != null) {
          try {
            cluster.stopProcessWithTimeout(gcProcess, 30, TimeUnit.SECONDS);
          } catch (ExecutionException | TimeoutException e) {
            log.warn("Garbage collector did not fully stop after 30 seconds", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            gcProcess = null;
          }
        }
        break;
      case ZOOKEEPER:
        if (zooKeeperProcess != null) {
          try {
            cluster.stopProcessWithTimeout(zooKeeperProcess, 30, TimeUnit.SECONDS);
          } catch (ExecutionException | TimeoutException e) {
            log.warn("ZooKeeper did not fully stop after 30 seconds", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            zooKeeperProcess = null;
          }
        }
        break;
      case TABLET_SERVER:
        synchronized (tabletServerProcesses) {
          try {
            final List<Process> procs = new ArrayList<>();
            tabletServerProcesses.values().forEach(procs::addAll);
            cluster.stopProcessesWithTimeout(ServerType.TABLET_SERVER, procs, 30, TimeUnit.SECONDS);
          } finally {
            tabletServerProcesses.clear();
          }
        }
        break;
      case MONITOR:
        if (monitor != null) {
          try {
            cluster.stopProcessWithTimeout(monitor, 30, TimeUnit.SECONDS);
          } catch (ExecutionException | TimeoutException e) {
            log.warn("Monitor did not fully stop after 30 seconds", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            monitor = null;
          }
        }
        break;
      case SCAN_SERVER:
        synchronized (scanServerProcesses) {
          try {
            final List<Process> procs = new ArrayList<>();
            scanServerProcesses.values().forEach(procs::addAll);
            cluster.stopProcessesWithTimeout(ServerType.SCAN_SERVER, procs, 30, TimeUnit.SECONDS);
          } finally {
            scanServerProcesses.clear();
          }
        }
        break;
      case COMPACTOR:
        synchronized (compactorProcesses) {
          try {
            final List<Process> procs = new ArrayList<>();
            compactorProcesses.values().forEach(procs::addAll);
            cluster.stopProcessesWithTimeout(ServerType.COMPACTOR, procs, 30, TimeUnit.SECONDS);
          } finally {
            compactorProcesses.clear();
          }
        }
        break;
      default:
        throw new UnsupportedOperationException("ServerType is not yet supported " + server);
    }

  }

  @Override
  public void signal(ServerType server, String hostname, String signal) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void suspend(ServerType server, String hostname) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resume(ServerType server, String hostname) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void killProcess(ServerType type, ProcessReference procRef)
      throws ProcessNotFoundException, InterruptedException {
    boolean found = false;
    switch (type) {
      case MANAGER:
        if (procRef.getProcess().equals(managerProcess)) {
          try {
            cluster.stopProcessWithTimeout(managerProcess, 30, TimeUnit.SECONDS);
          } catch (ExecutionException | TimeoutException e) {
            log.warn("Manager did not fully stop after 30 seconds", e);
          }
          managerProcess = null;
          found = true;
        }
        break;
      case TABLET_SERVER:
        synchronized (tabletServerProcesses) {
          for (List<Process> plist : tabletServerProcesses.values()) {
            Iterator<Process> iter = plist.iterator();
            while (!found && iter.hasNext()) {
              Process process = iter.next();
              if (procRef.getProcess().equals(process)) {
                iter.remove();
                try {
                  cluster.stopProcessWithTimeout(process, 30, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException e) {
                  log.warn("TabletServer did not fully stop after 30 seconds", e);
                }
                found = true;
                break;
              }
            }
          }
        }
        break;
      case ZOOKEEPER:
        if (procRef.getProcess().equals(zooKeeperProcess)) {
          try {
            cluster.stopProcessWithTimeout(zooKeeperProcess, 30, TimeUnit.SECONDS);
          } catch (ExecutionException | TimeoutException e) {
            log.warn("ZooKeeper did not fully stop after 30 seconds", e);
          }
          zooKeeperProcess = null;
          found = true;
        }
        break;
      case GARBAGE_COLLECTOR:
        if (procRef.getProcess().equals(gcProcess)) {
          try {
            cluster.stopProcessWithTimeout(gcProcess, 30, TimeUnit.SECONDS);
          } catch (ExecutionException | TimeoutException e) {
            log.warn("GarbageCollector did not fully stop after 30 seconds", e);
          }
          gcProcess = null;
          found = true;
        }
        break;
      case SCAN_SERVER:
        synchronized (scanServerProcesses) {
          for (List<Process> plist : scanServerProcesses.values()) {
            Iterator<Process> iter = plist.iterator();
            while (!found && iter.hasNext()) {
              Process process = iter.next();
              if (procRef.getProcess().equals(process)) {
                iter.remove();
                try {
                  cluster.stopProcessWithTimeout(process, 30, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException e) {
                  log.warn("TabletServer did not fully stop after 30 seconds", e);
                }
                found = true;
                break;
              }
            }
          }
        }
        break;
      case COMPACTOR:
        synchronized (compactorProcesses) {
          for (List<Process> plist : compactorProcesses.values()) {
            Iterator<Process> iter = plist.iterator();
            while (!found && iter.hasNext()) {
              Process process = iter.next();
              if (procRef.getProcess().equals(process)) {
                iter.remove();
                try {
                  cluster.stopProcessWithTimeout(process, 30, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException e) {
                  log.warn("TabletServer did not fully stop after 30 seconds", e);
                }
                found = true;
                break;
              }
            }
          }
        }
        break;
      default:
        // Ignore the other types MAC currently doesn't automateE
        found = true;
        break;
    }
    if (!found) {
      throw new ProcessNotFoundException();
    }
  }

  @Override
  public void kill(ServerType server, String hostname) throws IOException {
    stop(server, hostname);
  }

  public List<Process> getCompactors(String resourceGroup) {
    return compactorProcesses.get(resourceGroup);
  }

  public List<Process> getTabletServers(String resourceGroup) {
    return tabletServerProcesses.get(resourceGroup);
  }

  public void refreshProcesses(ServerType type) {
    switch (type) {
      case COMPACTOR:
        compactorProcesses.forEach((k, v) -> v.removeIf(process -> !process.isAlive()));
        break;
      case GARBAGE_COLLECTOR:
        if (!gcProcess.isAlive()) {
          gcProcess = null;
        }
        break;
      case MANAGER:
        if (!managerProcess.isAlive()) {
          managerProcess = null;
        }
        break;
      case MONITOR:
        if (!monitor.isAlive()) {
          monitor = null;
        }
        break;
      case SCAN_SERVER:
        scanServerProcesses.forEach((k, v) -> v.removeIf(process -> !process.isAlive()));
        break;
      case TABLET_SERVER:
        tabletServerProcesses.forEach((k, v) -> v.removeIf(process -> !process.isAlive()));
        break;
      case ZOOKEEPER:
        if (!zooKeeperProcess.isAlive()) {
          zooKeeperProcess = null;
        }
        break;
      default:
        throw new IllegalArgumentException("Unhandled type: " + type);
    }
  }

  public Set<Process> getProcesses(ServerType type) {
    switch (type) {
      case COMPACTOR:
        Set<Process> cprocesses = new HashSet<>();
        compactorProcesses.values().forEach(list -> list.forEach(cprocesses::add));
        return cprocesses;
      case GARBAGE_COLLECTOR:
        return gcProcess == null ? Set.of() : Set.of(gcProcess);
      case MANAGER:
        return managerProcess == null ? Set.of() : Set.of(managerProcess);
      case MONITOR:
        return monitor == null ? Set.of() : Set.of(monitor);
      case SCAN_SERVER:
        Set<Process> sprocesses = new HashSet<>();
        scanServerProcesses.values().forEach(list -> list.forEach(sprocesses::add));
        return sprocesses;
      case TABLET_SERVER:
        Set<Process> tprocesses = new HashSet<>();
        tabletServerProcesses.values().forEach(list -> list.forEach(tprocesses::add));
        return tprocesses;
      case ZOOKEEPER:
        return zooKeeperProcess == null ? Set.of() : Set.of(zooKeeperProcess);
      default:
        throw new IllegalArgumentException("Unhandled type: " + type);
    }
  }
}

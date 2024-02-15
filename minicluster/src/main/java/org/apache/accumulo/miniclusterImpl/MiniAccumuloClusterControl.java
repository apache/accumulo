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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.tserver.ScanServer;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
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
    Process p = cluster.exec(Admin.class, "stopAll").getProcess();
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
    start(server, Collections.emptyMap(), Integer.MAX_VALUE, null);
  }

  public synchronized void start(ServerType server, Map<String,String> configOverrides, int limit)
      throws IOException {
    start(server, configOverrides, limit, null);
  }

  public synchronized void start(ServerType server, Map<String,String> configOverrides, int limit,
      Class<?> classOverride) throws IOException {
    if (limit <= 0) {
      return;
    }

    Class<?> classToUse;
    if (classOverride != null) {
      classToUse = classOverride;
    } else {
      switch (server) {
        case COMPACTOR:
          classToUse = Compactor.class;
          break;
        case SCAN_SERVER:
          classToUse = ScanServer.class;
          break;
        case TABLET_SERVER:
          classToUse = TabletServer.class;
          break;
        case GARBAGE_COLLECTOR:
          classToUse = SimpleGarbageCollector.class;
          break;
        case MANAGER:
          classToUse = Manager.class;
          break;
        case MONITOR:
          classToUse = Monitor.class;
          break;
        case ZOOKEEPER:
          classToUse = ZooKeeperServerMain.class;
          break;
        default:
          throw new IllegalArgumentException("Unhandled server type: " + server);
      }
    }

    switch (server) {
      case TABLET_SERVER:
        synchronized (tabletServerProcesses) {
          Map<String,Integer> tserverGroups =
              cluster.getConfig().getClusterServerConfiguration().getTabletServerConfiguration();
          for (Entry<String,Integer> e : tserverGroups.entrySet()) {
            List<Process> processes =
                tabletServerProcesses.computeIfAbsent(e.getKey(), k -> new ArrayList<>());
            int count = 0;
            for (int i = processes.size(); count < limit && i < e.getValue(); i++, ++count) {
              processes.add(cluster._exec(classToUse, server, configOverrides, "-o",
                  Property.TSERV_GROUP_NAME.getKey() + "=" + e.getKey()).getProcess());
            }
          }
        }
        break;
      case MANAGER:
        if (managerProcess == null) {
          managerProcess = cluster._exec(classToUse, server, configOverrides).getProcess();
        }
        break;
      case ZOOKEEPER:
        if (zooKeeperProcess == null) {
          zooKeeperProcess = cluster
              ._exec(classToUse, server, configOverrides, cluster.getZooCfgFile().getAbsolutePath())
              .getProcess();
        }
        break;
      case GARBAGE_COLLECTOR:
        if (gcProcess == null) {
          gcProcess = cluster._exec(classToUse, server, configOverrides).getProcess();
        }
        break;
      case MONITOR:
        if (monitor == null) {
          monitor = cluster._exec(classToUse, server, configOverrides).getProcess();
        }
        break;
      case SCAN_SERVER:
        synchronized (scanServerProcesses) {
          Map<String,Integer> sserverGroups =
              cluster.getConfig().getClusterServerConfiguration().getScanServerConfiguration();
          for (Entry<String,Integer> e : sserverGroups.entrySet()) {
            List<Process> processes =
                scanServerProcesses.computeIfAbsent(e.getKey(), k -> new ArrayList<>());
            int count = 0;
            for (int i = processes.size(); count < limit && i < e.getValue(); i++, ++count) {
              processes.add(cluster._exec(classToUse, server, configOverrides, "-o",
                  Property.SSERV_GROUP_NAME.getKey() + "=" + e.getKey()).getProcess());
            }
          }
        }
        break;
      case COMPACTOR:
        synchronized (compactorProcesses) {
          Map<String,Integer> compactorGroups =
              cluster.getConfig().getClusterServerConfiguration().getCompactorConfiguration();
          for (Entry<String,Integer> e : compactorGroups.entrySet()) {
            List<Process> processes =
                compactorProcesses.computeIfAbsent(e.getKey(), k -> new ArrayList<>());
            int count = 0;
            for (int i = processes.size(); count < limit && i < e.getValue(); i++, ++count) {
              processes.add(cluster._exec(classToUse, server, configOverrides, "-o",
                  Property.COMPACTOR_GROUP_NAME.getKey() + "=" + e.getKey()).getProcess());
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
      group.forEach(process -> {
        try {
          cluster.stopProcessWithTimeout(process, 30, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException e) {
          log.warn("Compactor did not fully stop after 30 seconds", e);
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      compactorProcesses.remove(compactorResourceGroup);
    }
  }

  @Override
  public synchronized void stop(ServerType server, String hostname) throws IOException {
    switch (server) {
      case MANAGER:
        if (managerProcess != null) {
          try {
            cluster.stopProcessWithTimeout(managerProcess, 30, TimeUnit.SECONDS);
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
            tabletServerProcesses.values().forEach(list -> {
              list.forEach(process -> {
                try {
                  cluster.stopProcessWithTimeout(process, 30, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException e) {
                  log.warn("TabletServer did not fully stop after 30 seconds", e);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              });
            });
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
            scanServerProcesses.values().forEach(list -> {
              list.forEach(process -> {
                try {
                  cluster.stopProcessWithTimeout(process, 30, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException e) {
                  log.warn("TabletServer did not fully stop after 30 seconds", e);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              });
            });
          } finally {
            scanServerProcesses.clear();
          }
        }
        break;
      case COMPACTOR:
        synchronized (compactorProcesses) {
          try {
            compactorProcesses.values().forEach(list -> {
              list.forEach(process -> {
                try {
                  cluster.stopProcessWithTimeout(process, 30, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException e) {
                  log.warn("TabletServer did not fully stop after 30 seconds", e);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              });
            });
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

  public List<Process> getTabletServers(String resourceGroup) {
    return tabletServerProcesses.get(resourceGroup);
  }

}

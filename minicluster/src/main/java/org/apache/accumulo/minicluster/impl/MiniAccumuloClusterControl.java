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
package org.apache.accumulo.minicluster.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ZooKeeperServerMain;

import com.google.common.base.Preconditions;

/**
 *
 */
public class MiniAccumuloClusterControl implements ClusterControl {
  private static final Logger log = Logger.getLogger(MiniAccumuloClusterControl.class);

  protected MiniAccumuloClusterImpl cluster;

  Process zooKeeperProcess = null;
  Process masterProcess = null;
  Process gcProcess = null;
  List<Process> tabletServerProcesses = Collections.synchronizedList(new ArrayList<Process>());

  public MiniAccumuloClusterControl(MiniAccumuloClusterImpl cluster) {
    Preconditions.checkNotNull(cluster);
    this.cluster = cluster;
  }

  public void start(ServerType server) throws IOException {
    start(server, null);
  }

  @Override
  public int exec(Class<?> clz, String[] args) throws IOException {
    Process p = cluster.exec(clz, args);
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

  @Override
  public void adminStopAll() throws IOException {
    Process p = cluster.exec(Admin.class, "stopAll");
    try {
      p.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    if (0 != p.exitValue()) {
      throw new IOException("Failed to run `accumulo admin stopAll`");
    }
  }

  @Override
  public synchronized void startAllServers(ServerType server) throws IOException {
    start(server, null);
  }

  @Override
  public synchronized void start(ServerType server, String hostname) throws IOException {
    switch (server) {
      case TABLET_SERVER:
        synchronized (tabletServerProcesses) {
          for (int i = tabletServerProcesses.size(); i < cluster.getConfig().getNumTservers(); i++) {
            tabletServerProcesses.add(cluster._exec(TabletServer.class, server));
          }
        }
        break;
      case MASTER:
        if (null == masterProcess) {
          masterProcess = cluster._exec(Master.class, server);
        }
        break;
      case ZOOKEEPER:
        if (null == zooKeeperProcess) {
          zooKeeperProcess = cluster._exec(ZooKeeperServerMain.class, server, cluster.getZooCfgFile().getAbsolutePath());
        }
        break;
      case GARBAGE_COLLECTOR:
        if (null == gcProcess) {
          gcProcess = cluster._exec(SimpleGarbageCollector.class, server);
        }
        break;
      // TODO Enabled Monitor and Tracer for proper mini-ness
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

  @Override
  public synchronized void stop(ServerType server, String hostname) throws IOException {
    switch (server) {
      case MASTER:
        if (null != masterProcess) {
          try {
            cluster.stopProcessWithTimeout(masterProcess, 30, TimeUnit.SECONDS);
          } catch (ExecutionException e) {
            log.warn("Master did not fully stop after 30 seconds", e);
          } catch (TimeoutException e) {
            log.warn("Master did not fully stop after 30 seconds", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            masterProcess = null;
          }
        }
        break;
      case GARBAGE_COLLECTOR:
        if (null != gcProcess) {
          try {
            cluster.stopProcessWithTimeout(gcProcess, 30, TimeUnit.SECONDS);
          } catch (ExecutionException e) {
            log.warn("Garbage collector did not fully stop after 30 seconds", e);
          } catch (TimeoutException e) {
            log.warn("Garbage collector did not fully stop after 30 seconds", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            gcProcess = null;
          }
        }
        break;
      case ZOOKEEPER:
        if (null != zooKeeperProcess) {
          try {
            cluster.stopProcessWithTimeout(zooKeeperProcess, 30, TimeUnit.SECONDS);
          } catch (ExecutionException e) {
            log.warn("ZooKeeper did not fully stop after 30 seconds", e);
          } catch (TimeoutException e) {
            log.warn("ZooKeeper did not fully stop after 30 seconds", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            zooKeeperProcess = null;
          }
        }
        break;
      case TABLET_SERVER:
        if (tabletServerProcesses != null) {
          synchronized (tabletServerProcesses) {
            try {
              for (Process tserver : tabletServerProcesses) {
                try {
                  cluster.stopProcessWithTimeout(tserver, 30, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                  log.warn("TabletServer did not fully stop after 30 seconds", e);
                } catch (TimeoutException e) {
                  log.warn("TabletServer did not fully stop after 30 seconds", e);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
            } finally {
              tabletServerProcesses.clear();
            }
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

  public void killProcess(ServerType type, ProcessReference proc) throws ProcessNotFoundException, InterruptedException {
    boolean found = false;
    switch (type) {
      case MASTER:
        if (proc.equals(masterProcess)) {
          try {
            cluster.stopProcessWithTimeout(masterProcess, 30, TimeUnit.SECONDS);
          } catch (ExecutionException e) {
            log.warn("Master did not fully stop after 30 seconds", e);
          } catch (TimeoutException e) {
            log.warn("Master did not fully stop after 30 seconds", e);
          }
          masterProcess = null;
          found = true;
        }
        break;
      case TABLET_SERVER:
        synchronized (tabletServerProcesses) {
          for (Process tserver : tabletServerProcesses) {
            if (proc.equals(tserver)) {
              tabletServerProcesses.remove(tserver);
              try {
                cluster.stopProcessWithTimeout(tserver, 30, TimeUnit.SECONDS);
              } catch (ExecutionException e) {
                log.warn("TabletServer did not fully stop after 30 seconds", e);
              } catch (TimeoutException e) {
                log.warn("TabletServer did not fully stop after 30 seconds", e);
              }
              found = true;
              break;
            }
          }
        }
        break;
      case ZOOKEEPER:
        if (proc.equals(zooKeeperProcess)) {
          try {
            cluster.stopProcessWithTimeout(zooKeeperProcess, 30, TimeUnit.SECONDS);
          } catch (ExecutionException e) {
            log.warn("ZooKeeper did not fully stop after 30 seconds", e);
          } catch (TimeoutException e) {
            log.warn("ZooKeeper did not fully stop after 30 seconds", e);
          }
          zooKeeperProcess = null;
          found = true;
        }
        break;
      case GARBAGE_COLLECTOR:
        if (proc.equals(gcProcess)) {
          try {
            cluster.stopProcessWithTimeout(gcProcess, 30, TimeUnit.SECONDS);
          } catch (ExecutionException e) {
            log.warn("GarbageCollector did not fully stop after 30 seconds", e);
          } catch (TimeoutException e) {
            log.warn("GarbageCollector did not fully stop after 30 seconds", e);
          }
          gcProcess = null;
          found = true;
        }
        break;
      default:
        // Ignore the other types MAC currently doesn't automateE
        found = true;
        break;
    }
    if (!found)
      throw new ProcessNotFoundException();
  }

  @Override
  public void kill(ServerType server, String hostname) throws IOException {
    stop(server, hostname);
  }

}

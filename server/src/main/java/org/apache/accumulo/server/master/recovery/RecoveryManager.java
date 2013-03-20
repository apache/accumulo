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
package org.apache.accumulo.server.master.recovery;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.trace.TraceFileSystem;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

public class RecoveryManager {
  
  private static Logger log = Logger.getLogger(RecoveryManager.class);
  
  private Map<String,Long> recoveryDelay = new HashMap<String,Long>();
  private Set<String> closeTasksQueued = new HashSet<String>();
  private Set<String> sortsQueued = new HashSet<String>();
  private ScheduledExecutorService executor;
  private Master master;
  private ZooCache zooCache;
  
  public RecoveryManager(Master master) {
    this.master = master;
    executor = Executors.newScheduledThreadPool(4, new NamingThreadFactory("Walog sort starter "));
    zooCache = new ZooCache();
    try {
      List<String> workIDs = new DistributedWorkQueue(ZooUtil.getRoot(master.getInstance()) + Constants.ZRECOVERY).getWorkQueued();
      sortsQueued.addAll(workIDs);
    } catch (Exception e) {
      log.warn(e, e);
    }
  }

  private class LogSortTask implements Runnable {
    private String filename;
    private String host;
    private LogCloser closer;
    
    public LogSortTask(LogCloser closer, String host, String filename) {
      this.closer = closer;
      this.host = host;
      this.filename = filename;
    }

    @Override
    public void run() {
      boolean rescheduled = false;
      try {
        FileSystem localFs = master.getFileSystem();
        if (localFs instanceof TraceFileSystem)
          localFs = ((TraceFileSystem) localFs).getImplementation();
      
        long time = closer.close(localFs, getSource(host, filename));
      
        if (time > 0) {
          executor.schedule(this, time, TimeUnit.MILLISECONDS);
          rescheduled = true;
        } else {
          initiateSort(host, filename);
        }
      } catch (FileNotFoundException e) {
        log.debug("Unable to initate log sort for " + filename + ": " + e);
      } catch (Exception e) {
        log.warn("Failed to initiate log sort " + filename, e);
      } finally {
        if (!rescheduled) {
          synchronized (RecoveryManager.this) {
            closeTasksQueued.remove(filename);
          }
        }
      }
    }
    
  }
  
  private void initiateSort(String host, final String file) throws KeeperException, InterruptedException {
    String source = getSource(host, file).toString();
    new DistributedWorkQueue(ZooUtil.getRoot(master.getInstance()) + Constants.ZRECOVERY).addWork(file, source.getBytes());
    
    synchronized (this) {
      sortsQueued.add(file);
    }

    final String path = ZooUtil.getRoot(master.getInstance()) + Constants.ZRECOVERY + "/" + file;
    log.info("Created zookeeper entry " + path + " with data " + source);
  }
  
  private Path getSource(String server, String file) {
    String source = Constants.getWalDirectory(master.getSystemConfiguration()) + "/" + server + "/" + file;
    if (server.contains(":")) {
      // old-style logger log, copied from local file systems by tservers, unsorted into the wal base dir
      source = Constants.getWalDirectory(master.getSystemConfiguration()) + "/" + file;
    }
    return new Path(source);
  }

  public boolean recoverLogs(KeyExtent extent, Collection<Collection<String>> walogs) throws IOException {
    boolean recoveryNeeded = false;
    for (Collection<String> logs : walogs) {
      for (String walog : logs) {
        String parts[] = walog.split("/");
        String host = parts[0];
        String filename = parts[1];
        
        boolean sortQueued;
        synchronized (this) {
          sortQueued = sortsQueued.contains(filename);
        }
        
        if (sortQueued && zooCache.get(ZooUtil.getRoot(master.getInstance()) + Constants.ZRECOVERY + "/" + filename) == null) {
          synchronized (this) {
            sortsQueued.remove(filename);
          }
        }

        if (master.getFileSystem().exists(new Path(Constants.getRecoveryDir(master.getSystemConfiguration()) + "/" + filename + "/finished"))) {
          synchronized (this) {
            closeTasksQueued.remove(filename);
            recoveryDelay.remove(filename);
            sortsQueued.remove(filename);
          }
          continue;
        }
        
        recoveryNeeded = true;
        synchronized (this) {
          if (!closeTasksQueued.contains(filename) && !sortsQueued.contains(filename)) {
            AccumuloConfiguration aconf = master.getConfiguration().getConfiguration();
            LogCloser closer = Master.createInstanceFromPropertyName(aconf, Property.MASTER_WALOG_CLOSER_IMPLEMETATION, LogCloser.class,
                new HadoopLogCloser());
            Long delay = recoveryDelay.get(filename);
            if (delay == null) {
              delay = master.getSystemConfiguration().getTimeInMillis(Property.MASTER_RECOVERY_DELAY);
            } else {
              delay = Math.min(2 * delay, 1000 * 60 * 5l);
            }

            log.info("Starting recovery of " + filename + " (in : " + (delay / 1000) + "s) created for " + host + ", tablet " + extent + " holds a reference");

            executor.schedule(new LogSortTask(closer, host, filename), delay, TimeUnit.MILLISECONDS);
            closeTasksQueued.add(filename);
            recoveryDelay.put(filename, delay);
          }
        }
      }
    }
    return recoveryNeeded;
  }
}

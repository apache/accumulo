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
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.ZooCache;
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
    private String source;
    private String destination;
    private String sortId;
    private LogCloser closer;
    
    public LogSortTask(LogCloser closer, String source, String destination, String sortId) {
      this.closer = closer;
      this.source = source;
      this.destination = destination;
      this.sortId = sortId;
    }
    
    @Override
    public void run() {
      boolean rescheduled = false;
      try {
        
        long time = closer.close(master, master.getFileSystem(), new Path(source));
        
        if (time > 0) {
          executor.schedule(this, time, TimeUnit.MILLISECONDS);
          rescheduled = true;
        } else {
          initiateSort(sortId, source, destination);
        }
      } catch (FileNotFoundException e) {
        log.debug("Unable to initate log sort for " + source + ": " + e);
      } catch (Exception e) {
        log.warn("Failed to initiate log sort " + source, e);
      } finally {
        if (!rescheduled) {
          synchronized (RecoveryManager.this) {
            closeTasksQueued.remove(sortId);
          }
        }
      }
    }
    
  }
  
  private void initiateSort(String sortId, String source, final String destination) throws KeeperException, InterruptedException, IOException {
    String work =  source + "|" + destination; 
    new DistributedWorkQueue(ZooUtil.getRoot(master.getInstance()) + Constants.ZRECOVERY).addWork(sortId, work.getBytes());
    
    synchronized (this) {
      sortsQueued.add(sortId);
    }
    
    final String path = ZooUtil.getRoot(master.getInstance()) + Constants.ZRECOVERY + "/" + sortId;
    log.info("Created zookeeper entry " + path + " with data " + work);
  }
  
  public boolean recoverLogs(KeyExtent extent, Collection<Collection<String>> walogs) throws IOException {
    boolean recoveryNeeded = false;
    ;
    for (Collection<String> logs : walogs) {
      for (String walog : logs) {
        String hostFilename[] = walog.split("/", 2);
        String host = hostFilename[0];
        String filename = hostFilename[1];
        String parts[] = filename.split("/");
        String sortId = parts[parts.length - 1];
        String dest = master.getFileSystem().choose(ServerConstants.getRecoveryDirs()) + "/" + sortId;
        log.debug("Recovering " + filename + " to " + dest);
        
        boolean sortQueued;
        synchronized (this) {
          sortQueued = sortsQueued.contains(sortId);
        }
        
        if (sortQueued && zooCache.get(ZooUtil.getRoot(master.getInstance()) + Constants.ZRECOVERY + "/" + sortId) == null) {
          synchronized (this) {
            sortsQueued.remove(sortId);
          }
        }

        if (master.getFileSystem().exists(new Path(dest, "finished"))) {
          synchronized (this) {
            closeTasksQueued.remove(sortId);
            recoveryDelay.remove(sortId);
            sortsQueued.remove(sortId);
          }
          continue;
        }
        
        recoveryNeeded = true;
        synchronized (this) {
          if (!closeTasksQueued.contains(sortId) && !sortsQueued.contains(sortId)) {
            AccumuloConfiguration aconf = master.getConfiguration().getConfiguration();
            LogCloser closer = Master.createInstanceFromPropertyName(aconf, Property.MASTER_WALOG_CLOSER_IMPLEMETATION, LogCloser.class, new HadoopLogCloser());
            Long delay = recoveryDelay.get(sortId);
            if (delay == null) {
              delay = master.getSystemConfiguration().getTimeInMillis(Property.MASTER_RECOVERY_DELAY);
            } else {
              delay = Math.min(2 * delay, 1000 * 60 * 5l);
            }
            
            log.info("Starting recovery of " + filename + " (in : " + (delay / 1000) + "s) created for " + host + ", tablet " + extent + " holds a reference");
            
            executor.schedule(new LogSortTask(closer, filename, dest, sortId), delay, TimeUnit.MILLISECONDS);
            closeTasksQueued.add(sortId);
            recoveryDelay.put(sortId, delay);
          }
        }
      }
    }
    return recoveryNeeded;
  }
}

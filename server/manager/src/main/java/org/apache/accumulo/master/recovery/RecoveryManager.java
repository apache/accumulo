/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.recovery;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.server.master.recovery.HadoopLogCloser;
import org.apache.accumulo.server.master.recovery.LogCloser;
import org.apache.accumulo.server.master.recovery.RecoveryPath;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class RecoveryManager {

  private static final Logger log = LoggerFactory.getLogger(RecoveryManager.class);

  private Map<String,Long> recoveryDelay = new HashMap<>();
  private Set<String> closeTasksQueued = new HashSet<>();
  private Set<String> sortsQueued = new HashSet<>();
  private Cache<Path,Boolean> existenceCache;
  private ScheduledExecutorService executor;
  private Master master;
  private ZooCache zooCache;

  public RecoveryManager(Master master, long timeToCacheExistsInMillis) {
    this.master = master;
    existenceCache =
        CacheBuilder.newBuilder().expireAfterWrite(timeToCacheExistsInMillis, TimeUnit.MILLISECONDS)
            .maximumWeight(10_000_000).weigher((path, exist) -> path.toString().length()).build();

    executor = ThreadPools.createScheduledExecutorService(4, "Walog sort starter ", false);
    zooCache = new ZooCache(master.getContext().getZooReaderWriter(), null);
    try {
      List<String> workIDs =
          new DistributedWorkQueue(master.getZooKeeperRoot() + Constants.ZRECOVERY,
              master.getConfiguration()).getWorkQueued();
      sortsQueued.addAll(workIDs);
    } catch (Exception e) {
      log.warn("{}", e.getMessage(), e);
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
        long time = closer.close(master.getConfiguration(), master.getContext().getHadoopConf(),
            master.getVolumeManager(), new Path(source));

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

  private void initiateSort(String sortId, String source, final String destination)
      throws KeeperException, InterruptedException {
    String work = source + "|" + destination;
    new DistributedWorkQueue(master.getZooKeeperRoot() + Constants.ZRECOVERY,
        master.getConfiguration()).addWork(sortId, work.getBytes(UTF_8));

    synchronized (this) {
      sortsQueued.add(sortId);
    }

    final String path = master.getZooKeeperRoot() + Constants.ZRECOVERY + "/" + sortId;
    log.info("Created zookeeper entry {} with data {}", path, work);
  }

  private boolean exists(final Path path) throws IOException {
    try {
      return existenceCache.get(path, () -> master.getVolumeManager().exists(path));
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  public boolean recoverLogs(KeyExtent extent, Collection<Collection<String>> walogs)
      throws IOException {
    boolean recoveryNeeded = false;

    for (Collection<String> logs : walogs) {
      for (String walog : logs) {

        Path switchedWalog = VolumeUtil.switchVolume(walog, FileType.WAL, ServerConstants
            .getVolumeReplacements(master.getConfiguration(), master.getContext().getHadoopConf()));
        if (switchedWalog != null) {
          // replaces the volume used for sorting, but do not change entry in metadata table. When
          // the tablet loads it will change the metadata table entry. If
          // the tablet has the same replacement config, then it will find the sorted log.
          log.info("Volume replaced {} -> {}", walog, switchedWalog);
          walog = switchedWalog.toString();
        }

        String[] parts = walog.split("/");
        String sortId = parts[parts.length - 1];
        String filename = new Path(walog).toString();
        String dest = RecoveryPath.getRecoveryPath(new Path(filename)).toString();

        boolean sortQueued;
        synchronized (this) {
          sortQueued = sortsQueued.contains(sortId);
        }

        if (sortQueued
            && zooCache.get(master.getZooKeeperRoot() + Constants.ZRECOVERY + "/" + sortId)
                == null) {
          synchronized (this) {
            sortsQueued.remove(sortId);
          }
        }

        if (exists(SortedLogState.getFinishedMarkerPath(dest))) {
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
            AccumuloConfiguration aconf = master.getConfiguration();
            LogCloser closer = Property.createInstanceFromPropertyName(aconf,
                Property.MASTER_WALOG_CLOSER_IMPLEMETATION, LogCloser.class, new HadoopLogCloser());
            Long delay = recoveryDelay.get(sortId);
            if (delay == null) {
              delay = aconf.getTimeInMillis(Property.MASTER_RECOVERY_DELAY);
            } else {
              delay = Math.min(2 * delay, 1000 * 60 * 5L);
            }

            log.info("Starting recovery of {} (in : {}s), tablet {} holds a reference", filename,
                (delay / 1000), extent);

            executor.schedule(new LogSortTask(closer, filename, dest, sortId), delay,
                TimeUnit.MILLISECONDS);
            closeTasksQueued.add(sortId);
            recoveryDelay.put(sortId, delay);
          }
        }
      }
    }
    return recoveryNeeded;
  }
}

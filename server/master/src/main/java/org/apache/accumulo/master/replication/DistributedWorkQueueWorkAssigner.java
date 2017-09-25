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
package org.apache.accumulo.master.replication;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTableOfflineException;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.WorkAssigner;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Common methods for {@link WorkAssigner}s
 */
public abstract class DistributedWorkQueueWorkAssigner implements WorkAssigner {
  private static final Logger log = LoggerFactory.getLogger(DistributedWorkQueueWorkAssigner.class);

  protected boolean isWorkRequired(Status status) {
    return StatusUtil.isWorkRequired(status);
  }

  protected Connector conn;
  protected AccumuloConfiguration conf;
  protected DistributedWorkQueue workQueue;
  protected int maxQueueSize;
  protected ZooCache zooCache;

  /*
   * Getters/setters for testing purposes
   */
  protected Connector getConnector() {
    return conn;
  }

  protected void setConnector(Connector conn) {
    this.conn = conn;
  }

  protected AccumuloConfiguration getConf() {
    return conf;
  }

  protected void setConf(AccumuloConfiguration conf) {
    this.conf = conf;
  }

  protected DistributedWorkQueue getWorkQueue() {
    return workQueue;
  }

  protected void setWorkQueue(DistributedWorkQueue workQueue) {
    this.workQueue = workQueue;
  }

  protected int getMaxQueueSize() {
    return maxQueueSize;
  }

  protected void setMaxQueueSize(int maxQueueSize) {
    this.maxQueueSize = maxQueueSize;
  }

  protected ZooCache getZooCache() {
    return zooCache;
  }

  protected void setZooCache(ZooCache zooCache) {
    this.zooCache = zooCache;
  }

  /**
   * Initialize the DistributedWorkQueue using the proper ZK location
   */
  protected void initializeWorkQueue(AccumuloConfiguration conf) {
    workQueue = new DistributedWorkQueue(ZooUtil.getRoot(conn.getInstance()) + ReplicationConstants.ZOO_WORK_QUEUE, conf);
  }

  @Override
  public void configure(AccumuloConfiguration conf, Connector conn) {
    this.conf = conf;
    this.conn = conn;
  }

  @Override
  public void assignWork() {
    if (null == workQueue) {
      initializeWorkQueue(conf);
    }

    initializeQueuedWork();

    if (null == zooCache) {
      zooCache = new ZooCache();
    }

    // Get the maximum number of entries we want to queue work for (or the default)
    this.maxQueueSize = conf.getCount(Property.REPLICATION_MAX_WORK_QUEUE);

    // Scan over the work records, adding the work to the queue
    createWork();

    // Keep the state of the work we queued correct
    cleanupFinishedWork();
  }

  /**
   * Scan over the {@link WorkSection} of the replication table adding work for entries that have data to replicate and have not already been queued.
   */
  protected void createWork() {
    // Create a scanner over the replication table's order entries
    Scanner s;
    try {
      s = ReplicationTable.getScanner(conn);
    } catch (ReplicationTableOfflineException e) {
      // no work to do; replication is off
      return;
    }

    OrderSection.limit(s);

    Text buffer = new Text();
    for (Entry<Key,Value> orderEntry : s) {
      // If we're not working off the entries, we need to not shoot ourselves in the foot by continuing
      // to add more work entries
      if (getQueueSize() > maxQueueSize) {
        log.warn("Queued replication work exceeds configured maximum ({}), sleeping to allow work to occur", maxQueueSize);
        return;
      }

      String file = OrderSection.getFile(orderEntry.getKey(), buffer);
      OrderSection.getTableId(orderEntry.getKey(), buffer);
      String sourceTableId = buffer.toString();

      log.info("Determining if {} from {} needs to be replicated", file, sourceTableId);

      Scanner workScanner;
      try {
        workScanner = ReplicationTable.getScanner(conn);
      } catch (ReplicationTableOfflineException e) {
        log.warn("Replication table is offline. Will retry...");
        sleepUninterruptibly(5, TimeUnit.SECONDS);
        return;
      }

      WorkSection.limit(workScanner);
      workScanner.setRange(Range.exact(file));

      int newReplicationTasksSubmitted = 0, workEntriesRead = 0;
      // For a file, we can concurrently replicate it to multiple targets
      for (Entry<Key,Value> workEntry : workScanner) {
        workEntriesRead++;
        Status status;
        try {
          status = StatusUtil.fromValue(workEntry.getValue());
        } catch (InvalidProtocolBufferException e) {
          log.warn("Could not deserialize protobuf from work entry for {} to {}, will retry", file,
              ReplicationTarget.from(workEntry.getKey().getColumnQualifier()), e);
          continue;
        }

        // Get the ReplicationTarget for this Work record
        ReplicationTarget target = WorkSection.getTarget(workEntry.getKey(), buffer);

        // Get the file (if any) currently being replicated to the given peer for the given source table
        Collection<String> keysBeingReplicated = getQueuedWork(target);

        Path p = new Path(file);
        String filename = p.getName();
        String key = DistributedWorkQueueWorkAssignerHelper.getQueueKey(filename, target);

        if (!shouldQueueWork(target)) {
          if (!isWorkRequired(status) && keysBeingReplicated.contains(key)) {
            log.debug("Removing {} from replication state to {} because replication is complete", key, target.getPeerName());
            this.removeQueuedWork(target, key);
          }

          continue;
        }

        // If there is work to do
        if (isWorkRequired(status)) {
          if (queueWork(p, target)) {
            newReplicationTasksSubmitted++;
          }
        } else {
          log.debug("Not queueing work for {} to {} because {} doesn't need replication", file, target, ProtobufUtil.toString(status));
          if (keysBeingReplicated.contains(key)) {
            log.debug("Removing {} from replication state to {} because replication is complete", key, target.getPeerName());
            this.removeQueuedWork(target, key);
          }
        }
      }

      log.debug("Read {} replication entries from the WorkSection of the replication table", workEntriesRead);
      log.info("Assigned {} replication work entries for {}", newReplicationTasksSubmitted, file);
    }
  }

  /**
   * @return Can replication work for the given {@link ReplicationTarget} be submitted to be worked on.
   */
  protected abstract boolean shouldQueueWork(ReplicationTarget target);

  /**
   * @return the size of the queued work
   */
  protected abstract int getQueueSize();

  /**
   * Set up any internal state before using the WorkAssigner
   */
  protected abstract void initializeQueuedWork();

  /**
   * Queue the given work for the target
   *
   * @param path
   *          File to replicate
   * @param target
   *          Target for the work
   * @return True if the work was queued, false otherwise
   */
  protected abstract boolean queueWork(Path path, ReplicationTarget target);

  /**
   * @param target
   *          Target for the work
   * @return Queued work for the given target
   */
  protected abstract Set<String> getQueuedWork(ReplicationTarget target);

  /**
   * Remove the given work from the internal state
   */
  protected abstract void removeQueuedWork(ReplicationTarget target, String queueKey);

  /**
   * Remove finished replication work from the internal state
   */
  protected abstract void cleanupFinishedWork();
}

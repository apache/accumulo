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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.replication.AbstractWorkAssigner;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Creates work in ZK which is <code>filename.serialized_ReplicationTarget => filename</code>
 */
public class SequentialWorkAssigner extends AbstractWorkAssigner {
  private static final Logger log = LoggerFactory.getLogger(SequentialWorkAssigner.class);
  private static final String NAME = "Sequential Work Assigner";

  private Connector conn;
  private AccumuloConfiguration conf;

  // @formatter.off
  /*
   * { 
   *    peer1 => {sourceTableId1 => work_queue_key1, sourceTableId2 => work_queue_key2, ...}
   *    peer2 => {sourceTableId1 => work_queue_key1, sourceTableId3 => work_queue_key4, ...}
   *    ...
   * }
   */
  // @formatter.on
  private Map<String,Map<String,String>> queuedWorkByPeerName;

  private DistributedWorkQueue workQueue;
  private int maxQueueSize;
  private ZooCache zooCache;

  public SequentialWorkAssigner() {}

  public SequentialWorkAssigner(AccumuloConfiguration conf, Connector conn) {
    this.conf = conf;
    this.conn = conn;
  }

  @Override
  public void configure(AccumuloConfiguration conf, Connector conn) {
    this.conf = conf;
    this.conn = conn;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void assignWork() {
    if (null == workQueue) {
      initializeWorkQueue(conf);
    }

    if (null == queuedWorkByPeerName) {
      initializeQueuedWork();
    }

    if (null == zooCache) {
      zooCache = new ZooCache();
    }

    // Get the maximum number of entries we want to queue work for (or the default)
    this.maxQueueSize = conf.getCount(Property.REPLICATION_MAX_WORK_QUEUE);

    for (Entry<String,Map<String,String>> peer : this.queuedWorkByPeerName.entrySet()) {
      log.info("In progress replications for {}", peer.getKey());
      for (Entry<String,String> tableRepl : peer.getValue().entrySet()) {
        log.info("Replicating {} for table ID {}", tableRepl.getValue(), tableRepl.getKey());
      }
    }

    // Scan over the work records, adding the work to the queue
    createWork();

    // Keep the state of the work we queued correct
    cleanupFinishedWork();
  }

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

  protected Map<String,Map<String,String>> getQueuedWork() {
    return queuedWorkByPeerName;
  }

  protected void setQueuedWork(Map<String,Map<String,String>> queuedWork) {
    this.queuedWorkByPeerName = queuedWork;
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
   * 
   * @param conf
   */
  protected void initializeWorkQueue(AccumuloConfiguration conf) {
    workQueue = new DistributedWorkQueue(ZooUtil.getRoot(conn.getInstance()) + Constants.ZREPLICATION_WORK_QUEUE, conf);
  }

  /**
   * Initialize the queuedWork set with the work already sent out
   */
  protected void initializeQueuedWork() {
    Preconditions.checkArgument(null == queuedWorkByPeerName, "Expected queuedWork to be null");
    queuedWorkByPeerName = new HashMap<>();
    List<String> existingWork;
    try {
      existingWork = workQueue.getWorkQueued();
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Error reading existing queued replication work", e);
    }

    log.info("Restoring replication work queue state from zookeeper");

    for (String work : existingWork) {
      Entry<String,ReplicationTarget> entry = fromQueueKey(work);
      String filename = entry.getKey();
      String peerName = entry.getValue().getPeerName();
      String sourceTableId = entry.getValue().getSourceTableId();

      log.debug("In progress replication of {} from table with ID {} to peer {}", filename, sourceTableId, peerName);

      Map<String,String> replicationForPeer = queuedWorkByPeerName.get(peerName);
      if (null == replicationForPeer) {
        replicationForPeer = new HashMap<>();
        queuedWorkByPeerName.put(sourceTableId, replicationForPeer);
      }

      replicationForPeer.put(sourceTableId, work);
    }
  }

  /**
   * Scan over the {@link WorkSection} of the replication table adding work for entries that have data to replicate and have not already been queued.
   */
  protected void createWork() {
    // Create a scanner over the replication table's order entries
    Scanner s;
    try {
      s = ReplicationTable.getScanner(conn);
    } catch (TableNotFoundException e) {
      return;
    }

    OrderSection.limit(s);

    Text buffer = new Text();
    for (Entry<Key,Value> orderEntry : s) {
      // If we're not working off the entries, we need to not shoot ourselves in the foot by continuing
      // to add more work entries
      if (queuedWorkByPeerName.size() > maxQueueSize) {
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
      } catch (TableNotFoundException e) {
        log.warn("Replication table was deleted. Will retry...");
        UtilWaitThread.sleep(5000);
        return;
      }

      WorkSection.limit(workScanner);
      workScanner.setRange(Range.exact(file));

      int newReplicationTasksSubmitted = 0;
      // For a file, we can concurrently replicate it to multiple targets
      for (Entry<Key,Value> workEntry : workScanner) {
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

        Map<String,String> queuedWorkForPeer = queuedWorkByPeerName.get(target.getPeerName());
        if (null == queuedWorkForPeer) {
          queuedWorkForPeer = new HashMap<>();
          queuedWorkByPeerName.put(target.getPeerName(), queuedWorkForPeer);
        }

        Path p = new Path(file);
        String filename = p.getName();
        String key = getQueueKey(filename, target);

        // Get the file (if any) currently being replicated to the given peer for the given source table
        String keyBeingReplicated = queuedWorkForPeer.get(sourceTableId);

        // If there is work to do
        if (isWorkRequired(status)) {
          if (null == keyBeingReplicated) {
            // If there is no file, submit this one for replication
            newReplicationTasksSubmitted += queueWork(key, file, sourceTableId, queuedWorkForPeer);
          } else {
            log.debug("Not queueing {} for work as {} must be replicated to {} first", file, keyBeingReplicated, target.getPeerName());
          }
        } else {
          log.debug("Not queueing work for {} because [{}] doesn't need replication", file, ProtobufUtil.toString(status));
          if (key.equals(keyBeingReplicated)) {
            log.debug("Removing {} from replication state to {} because replication is complete", keyBeingReplicated, target.getPeerName());
            queuedWorkForPeer.remove(sourceTableId);
            log.debug("State after removing element: {}", this.queuedWorkByPeerName);
          }
        }
      }

      log.info("Assigned {} replication work entries for {}", newReplicationTasksSubmitted, file);
    }
  }

  /**
   * Distribute the work for the given path with filename
   * 
   * @param key
   *          Unique key to identify this work in the queue
   * @param path
   *          Full path to a file
   */
  protected int queueWork(String key, String path, String sourceTableId, Map<String,String> queuedWorkForPeer) {
    try {
      log.debug("Queued work for {} and {} from table ID {}", key, path, sourceTableId);

      workQueue.addWork(key, path);      
      queuedWorkForPeer.put(sourceTableId, key);

      return 1;
    } catch (KeeperException | InterruptedException e) {
      log.warn("Could not queue work for {}", path, e);
      return 0;
    }
  }

  /**
   * Iterate over the queued work to remove entries that have been completed.
   */
  protected void cleanupFinishedWork() {
    final Iterator<Entry<String,Map<String,String>>> queuedWork = queuedWorkByPeerName.entrySet().iterator();
    final String instanceId = conn.getInstance().getInstanceID();

    int elementsRemoved = 0;
    // Check the status of all the work we've queued up
    while (queuedWork.hasNext()) {
      // {peer -> {tableId -> workKey, tableId -> workKey, ... }, peer -> ...}
      Entry<String,Map<String,String>> workForPeer = queuedWork.next();

      // TableID to workKey (filename and ReplicationTarget)
      Map<String,String> queuedReplication = workForPeer.getValue();

      Iterator<Entry<String,String>> iter = queuedReplication.entrySet().iterator();
      // Loop over every target we need to replicate this file to, removing the target when
      // the replication task has finished
      while (iter.hasNext()) {
        // tableID -> workKey
        Entry<String,String> entry = iter.next();
        // Null equates to the work for this target was finished
        if (null == zooCache.get(ZooUtil.getRoot(instanceId) + Constants.ZREPLICATION_WORK_QUEUE + "/" + entry.getValue())) {
          log.debug("Removing {} from work assignment state", entry.getValue());
          iter.remove();
          elementsRemoved++;
        }
      }
    }

    log.info("Removed {} elements from internal workqueue state because the work was complete", elementsRemoved);
  }
}

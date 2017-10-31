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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates work in ZK which is <code>filename.serialized_ReplicationTarget =&gt; filename</code>, but replicates files in the order in which they were created.
 * <p>
 * The intent is to ensure that WALs are replayed in the same order on the peer in which they were applied on the primary.
 */
public class SequentialWorkAssigner extends DistributedWorkQueueWorkAssigner {
  private static final Logger log = LoggerFactory.getLogger(SequentialWorkAssigner.class);
  private static final String NAME = "Sequential Work Assigner";

  // @formatter:off
  /*
   * {
   *    peer1 => {sourceTableId1 => work_queue_key1, sourceTableId2 => work_queue_key2, ...}
   *    peer2 => {sourceTableId1 => work_queue_key1, sourceTableId3 => work_queue_key4, ...}
   *    ...
   * }
   */
  // @formatter:on
  private Map<String,Map<Table.ID,String>> queuedWorkByPeerName;

  public SequentialWorkAssigner() {}

  public SequentialWorkAssigner(AccumuloConfiguration conf, Connector conn) {
    configure(conf, conn);
  }

  @Override
  public String getName() {
    return NAME;
  }

  protected Map<String,Map<Table.ID,String>> getQueuedWork() {
    return queuedWorkByPeerName;
  }

  protected void setQueuedWork(Map<String,Map<Table.ID,String>> queuedWork) {
    this.queuedWorkByPeerName = queuedWork;
  }

  /**
   * Initialize the queuedWork set with the work already sent out
   */
  @Override
  protected void initializeQueuedWork() {
    if (null != queuedWorkByPeerName) {
      return;
    }

    queuedWorkByPeerName = new HashMap<>();
    List<String> existingWork;
    try {
      existingWork = workQueue.getWorkQueued();
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Error reading existing queued replication work", e);
    }

    log.info("Restoring replication work queue state from zookeeper");

    for (String work : existingWork) {
      Entry<String,ReplicationTarget> entry = DistributedWorkQueueWorkAssignerHelper.fromQueueKey(work);
      String filename = entry.getKey();
      String peerName = entry.getValue().getPeerName();
      Table.ID sourceTableId = entry.getValue().getSourceTableId();

      log.debug("In progress replication of {} from table with ID {} to peer {}", filename, sourceTableId, peerName);

      Map<Table.ID,String> replicationForPeer = queuedWorkByPeerName.get(peerName);
      if (null == replicationForPeer) {
        replicationForPeer = new HashMap<>();
        queuedWorkByPeerName.put(peerName, replicationForPeer);
      }

      replicationForPeer.put(sourceTableId, work);
    }
  }

  /**
   * Iterate over the queued work to remove entries that have been completed.
   */
  @Override
  protected void cleanupFinishedWork() {
    final Iterator<Entry<String,Map<Table.ID,String>>> queuedWork = queuedWorkByPeerName.entrySet().iterator();
    final String instanceId = conn.getInstance().getInstanceID();

    int elementsRemoved = 0;
    // Check the status of all the work we've queued up
    while (queuedWork.hasNext()) {
      // {peer -> {tableId -> workKey, tableId -> workKey, ... }, peer -> ...}
      Entry<String,Map<Table.ID,String>> workForPeer = queuedWork.next();

      // TableID to workKey (filename and ReplicationTarget)
      Map<Table.ID,String> queuedReplication = workForPeer.getValue();

      Iterator<Entry<Table.ID,String>> iter = queuedReplication.entrySet().iterator();
      // Loop over every target we need to replicate this file to, removing the target when
      // the replication task has finished
      while (iter.hasNext()) {
        // tableID -> workKey
        Entry<Table.ID,String> entry = iter.next();
        // Null equates to the work for this target was finished
        if (null == zooCache.get(ZooUtil.getRoot(instanceId) + ReplicationConstants.ZOO_WORK_QUEUE + "/" + entry.getValue())) {
          log.debug("Removing {} from work assignment state", entry.getValue());
          iter.remove();
          elementsRemoved++;
        }
      }
    }

    log.info("Removed {} elements from internal workqueue state because the work was complete", elementsRemoved);
  }

  @Override
  protected int getQueueSize() {
    return queuedWorkByPeerName.size();
  }

  @Override
  protected boolean shouldQueueWork(ReplicationTarget target) {
    Map<Table.ID,String> queuedWorkForPeer = this.queuedWorkByPeerName.get(target.getPeerName());
    if (null == queuedWorkForPeer) {
      return true;
    }

    String queuedWork = queuedWorkForPeer.get(target.getSourceTableId());

    // If we have no work for the local table to the given peer, submit some!
    return null == queuedWork;
  }

  @Override
  protected boolean queueWork(Path path, ReplicationTarget target) {
    String queueKey = DistributedWorkQueueWorkAssignerHelper.getQueueKey(path.getName(), target);
    Map<Table.ID,String> workForPeer = this.queuedWorkByPeerName.get(target.getPeerName());
    if (null == workForPeer) {
      workForPeer = new HashMap<>();
      this.queuedWorkByPeerName.put(target.getPeerName(), workForPeer);
    }

    String queuedWork = workForPeer.get(target.getSourceTableId());
    if (null == queuedWork) {
      try {
        workQueue.addWork(queueKey, path.toString());
        workForPeer.put(target.getSourceTableId(), queueKey);
      } catch (KeeperException | InterruptedException e) {
        log.warn("Could not queue work for {} to {}", path, target, e);
        return false;
      }

      return true;
    } else if (queuedWork.startsWith(path.getName())) {
      log.debug("Not re-queueing work for {} as it has already been queued for replication to {}", path, target);
      return false;
    } else {
      log.debug("Not queueing {} for work as {} must be replicated to {} first", path, queuedWork, target.getPeerName());
      return false;
    }
  }

  @Override
  protected Set<String> getQueuedWork(ReplicationTarget target) {
    Map<Table.ID,String> queuedWorkForPeer = this.queuedWorkByPeerName.get(target.getPeerName());
    if (null == queuedWorkForPeer) {
      return Collections.emptySet();
    }

    String queuedWork = queuedWorkForPeer.get(target.getSourceTableId());
    if (null == queuedWork) {
      return Collections.emptySet();
    } else {
      return Collections.singleton(queuedWork);
    }
  }

  @Override
  protected void removeQueuedWork(ReplicationTarget target, String queueKey) {
    Map<Table.ID,String> queuedWorkForPeer = this.queuedWorkByPeerName.get(target.getPeerName());
    if (null == queuedWorkForPeer) {
      log.warn("removeQueuedWork called when no work was queued for {}", target.getPeerName());
      return;
    }

    String queuedWork = queuedWorkForPeer.get(target.getSourceTableId());
    if (queuedWork.equals(queueKey)) {
      queuedWorkForPeer.remove(target.getSourceTableId());
    } else {
      log.warn("removeQueuedWork called on {} with differing queueKeys, expected {} but was {}", target, queueKey, queuedWork);
      return;
    }
  }
}

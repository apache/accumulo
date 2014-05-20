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
package org.apache.accumulo.tserver.replication;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.replication.ReplicaSystem;
import org.apache.accumulo.core.client.replication.ReplicaSystemFactory;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.replication.AbstractWorkAssigner;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue.Processor;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;

/**
 * Transmit the given data to a peer
 */
public class ReplicationProcessor implements Processor {
  private static final Logger log = LoggerFactory.getLogger(ReplicationProcessor.class);

  private final Instance inst;
  private final AccumuloConfiguration conf;
  private final VolumeManager fs;
  private final Credentials creds;

  public ReplicationProcessor(Instance inst, AccumuloConfiguration conf, VolumeManager fs, Credentials creds) {
    this.inst = inst;
    this.conf = conf;
    this.fs = fs;
    this.creds = creds;
  }

  @Override
  public ReplicationProcessor newProcessor() {
    return new ReplicationProcessor(inst, new ServerConfiguration(inst).getConfiguration(), fs, creds);
  }

  @Override
  public void process(String workID, byte[] data) {
    ReplicationTarget target = AbstractWorkAssigner.fromQueueKey(workID).getValue();
    String file = new String(data);

    log.debug("Received replication work for {} to {}", file, target);

    // Find the configured replication peer so we know how to replicate to it
    // Classname,Configuration
    String peerType = getPeerType(target.getPeerName());

    // Get the peer that we're replicating to
    ReplicaSystem replica = ReplicaSystemFactory.get(peerType);
    Status status;
    try {
      status = getStatus(file, target);
    } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
      log.error("Could not look for replication record", e);
      throw new IllegalStateException("Could not look for replication record", e);
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize Status from Work section for {} and ", file, target);
      throw new RuntimeException("Could not parse Status for work record", e);
    }

    log.debug("Current status for {} replicating to {}: {}", file, target, ProtobufUtil.toString(status));

    // We don't need to do anything (shouldn't have gotten this work record in the first place)
    if (!StatusUtil.isWorkRequired(status)) {
      log.info("Received work request for {} and {}, but it does not need replication. Ignoring...", file, target);
      return;
    }

    // Sanity check that nothing bad happened and our replication source still exists
    Path filePath = new Path(file);
    try {
      if (!fs.exists(filePath)) {
        log.warn("Received work request for {} and {}, but the file doesn't exist", filePath, target);
        return;
      }
    } catch (IOException e) {
      log.error("Could not determine if file exists {}", filePath, e);
      throw new RuntimeException(e);
    }

    log.debug("Replicating {} to {} using {}", filePath, target, replica.getClass().getName());

    // Replicate that sucker
    Status replicatedStatus = replica.replicate(filePath, status, target);

    log.debug("Completed replication of {} to {}, with new Status [{}]", filePath, target, ProtobufUtil.toString(replicatedStatus));

    // If we got a different status
    if (!replicatedStatus.equals(status)) {
      // We actually did some work!
      recordNewStatus(filePath, replicatedStatus, target);
      return;
    }

    log.debug("Did not replicate any new data for {} to {}, (was [{}], now is [{}])", filePath, target, TextFormat.shortDebugString(status),
        TextFormat.shortDebugString(replicatedStatus));

    // otherwise, we didn't actually replicate because there was error sending the data
    // we can just not record any updates, and it will be picked up again by the work assigner
  }

  public String getPeerType(String peerName) {
    // Find the configured replication peer so we know how to replicate to it
    Map<String,String> configuredPeers = conf.getAllPropertiesWithPrefix(Property.REPLICATION_PEERS);
    String peerType = configuredPeers.get(Property.REPLICATION_PEERS.getKey() + peerName);
    if (null == peerType) {
      String msg = "Cannot process replication for unknown peer: " +  peerName;
      log.warn(msg);
      throw new IllegalArgumentException(msg);
    }

    return peerType;
  }

  public Status getStatus(String file, ReplicationTarget target) throws TableNotFoundException, AccumuloException, AccumuloSecurityException, InvalidProtocolBufferException {
    Scanner s = ReplicationTable.getScanner(inst.getConnector(creds.getPrincipal(), creds.getToken()));
    s.setRange(Range.exact(file));
    s.fetchColumn(WorkSection.NAME, target.toText());
    
    return Status.parseFrom(Iterables.getOnlyElement(s).getValue().get());
  }

  /**
   * Record the updated Status for this file and target
   * @param filePath Path to file being replicated
   * @param status Updated Status after replication
   * @param target Peer that was replicated to
   */
  public void recordNewStatus(Path filePath, Status status, ReplicationTarget target) {
    try {
      Connector conn = inst.getConnector(creds.getPrincipal(), creds.getToken());
      BatchWriter bw = ReplicationTable.getBatchWriter(conn);
      log.debug("Recording new status for {}, [{}]", filePath.toString(), TextFormat.shortDebugString(status));
      Mutation m = new Mutation(filePath.toString());
      WorkSection.add(m, target.toText(), ProtobufUtil.toValue(status));
      bw.addMutation(m);
      bw.close();
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      log.error("Error recording updated Status for {}", filePath, e);
      throw new RuntimeException(e);
    }
    
  }
}

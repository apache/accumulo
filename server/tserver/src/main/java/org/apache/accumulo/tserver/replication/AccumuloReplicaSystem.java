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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.ReplicationClient;
import org.apache.accumulo.core.client.impl.ServerConfigurationUtil;
import org.apache.accumulo.core.client.replication.ReplicaSystem;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.replication.thrift.KeyValues;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer.Client;
import org.apache.accumulo.core.replication.thrift.WalEdits;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.log.DfsLogger.DFSLoggerInputStreams;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * 
 */
public class AccumuloReplicaSystem implements ReplicaSystem {
  private static final Logger log = LoggerFactory.getLogger(AccumuloReplicaSystem.class);
  private static final String RFILE_SUFFIX = "." + RFile.EXTENSION;
  
  private String instanceName, zookeepers;
  private AccumuloConfiguration conf;
  private VolumeManager fs;

  /**
   * Generate the configuration string for this ReplicaSystem
   */
  public static String buildConfiguration(String instanceName, String zookeepers) {
    return instanceName + "," + zookeepers;
  }

  @Override
  public void configure(String configuration) {
    Preconditions.checkNotNull(configuration);

    // instance_name,zookeepers
    int index = configuration.indexOf(',');
    if (-1 == index) {
      throw new IllegalArgumentException("Expected comma in configuration string");
    }

    instanceName = configuration.substring(0, index);
    zookeepers = configuration.substring(index + 1);

    conf = ServerConfiguration.getSiteConfiguration();

    try {
      fs = VolumeManagerImpl.get(conf);
    } catch (IOException e) {
      log.error("Could not connect to filesystem", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Status replicate(final Path p, final Status status, final ReplicationTarget target) {
    Instance localInstance = HdfsZooInstance.getInstance();
    AccumuloConfiguration localConf = ServerConfigurationUtil.getConfiguration(localInstance);
    
    Instance peerInstance = getPeerInstance(target);
    // Remote identifier is an integer (table id) in this case.
    final int remoteTableId = Integer.parseInt(target.getRemoteIdentifier());

    // Attempt the replication of this status a number of times before giving up and
    // trying to replicate it again later some other time.
    for (int i = 0; i < localConf.getCount(Property.REPLICATION_WORK_ATTEMPTS); i++) {
      String peerTserver;
      try {
        // Ask the master on the remote what TServer we should talk with to replicate the data
        peerTserver = ReplicationClient.executeCoordinatorWithReturn(peerInstance, new ClientExecReturn<String,ReplicationCoordinator.Client>() {
  
          @Override
          public String execute(ReplicationCoordinator.Client client) throws Exception {
            return client.getServicerAddress(remoteTableId);
          }
          
        });
      } catch (AccumuloException | AccumuloSecurityException e) {
        // No progress is made
        log.error("Could not connect to master at {}, cannot proceed with replication. Will retry", target, e);
        continue;
      }
  
      if (null == peerTserver) {
        // Something went wrong, and we didn't get a valid tserver from the remote for some reason
        log.warn("Did not receive tserver from master at {}, cannot proceed with replication. Will retry.", target);
        continue;
      }
  
      // We have a tserver on the remote -- send the data its way.
      Long entriesReplicated;
      //TODO should chunk up the given file into some configurable sizes instead of just sending the entire file all at once
      //     configuration should probably just be size based.
      final long sizeLimit = conf.getMemoryInBytes(Property.REPLICATION_MAX_UNIT_SIZE);
      try {
        entriesReplicated = ReplicationClient.executeServicerWithReturn(peerInstance, peerTserver, new ClientExecReturn<Long,ReplicationServicer.Client>() {
          @Override
          public Long execute(Client client) throws Exception {
            // RFiles have an extension, call everything else a WAL
            if (p.getName().endsWith(RFILE_SUFFIX)) {
              KeyValues kvs = getKeyValues(target, p, status, sizeLimit);
              if (0 < kvs.getKeyValuesSize()) {
                return client.replicateKeyValues(remoteTableId, kvs);
              }
            } else {
              WalEdits edits = getWalEdits(target, p, status, sizeLimit);
              if (0 < edits.getEditsSize()) {
                return client.replicateLog(remoteTableId, edits);
              }
            }

            return 0l;
          }
        });

        log.debug("Replicated {} entries from {} to {} which is a member of the peer '{}'", entriesReplicated, p, peerTserver, peerInstance.getInstanceName());

        // Update the begin to account for what we replicated
        Status updatedStatus = Status.newBuilder(status).setBegin(status.getBegin() + entriesReplicated).build();

        return updatedStatus;
      } catch (TTransportException | AccumuloException | AccumuloSecurityException e) {
        log.warn("Could not connect to remote server {}, will retry", peerTserver, e);
        UtilWaitThread.sleep(250);
      }
    }

    // We made no status, punt on it for now, and let it re-queue itself for work
    return status;
  }

  protected Instance getPeerInstance(ReplicationTarget target) {
    return new ZooKeeperInstance(instanceName, zookeepers);
  }

  protected KeyValues getKeyValues(ReplicationTarget target, Path p, Status status, long sizeLimit) {
    // TODO Implement me
    throw new UnsupportedOperationException();
  }

  protected WalEdits getWalEdits(ReplicationTarget target, Path p, Status status, long sizeLimit) throws IOException {
    DFSLoggerInputStreams streams = DfsLogger.readHeaderAndReturnStream(fs, p, conf);
    DataInputStream wal = streams.getDecryptingInputStream();
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    // Read through the stuff we don't need to replicate
    for (long i = 0; i < status.getBegin(); i++) {
      try {
        key.readFields(wal);
        value.readFields(wal);
      } catch (EOFException e) {
        log.warn("Unexpectedly reached the end of file. Nothing more to replicate.");
        return null;
      }
    }

    Entry<Long,WalEdits> pair = getEdits(wal, sizeLimit, target);

    log.debug("Binned {} bytes of WAL entries for replication to peer '{}'", pair.getKey(), p);

    return pair.getValue();
  }

  protected Entry<Long,WalEdits> getEdits(DataInputStream wal, long sizeLimit, ReplicationTarget target) throws IOException {
    WalEdits edits = new WalEdits();
    edits.edits = new ArrayList<ByteBuffer>();
    long size = 0l;
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    // TODO do we need to track *all* possible tids when the wal is not sorted?
    Set<Integer> desiredTids = new HashSet<>();

    while (size < sizeLimit) {
      try {
        key.readFields(wal);
        value.readFields(wal);
      } catch (EOFException e) {
        log.debug("Caught EOFException, no more data to replicate");
        break;
      }

      switch (key.event) {
        case DEFINE_TABLET:
          if (target.getSourceTableId().equals(key.tablet.getTableId().toString())) {
            desiredTids.add(key.tid);
          }
          break;
        case MUTATION:
        case MANY_MUTATIONS:
          // Only write out mutations for tids that are for the desired tablet
          if (desiredTids.contains(key.tid)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
  
            key.write(out);
            value.write(out);
  
            out.flush();
            byte[] data = baos.toByteArray();
            size += data.length;
            edits.addToEdits(ByteBuffer.wrap(data));
          }
          break;
        default:
          log.trace("Ignorning WAL entry which doesn't contain mutations");
          break;
      }
    }

    return Maps.immutableEntry(size, edits);
  }
}

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
import org.apache.accumulo.core.data.Mutation;
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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class AccumuloReplicaSystem implements ReplicaSystem {
  private static final Logger log = LoggerFactory.getLogger(AccumuloReplicaSystem.class);
  private static final String RFILE_SUFFIX = "." + RFile.EXTENSION;

  private String instanceName, zookeepers;
  private AccumuloConfiguration conf;
  private VolumeManager fs;

  protected String getInstanceName() {
    return instanceName;
  }

  protected void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }

  protected String getZookeepers() {
    return zookeepers;
  }

  protected void setZookeepers(String zookeepers) {
    this.zookeepers = zookeepers;
  }

  protected AccumuloConfiguration getConf() {
    return conf;
  }

  protected void setConf(AccumuloConfiguration conf) {
    this.conf = conf;
  }

  protected VolumeManager getFs() {
    return fs;
  }

  protected void setFs(VolumeManager fs) {
    this.fs = fs;
  }

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

    conf = ServerConfiguration.getSystemConfiguration(HdfsZooInstance.getInstance());

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
      ReplicationStats replResult;
      // TODO should chunk up the given file into some configurable sizes instead of just sending the entire file all at once
      // configuration should probably just be size based.
      final long sizeLimit = conf.getMemoryInBytes(Property.REPLICATION_MAX_UNIT_SIZE);
      try {
        replResult = ReplicationClient.executeServicerWithReturn(peerInstance, peerTserver,
            new ClientExecReturn<ReplicationStats,ReplicationServicer.Client>() {
              @Override
              public ReplicationStats execute(Client client) throws Exception {
                // RFiles have an extension, call everything else a WAL
                if (p.getName().endsWith(RFILE_SUFFIX)) {
                  RFileReplication kvs = getKeyValues(target, p, status, sizeLimit);
                  if (0 < kvs.keyValues.getKeyValuesSize()) {
                    long entriesReplicated = client.replicateKeyValues(remoteTableId, kvs.keyValues);
                    if (entriesReplicated != kvs.keyValues.getKeyValuesSize()) {
                      log.warn("Sent {} KeyValue entries for replication but only {} were reported as replicated", kvs.keyValues.getKeyValuesSize(),
                          entriesReplicated);
                    }

                    // Not as important to track as WALs because we don't skip any KVs in an RFile
                    return kvs;
                  }
                } else {
                  WalReplication edits = getWalEdits(target, p, status, sizeLimit);

                  // If we have some edits to send
                  if (0 < edits.walEdits.getEditsSize()) {
                    long entriesReplicated = client.replicateLog(remoteTableId, edits.walEdits);
                    if (entriesReplicated != edits.numUpdates) {
                      log.warn("Sent {} WAL entries for replication but {} were reported as replicated", edits.numUpdates, entriesReplicated);
                    }

                    // We don't have to replicate every LogEvent in the file (only Mutation LogEvents), but we
                    // want to track progress in the file relative to all LogEvents (to avoid duplicative processing/replication)
                    return edits;
                  } else if (edits.entriesConsumed == Long.MAX_VALUE) {
                    // Even if we send no data, we must record the new begin value to account for the inf+ length
                    return edits;
                  }
                }

                // No data sent (bytes nor records) and no progress made
                return new ReplicationStats(0l, 0l, 0l);
              }
            });

        log.debug("Replicated {} entries from {} to {} which is a member of the peer '{}'", replResult.sizeInRecords, p, peerTserver,
            peerInstance.getInstanceName());

        // Catch the overflow
        long newBegin = status.getBegin() + replResult.entriesConsumed;
        if (newBegin < 0) {
          newBegin = Long.MAX_VALUE;
        }

        // Update the begin to account for what we replicated
        Status updatedStatus = Status.newBuilder(status).setBegin(newBegin).build();

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

  protected RFileReplication getKeyValues(ReplicationTarget target, Path p, Status status, long sizeLimit) {
    // TODO Implement me
    throw new UnsupportedOperationException();
  }

  protected WalReplication getWalEdits(ReplicationTarget target, Path p, Status status, long sizeLimit) throws IOException {
    DFSLoggerInputStreams streams = DfsLogger.readHeaderAndReturnStream(fs, p, conf);
    DataInputStream wal = streams.getDecryptingInputStream();
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    // Read through the stuff we've already processed in a previous replication attempt
    for (long i = 0; i < status.getBegin(); i++) {
      try {
        key.readFields(wal);
        value.readFields(wal);
      } catch (EOFException e) {
        log.warn("Unexpectedly reached the end of file.");
        return new WalReplication(new WalEdits(), 0, 0, 0);
      }
    }

    WalReplication repl = getEdits(wal, sizeLimit, target, status, p);

    log.debug("Read {} WAL entries and retained {} bytes of WAL entries for replication to peer '{}'", (Long.MAX_VALUE == repl.entriesConsumed) ? "all"
        : repl.entriesConsumed, repl.sizeInBytes, p);

    return repl;
  }

  protected WalReplication getEdits(DataInputStream wal, long sizeLimit, ReplicationTarget target, Status status, Path p) throws IOException {
    WalEdits edits = new WalEdits();
    edits.edits = new ArrayList<ByteBuffer>();
    long size = 0l;
    long entriesConsumed = 0l;
    long numUpdates = 0l;
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    // Any tid for our table needs to be tracked
    Set<Integer> desiredTids = new HashSet<>();

    while (size < sizeLimit) {
      try {
        key.readFields(wal);
        value.readFields(wal);
      } catch (EOFException e) {
        log.debug("Caught EOFException reading {}", p);
        if (status.getInfiniteEnd() && status.getClosed()) {
          log.debug("{} is closed and has unknown length, assuming entire file has been consumed", p);
          entriesConsumed = Long.MAX_VALUE;
        }
        break;
      }

      entriesConsumed++;

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

            // Only write out the mutations that don't have the given ReplicationTarget
            // as a replicate source (this prevents infinite replication loops: a->b, b->a, repeat)
            numUpdates += writeValueAvoidingReplicationCycles(out, value, target);

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

    return new WalReplication(edits, size, entriesConsumed, numUpdates);
  }

  /**
   * Wrapper around {@link LogFileValue#write(java.io.DataOutput)} which does not serialize {@link Mutation}s that do not need to be replicate to the given
   * {@link ReplicationTarget}
   * 
   * @throws IOException
   */
  protected long writeValueAvoidingReplicationCycles(DataOutputStream out, LogFileValue value, ReplicationTarget target) throws IOException {
    int mutationsToSend = 0;
    for (Mutation m : value.mutations) {
      if (!m.getReplicationSources().contains(target.getPeerName())) {
        mutationsToSend++;
      }
    }

    log.debug("Removing {} mutations from WAL entry as they have already been replicated to {}", value.mutations.size() - mutationsToSend, target.getPeerName());

    out.writeInt(mutationsToSend);
    for (Mutation m : value.mutations) {
      // If we haven't yet replicated to this peer
      if (!m.getReplicationSources().contains(target.getPeerName())) {
        // Add our name, and send it
        String name = conf.get(Property.REPLICATION_NAME);
        if (StringUtils.isBlank(name)) {
          throw new IllegalArgumentException("Local system has no replication name configured");
        }

        m.addReplicationSource(name);

        m.write(out);
      }
    }

    return mutationsToSend;
  }

  public static class ReplicationStats {
    /**
     * The size, in bytes, of the data sent
     */
    public long sizeInBytes;

    /**
     * The number of records sent
     */
    public long sizeInRecords;

    /**
     * The number of entries consumed from the log (to increment {@link Status}'s begin)
     */
    public long entriesConsumed;

    public ReplicationStats(long sizeInBytes, long sizeInRecords, long entriesConsumed) {
      this.sizeInBytes = sizeInBytes;
      this.sizeInRecords = sizeInRecords;
      this.entriesConsumed = entriesConsumed;
    }
  }

  public static class RFileReplication extends ReplicationStats {
    /**
     * The data to send
     */
    public KeyValues keyValues;

    public RFileReplication(KeyValues kvs, long size) {
      super(size, kvs.keyValues.size(), kvs.keyValues.size());
      this.keyValues = kvs;
    }
  }

  /**
   * A "struct" to avoid a nested Entry. Contains the resultant information from collecting data for replication
   */
  public static class WalReplication extends ReplicationStats {
    /**
     * The data to send over the wire
     */
    public WalEdits walEdits;

    /**
     * The number of updates contained in this batch
     */
    public long numUpdates;

    public WalReplication(WalEdits edits, long size, long entriesConsumed, long numMutations) {
      super(size, edits.getEditsSize(), entriesConsumed);
      this.walEdits = edits;
      this.numUpdates = numMutations;
    }
  }
}

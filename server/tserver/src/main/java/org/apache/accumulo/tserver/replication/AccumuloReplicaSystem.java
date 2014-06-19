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
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.ReplicationClient;
import org.apache.accumulo.core.client.impl.ServerConfigurationUtil;
import org.apache.accumulo.core.client.replication.ReplicaSystem;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicaSystemHelper;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.replication.thrift.KeyValues;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer.Client;
import org.apache.accumulo.core.replication.thrift.WalEdits;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
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
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
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
  public Status replicate(final Path p, final Status status, final ReplicationTarget target, final ReplicaSystemHelper helper) {
    final Instance localInstance = HdfsZooInstance.getInstance();
    final AccumuloConfiguration localConf = ServerConfigurationUtil.getConfiguration(localInstance);
    Credentials credentialsForPeer = getCredentialsForPeer(localConf, target);
    final TCredentials tCredsForPeer = credentialsForPeer.toThrift(localInstance);

    try {
      Trace.on("AccumuloReplicaSystem");

      Instance peerInstance = getPeerInstance(target);
      // Remote identifier is an integer (table id) in this case.
      final String remoteTableId = target.getRemoteIdentifier();

      // Attempt the replication of this status a number of times before giving up and
      // trying to replicate it again later some other time.
      int numAttempts = localConf.getCount(Property.REPLICATION_WORK_ATTEMPTS);
      for (int i = 0; i < numAttempts; i++) {
        String peerTserver;
        Span span = Trace.start("Fetch peer tserver");
        try {
          // Ask the master on the remote what TServer we should talk with to replicate the data
          peerTserver = ReplicationClient.executeCoordinatorWithReturn(peerInstance, new ClientExecReturn<String,ReplicationCoordinator.Client>() {

            @Override
            public String execute(ReplicationCoordinator.Client client) throws Exception {
              return client.getServicerAddress(remoteTableId, tCredsForPeer);
            }

          });
        } catch (AccumuloException | AccumuloSecurityException e) {
          // No progress is made
          log.error("Could not connect to master at {}, cannot proceed with replication. Will retry", target, e);
          continue;
        } finally {
          span.stop();
        }

        if (null == peerTserver) {
          // Something went wrong, and we didn't get a valid tserver from the remote for some reason
          log.warn("Did not receive tserver from master at {}, cannot proceed with replication. Will retry.", target);
          continue;
        }

        // We have a tserver on the remote -- send the data its way.
        Status finalStatus;
        final long sizeLimit = conf.getMemoryInBytes(Property.REPLICATION_MAX_UNIT_SIZE);
        try {
          if (p.getName().endsWith(RFILE_SUFFIX)) {
            span = Trace.start("RFile replication");
            try {
              finalStatus = replicateRFiles(peerInstance, peerTserver, target, p, status, sizeLimit, remoteTableId, tCredsForPeer, helper);
            } finally {
              span.stop();
            }
          } else {
            span = Trace.start("WAL replication");
            try {
              finalStatus = replicateLogs(peerInstance, peerTserver, target, p, status, sizeLimit, remoteTableId, tCredsForPeer, helper);
            } finally {
              span.stop();
            }
          }

          log.debug("New status for {} after replicating to {} is {}", p, peerInstance, ProtobufUtil.toString(finalStatus));

          return finalStatus;
        } catch (TTransportException | AccumuloException | AccumuloSecurityException e) {
          log.warn("Could not connect to remote server {}, will retry", peerTserver, e);
          UtilWaitThread.sleep(1000);
        }
      }

      log.info("No progress was made after {} attempts to replicate {}, returning so file can be re-queued", numAttempts, p);

      // We made no status, punt on it for now, and let it re-queue itself for work
      return status;
    } finally {
      Trace.offNoFlush();
    }
  }

  protected Status replicateRFiles(final Instance peerInstance, final String peerTserver, final ReplicationTarget target, final Path p, final Status status,
      final long sizeLimit, final String remoteTableId, final TCredentials tcreds, final ReplicaSystemHelper helper) throws TTransportException,
      AccumuloException, AccumuloSecurityException {
    DataInputStream input;
    try {
      input = getRFileInputStream(p);
    } catch (IOException e) {
      log.error("Could not create input stream from RFile, will retry", e);
      return status;
    }

    Status lastStatus = status, currentStatus = status;
    while (true) {
      // Read and send a batch of mutations
      ReplicationStats replResult = ReplicationClient.executeServicerWithReturn(peerInstance, peerTserver, new RFileClientExecReturn(target, input, p,
          currentStatus, sizeLimit, remoteTableId, tcreds));

      // Catch the overflow
      long newBegin = currentStatus.getBegin() + replResult.entriesConsumed;
      if (newBegin < 0) {
        newBegin = Long.MAX_VALUE;
      }

      currentStatus = Status.newBuilder(currentStatus).setBegin(newBegin).build();

      log.debug("Sent batch for replication of {} to {}, with new Status {}", p, target, ProtobufUtil.toString(currentStatus));

      // If we got a different status
      if (!currentStatus.equals(lastStatus)) {
        // If we don't have any more work, just quit
        if (!StatusUtil.isWorkRequired(currentStatus)) {
          return currentStatus;
        } else {
          // Otherwise, let it loop and replicate some more data
          lastStatus = currentStatus;
        }
      } else {
        log.debug("Did not replicate any new data for {} to {}, (state was {})", p, target, ProtobufUtil.toString(lastStatus));

        // otherwise, we didn't actually replicate (likely because there was error sending the data)
        // we can just not record any updates, and it will be picked up again by the work assigner
        return status;
      }
    }
  }

  protected Status replicateLogs(final Instance peerInstance, final String peerTserver, final ReplicationTarget target, final Path p, final Status status,
      final long sizeLimit, final String remoteTableId, final TCredentials tcreds, final ReplicaSystemHelper helper) throws TTransportException,
      AccumuloException, AccumuloSecurityException {

    final Set<Integer> tids;
    final DataInputStream input;
    Span span = Trace.start("Read WAL header");
    span.data("file", p.toString());
    try {
      input = getWalStream(p);
    } catch (IOException e) {
      log.error("Could not create stream for WAL", e);
      // No data sent (bytes nor records) and no progress made
      return status;
    } finally {
      span.stop();
    }

    span = Trace.start("Consume WAL prefix");
    span.data("file", p.toString());
    try {
      // We want to read all records in the WAL up to the "begin" offset contained in the Status message,
      // building a Set of tids from DEFINE_TABLET events which correspond to table ids for future mutations
      tids = consumeWalPrefix(target, input, p, status, sizeLimit);
    } catch (IOException e) {
      log.warn("Unexpected error consuming file.");
      return status;
    } finally {
      span.stop();
    }

    Status lastStatus = status, currentStatus = status;
    while (true) {
      // Set some trace info
      span = Trace.start("Replicate WAL batch");
      span.data("Batch size (bytes)", Long.toString(sizeLimit));
      span.data("File", p.toString());
      span.data("Peer instance name", peerInstance.getInstanceName());
      span.data("Peer tserver", peerTserver);
      span.data("Remote table ID", remoteTableId);

      ReplicationStats replResult;
      try {
        // Read and send a batch of mutations
        replResult = ReplicationClient.executeServicerWithReturn(peerInstance, peerTserver, new WalClientExecReturn(target, input, p, currentStatus, sizeLimit,
            remoteTableId, tcreds, tids));
      } finally {
        span.stop();
      }

      // Catch the overflow
      long newBegin = currentStatus.getBegin() + replResult.entriesConsumed;
      if (newBegin < 0) {
        newBegin = Long.MAX_VALUE;
      }

      currentStatus = Status.newBuilder(currentStatus).setBegin(newBegin).build();

      log.debug("Sent batch for replication of {} to {}, with new Status {}", p, target, ProtobufUtil.toString(currentStatus));

      // If we got a different status
      if (!currentStatus.equals(lastStatus)) {
        span = Trace.start("Update replication table");
        try {
          helper.recordNewStatus(p, currentStatus, target);
        } catch (TableNotFoundException e) {
          log.error("Tried to update status in replication table for {} as {}, but the table did not exist", p, ProtobufUtil.toString(currentStatus), e);
          throw new RuntimeException("Replication table did not exist, will retry", e);
        } finally {
          span.stop();
        }

        // If we don't have any more work, just quit
        if (!StatusUtil.isWorkRequired(currentStatus)) {
          return currentStatus;
        } else {
          // Otherwise, let it loop and replicate some more data
          lastStatus = currentStatus;
        }
      } else {
        log.debug("Did not replicate any new data for {} to {}, (state was {})", p, target, ProtobufUtil.toString(lastStatus));

        // otherwise, we didn't actually replicate (likely because there was error sending the data)
        // we can just not record any updates, and it will be picked up again by the work assigner
        return status;
      }
    }
  }

  protected class WalClientExecReturn implements ClientExecReturn<ReplicationStats,ReplicationServicer.Client> {

    private ReplicationTarget target;
    private DataInputStream input;
    private Path p;
    private Status status;
    private long sizeLimit;
    private String remoteTableId;
    private TCredentials tcreds;
    private Set<Integer> tids;

    public WalClientExecReturn(ReplicationTarget target, DataInputStream input, Path p, Status status, long sizeLimit, String remoteTableId, TCredentials tcreds,
        Set<Integer> tids) {
      this.target = target;
      this.input = input;
      this.p = p;
      this.status = status;
      this.sizeLimit = sizeLimit;
      this.remoteTableId = remoteTableId;
      this.tcreds = tcreds;
      this.tids = tids;
    }

    @Override
    public ReplicationStats execute(Client client) throws Exception {
      WalReplication edits = getWalEdits(target, input, p, status, sizeLimit, tids);

      log.debug("Read {} WAL entries and retained {} bytes of WAL entries for replication to peer '{}'", (Long.MAX_VALUE == edits.entriesConsumed) ? "all"
          : edits.entriesConsumed, edits.sizeInBytes, p);

      // If we have some edits to send
      if (0 < edits.walEdits.getEditsSize()) {
        long entriesReplicated = client.replicateLog(remoteTableId, edits.walEdits, tcreds);
        if (entriesReplicated != edits.numUpdates) {
          log.warn("Sent {} WAL entries for replication but {} were reported as replicated", edits.numUpdates, entriesReplicated);
        }

        // We don't have to replicate every LogEvent in the file (only Mutation LogEvents), but we
        // want to track progress in the file relative to all LogEvents (to avoid duplicative processing/replication)
        return edits;
      } else if (edits.entriesConsumed > 0) {
        // Even if we send no data, we want to record a non-zero new begin value to avoid checking the same
        // log entries multiple times to determine if they should be sent
        return edits;
      }

      // No data sent (bytes nor records) and no progress made
      return new ReplicationStats(0l, 0l, 0l);
    }
  }

  protected class RFileClientExecReturn implements ClientExecReturn<ReplicationStats,ReplicationServicer.Client> {

    private ReplicationTarget target;
    private DataInputStream input;
    private Path p;
    private Status status;
    private long sizeLimit;
    private String remoteTableId;
    private TCredentials tcreds;

    public RFileClientExecReturn(ReplicationTarget target, DataInputStream input, Path p, Status status, long sizeLimit, String remoteTableId, TCredentials tcreds) {
      this.target = target;
      this.input = input;
      this.p = p;
      this.status = status;
      this.sizeLimit = sizeLimit;
      this.remoteTableId = remoteTableId;
      this.tcreds = tcreds;
    }

    @Override
    public ReplicationStats execute(Client client) throws Exception {
      RFileReplication kvs = getKeyValues(target, input, p, status, sizeLimit);
      if (0 < kvs.keyValues.getKeyValuesSize()) {
        long entriesReplicated = client.replicateKeyValues(remoteTableId, kvs.keyValues, tcreds);
        if (entriesReplicated != kvs.keyValues.getKeyValuesSize()) {
          log.warn("Sent {} KeyValue entries for replication but only {} were reported as replicated", kvs.keyValues.getKeyValuesSize(), entriesReplicated);
        }

        // Not as important to track as WALs because we don't skip any KVs in an RFile
        return kvs;
      }

      // No data sent (bytes nor records) and no progress made
      return new ReplicationStats(0l, 0l, 0l);
    }
  }

  protected Credentials getCredentialsForPeer(AccumuloConfiguration conf, ReplicationTarget target) {
    Preconditions.checkNotNull(conf);
    Preconditions.checkNotNull(target);

    String peerName = target.getPeerName();
    String userKey = Property.REPLICATION_PEER_USER.getKey() + peerName, passwordKey = Property.REPLICATION_PEER_PASSWORD.getKey() + peerName;
    Map<String,String> peerUsers = conf.getAllPropertiesWithPrefix(Property.REPLICATION_PEER_USER);
    Map<String,String> peerPasswords = conf.getAllPropertiesWithPrefix(Property.REPLICATION_PEER_PASSWORD);

    String user = peerUsers.get(userKey);
    String password = peerPasswords.get(passwordKey);
    if (null == user || null == password) {
      throw new IllegalArgumentException(userKey + " and " + passwordKey + " not configured, cannot replicate");
    }

    return new Credentials(user, new PasswordToken(password));
  }

  protected Instance getPeerInstance(ReplicationTarget target) {
    return new ZooKeeperInstance(instanceName, zookeepers);
  }

  protected RFileReplication getKeyValues(ReplicationTarget target, DataInputStream input, Path p, Status status, long sizeLimit) {
    // TODO ACCUMULO-2580 Implement me
    throw new UnsupportedOperationException();
  }

  protected Set<Integer> consumeWalPrefix(ReplicationTarget target, DataInputStream wal, Path p, Status status, long sizeLimit) throws IOException {
    Set<Integer> tids = new HashSet<>();
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    Set<Integer> desiredTids = new HashSet<>();

    // Read through the stuff we've already processed in a previous replication attempt
    // We also need to track the tids that occurred earlier in the file as mutations
    // later on might use that tid
    for (long i = 0; i < status.getBegin(); i++) {
      key.readFields(wal);
      value.readFields(wal);

      switch (key.event) {
        case DEFINE_TABLET:
          if (target.getSourceTableId().equals(key.tablet.getTableId().toString())) {
            desiredTids.add(key.tid);
          }
          break;
        default:
          break;
      }
    }

    return tids;
  }

  public DataInputStream getWalStream(Path p) throws IOException {
    DFSLoggerInputStreams streams = DfsLogger.readHeaderAndReturnStream(fs, p, conf);
    return streams.getDecryptingInputStream();
  }

  protected WalReplication getWalEdits(ReplicationTarget target, DataInputStream wal, Path p, Status status, long sizeLimit, Set<Integer> desiredTids)
      throws IOException {
    WalEdits edits = new WalEdits();
    edits.edits = new ArrayList<ByteBuffer>();
    long size = 0l;
    long entriesConsumed = 0l;
    long numUpdates = 0l;
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

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
          // For new DEFINE_TABLETs, we also need to record the new tids we see
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
   */
  protected long writeValueAvoidingReplicationCycles(DataOutputStream out, LogFileValue value, ReplicationTarget target) throws IOException {
    int mutationsToSend = 0;
    for (Mutation m : value.mutations) {
      if (!m.getReplicationSources().contains(target.getPeerName())) {
        mutationsToSend++;
      }
    }

    int mutationsRemoved = value.mutations.size() - mutationsToSend;
    if (mutationsRemoved > 0) {
      log.debug("Removing {} mutations from WAL entry as they have already been replicated to {}", mutationsRemoved, target.getPeerName());
    }

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

  protected DataInputStream getRFileInputStream(Path p) throws IOException {
    throw new UnsupportedOperationException("Not yet implemented");
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

    @Override
    public boolean equals(Object o) {
      if (ReplicationStats.class.isAssignableFrom(o.getClass())) {
        ReplicationStats other = (ReplicationStats) o;
        return sizeInBytes == other.sizeInBytes && sizeInRecords == other.sizeInRecords && entriesConsumed == other.entriesConsumed;
      }
      return false;
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

    @Override
    public boolean equals(Object o) {
      if (o instanceof WalReplication) {
        WalReplication other = (WalReplication) o;

        return super.equals(other) && walEdits.equals(other.walEdits) && numUpdates == other.numUpdates;
      }

      return false;
    }
  }
}

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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.Objects.requireNonNull;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.ReplicationClient;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.thrift.KeyValues;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer.Client;
import org.apache.accumulo.core.replication.thrift.WalEdits;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.trace.ProbabilitySampler;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.replication.ReplicaSystem;
import org.apache.accumulo.server.replication.ReplicaSystemHelper;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.log.DfsLogger.DFSLoggerInputStreams;
import org.apache.accumulo.tserver.log.DfsLogger.LogHeaderIncompleteException;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

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
    requireNonNull(configuration);

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

    conf = new ServerConfigurationFactory(HdfsZooInstance.getInstance()).getSystemConfiguration();

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
    final AccumuloConfiguration localConf = new ServerConfigurationFactory(localInstance).getSystemConfiguration();

    log.debug("Replication RPC timeout is {}", localConf.get(Property.REPLICATION_RPC_TIMEOUT.getKey()));

    final String principal = getPrincipal(localConf, target);
    final File keytab;
    final String password;
    if (localConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      String keytabPath = getKeytab(localConf, target);
      keytab = new File(keytabPath);
      if (!keytab.exists() || !keytab.isFile()) {
        log.error("{} is not a regular file. Cannot login to replicate", keytabPath);
        return status;
      }
      password = null;
    } else {
      keytab = null;
      password = getPassword(localConf, target);
    }

    if (null != keytab) {
      try {
        final UserGroupInformation accumuloUgi = UserGroupInformation.getCurrentUser();
        // Get a UGI with the principal + keytab
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab.getAbsolutePath());

        // Run inside a doAs to avoid nuking the Tserver's user
        return ugi.doAs(new PrivilegedAction<Status>() {
          @Override
          public Status run() {
            KerberosToken token;
            try {
              // Do *not* replace the current user
              token = new KerberosToken(principal, keytab);
            } catch (IOException e) {
              log.error("Failed to create KerberosToken", e);
              return status;
            }
            ClientContext peerContext = getContextForPeer(localConf, target, principal, token);
            return _replicate(p, status, target, helper, localConf, peerContext, accumuloUgi);
          }
        });
      } catch (IOException e) {
        // Can't log in, can't replicate
        log.error("Failed to perform local login", e);
        return status;
      }
    } else {
      // Simple case: make a password token, context and then replicate
      PasswordToken token = new PasswordToken(password);
      ClientContext peerContext = getContextForPeer(localConf, target, principal, token);
      return _replicate(p, status, target, helper, localConf, peerContext, null);
    }
  }

  /**
   * Perform replication, making a few attempts when an exception is returned.
   *
   * @param p
   *          Path of WAL to replicate
   * @param status
   *          Current status for the WAL
   * @param target
   *          Where we're replicating to
   * @param helper
   *          A helper for replication
   * @param localConf
   *          The local instance's configuration
   * @param peerContext
   *          The ClientContext to connect to the peer
   * @return The new (or unchanged) Status for the WAL
   */
  private Status _replicate(final Path p, final Status status, final ReplicationTarget target, final ReplicaSystemHelper helper,
      final AccumuloConfiguration localConf, final ClientContext peerContext, final UserGroupInformation accumuloUgi) {
    try {
      double tracePercent = localConf.getFraction(Property.REPLICATION_TRACE_PERCENT);
      ProbabilitySampler sampler = new ProbabilitySampler(tracePercent);
      Trace.on("AccumuloReplicaSystem", sampler);

      // Remote identifier is an integer (table id) in this case.
      final String remoteTableId = target.getRemoteIdentifier();

      // Attempt the replication of this status a number of times before giving up and
      // trying to replicate it again later some other time.
      int numAttempts = localConf.getCount(Property.REPLICATION_WORK_ATTEMPTS);
      for (int i = 0; i < numAttempts; i++) {
        log.debug("Attempt {}", i);
        String peerTserverStr;
        log.debug("Fetching peer tserver address");
        Span span = Trace.start("Fetch peer tserver");
        try {
          // Ask the master on the remote what TServer we should talk with to replicate the data
          peerTserverStr = ReplicationClient.executeCoordinatorWithReturn(peerContext, new ClientExecReturn<String,ReplicationCoordinator.Client>() {

            @Override
            public String execute(ReplicationCoordinator.Client client) throws Exception {
              return client.getServicerAddress(remoteTableId, peerContext.rpcCreds());
            }

          });
        } catch (AccumuloException | AccumuloSecurityException e) {
          // No progress is made
          log.error("Could not connect to master at {}, cannot proceed with replication. Will retry", target, e);
          continue;
        } finally {
          span.stop();
        }

        if (null == peerTserverStr) {
          // Something went wrong, and we didn't get a valid tserver from the remote for some reason
          log.warn("Did not receive tserver from master at {}, cannot proceed with replication. Will retry.", target);
          continue;
        }

        final HostAndPort peerTserver = HostAndPort.fromString(peerTserverStr);

        final long timeout = localConf.getTimeInMillis(Property.REPLICATION_RPC_TIMEOUT);

        // We have a tserver on the remote -- send the data its way.
        Status finalStatus;
        final long sizeLimit = conf.getAsBytes(Property.REPLICATION_MAX_UNIT_SIZE);
        try {
          if (p.getName().endsWith(RFILE_SUFFIX)) {
            span = Trace.start("RFile replication");
            try {
              finalStatus = replicateRFiles(peerContext, peerTserver, target, p, status, sizeLimit, remoteTableId, peerContext.rpcCreds(), helper, timeout);
            } finally {
              span.stop();
            }
          } else {
            span = Trace.start("WAL replication");
            try {
              finalStatus = replicateLogs(peerContext, peerTserver, target, p, status, sizeLimit, remoteTableId, peerContext.rpcCreds(), helper, accumuloUgi,
                  timeout);
            } finally {
              span.stop();
            }
          }

          log.debug("New status for {} after replicating to {} is {}", p, peerContext.getInstance(), ProtobufUtil.toString(finalStatus));

          return finalStatus;
        } catch (TTransportException | AccumuloException | AccumuloSecurityException e) {
          log.warn("Could not connect to remote server {}, will retry", peerTserverStr, e);
          sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
      }

      log.info("No progress was made after {} attempts to replicate {}, returning so file can be re-queued", numAttempts, p);

      // We made no status, punt on it for now, and let it re-queue itself for work
      return status;
    } finally {
      Trace.off();
    }
  }

  protected Status replicateRFiles(ClientContext peerContext, final HostAndPort peerTserver, final ReplicationTarget target, final Path p, final Status status,
      final long sizeLimit, final String remoteTableId, final TCredentials tcreds, final ReplicaSystemHelper helper, long timeout) throws TTransportException,
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
      ReplicationStats replResult = ReplicationClient.executeServicerWithReturn(peerContext, peerTserver, new RFileClientExecReturn(target, input, p,
          currentStatus, sizeLimit, remoteTableId, tcreds), timeout);

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

  protected Status replicateLogs(ClientContext peerContext, final HostAndPort peerTserver, final ReplicationTarget target, final Path p, final Status status,
      final long sizeLimit, final String remoteTableId, final TCredentials tcreds, final ReplicaSystemHelper helper, final UserGroupInformation accumuloUgi,
      long timeout) throws TTransportException, AccumuloException, AccumuloSecurityException {

    log.debug("Replication WAL to peer tserver");
    final Set<Integer> tids;
    final DataInputStream input;
    Span span = Trace.start("Read WAL header");
    span.data("file", p.toString());
    try {
      input = getWalStream(p);
    } catch (LogHeaderIncompleteException e) {
      log.warn("Could not read header from {}, assuming that there is no data present in the WAL, therefore replication is complete", p);
      Status newStatus;
      // Bump up the begin to the (infinite) end, trying to be accurate
      if (status.getInfiniteEnd()) {
        newStatus = Status.newBuilder(status).setBegin(Long.MAX_VALUE).build();
      } else {
        newStatus = Status.newBuilder(status).setBegin(status.getEnd()).build();
      }
      span = Trace.start("Update replication table");
      try {
        helper.recordNewStatus(p, newStatus, target);
      } catch (TableNotFoundException tnfe) {
        log.error("Tried to update status in replication table for {} as {}, but the table did not exist", p, ProtobufUtil.toString(newStatus), e);
        throw new RuntimeException("Replication table did not exist, will retry", e);
      } finally {
        span.stop();
      }
      return newStatus;
    } catch (IOException e) {
      log.error("Could not create stream for WAL", e);
      // No data sent (bytes nor records) and no progress made
      return status;
    } finally {
      span.stop();
    }

    log.debug("Skipping unwanted data in WAL");
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

    log.debug("Sending batches of data to peer tserver");

    Status lastStatus = status, currentStatus = status;
    final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
    while (true) {
      // Set some trace info
      span = Trace.start("Replicate WAL batch");
      span.data("Batch size (bytes)", Long.toString(sizeLimit));
      span.data("File", p.toString());
      span.data("Peer instance name", peerContext.getInstance().getInstanceName());
      span.data("Peer tserver", peerTserver.toString());
      span.data("Remote table ID", remoteTableId);

      ReplicationStats replResult;
      try {
        // Read and send a batch of mutations
        replResult = ReplicationClient.executeServicerWithReturn(peerContext, peerTserver, new WalClientExecReturn(target, input, p, currentStatus, sizeLimit,
            remoteTableId, tcreds, tids), timeout);
      } catch (Exception e) {
        log.error("Caught exception replicating data to {} at {}", peerContext.getInstance().getInstanceName(), peerTserver, e);
        throw e;
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
          if (null != accumuloUgi) {
            final Status copy = currentStatus;
            accumuloUgi.doAs(new PrivilegedAction<Void>() {
              @Override
              public Void run() {
                try {
                  helper.recordNewStatus(p, copy, target);
                } catch (Exception e) {
                  exceptionRef.set(e);
                }
                return null;
              }
            });
            Exception e = exceptionRef.get();
            if (null != e) {
              if (e instanceof TableNotFoundException) {
                throw (TableNotFoundException) e;
              } else if (e instanceof AccumuloSecurityException) {
                throw (AccumuloSecurityException) e;
              } else if (e instanceof AccumuloException) {
                throw (AccumuloException) e;
              } else {
                throw new RuntimeException("Received unexpected exception", e);
              }
            }
          } else {
            helper.recordNewStatus(p, currentStatus, target);
          }
        } catch (TableNotFoundException e) {
          log.error("Tried to update status in replication table for {} as {}, but the table did not exist", p, ProtobufUtil.toString(currentStatus), e);
          throw new RuntimeException("Replication table did not exist, will retry", e);
        } finally {
          span.stop();
        }

        log.debug("Recorded updated status for {}: {}", p, ProtobufUtil.toString(currentStatus));

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

    public WalClientExecReturn(ReplicationTarget target, DataInputStream input, Path p, Status status, long sizeLimit, String remoteTableId,
        TCredentials tcreds, Set<Integer> tids) {
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

      log.debug("Read {} WAL entries and retained {} bytes of WAL entries for replication to peer '{}'",
          (Long.MAX_VALUE == edits.entriesConsumed) ? "all remaining" : edits.entriesConsumed, edits.sizeInBytes, p);

      // If we have some edits to send
      if (0 < edits.walEdits.getEditsSize()) {
        log.debug("Sending {} edits", edits.walEdits.getEditsSize());
        long entriesReplicated = client.replicateLog(remoteTableId, edits.walEdits, tcreds);
        if (entriesReplicated != edits.numUpdates) {
          log.warn("Sent {} WAL entries for replication but {} were reported as replicated", edits.numUpdates, entriesReplicated);
        } else {
          log.debug("Replicated {} edits", entriesReplicated);
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

    public RFileClientExecReturn(ReplicationTarget target, DataInputStream input, Path p, Status status, long sizeLimit, String remoteTableId,
        TCredentials tcreds) {
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

  protected String getPassword(AccumuloConfiguration localConf, ReplicationTarget target) {
    requireNonNull(localConf);
    requireNonNull(target);

    Map<String,String> peerPasswords = localConf.getAllPropertiesWithPrefix(Property.REPLICATION_PEER_PASSWORD);
    String password = peerPasswords.get(Property.REPLICATION_PEER_PASSWORD.getKey() + target.getPeerName());
    if (null == password) {
      throw new IllegalArgumentException("Cannot get password for " + target.getPeerName());
    }
    return password;
  }

  protected String getKeytab(AccumuloConfiguration localConf, ReplicationTarget target) {
    requireNonNull(localConf);
    requireNonNull(target);

    Map<String,String> peerKeytabs = localConf.getAllPropertiesWithPrefix(Property.REPLICATION_PEER_KEYTAB);
    String keytab = peerKeytabs.get(Property.REPLICATION_PEER_KEYTAB.getKey() + target.getPeerName());
    if (null == keytab) {
      throw new IllegalArgumentException("Cannot get keytab for " + target.getPeerName());
    }
    return keytab;
  }

  protected String getPrincipal(AccumuloConfiguration localConf, ReplicationTarget target) {
    requireNonNull(localConf);
    requireNonNull(target);

    String peerName = target.getPeerName();
    String userKey = Property.REPLICATION_PEER_USER.getKey() + peerName;
    Map<String,String> peerUsers = localConf.getAllPropertiesWithPrefix(Property.REPLICATION_PEER_USER);

    String user = peerUsers.get(userKey);
    if (null == user) {
      throw new IllegalArgumentException("Cannot get user for " + target.getPeerName());
    }
    return user;
  }

  protected ClientContext getContextForPeer(AccumuloConfiguration localConf, ReplicationTarget target, String principal, AuthenticationToken token) {
    requireNonNull(localConf);
    requireNonNull(target);
    requireNonNull(principal);
    requireNonNull(token);

    return new ClientContext(getPeerInstance(target), new Credentials(principal, token), localConf);
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
          if (target.getSourceTableId().equals(key.tablet.getTableId())) {
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
    edits.edits = new ArrayList<>();
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
          if (target.getSourceTableId().equals(key.tablet.getTableId())) {
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
          log.trace("Ignorning WAL entry which doesn't contain mutations, should not have received such entries");
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
    // TODO This works like LogFileValue, and needs to be parsable by it, which makes this serialization brittle.
    // see matching TODO in BatchWriterReplicationReplayer

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

    // Add our name, and send it
    final String name = conf.get(Property.REPLICATION_NAME);
    if (StringUtils.isBlank(name)) {
      throw new IllegalArgumentException("Local system has no replication name configured");
    }

    out.writeInt(mutationsToSend);
    for (Mutation m : value.mutations) {
      // If we haven't yet replicated to this peer
      if (!m.getReplicationSources().contains(target.getPeerName())) {
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
    public int hashCode() {
      return Objects.hashCode(sizeInBytes + sizeInRecords + entriesConsumed);
    }

    @Override
    public boolean equals(Object o) {
      if (o != null) {
        if (ReplicationStats.class.isAssignableFrom(o.getClass())) {
          ReplicationStats other = (ReplicationStats) o;
          return sizeInBytes == other.sizeInBytes && sizeInRecords == other.sizeInRecords && entriesConsumed == other.entriesConsumed;
        }
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
    public int hashCode() {
      return super.hashCode() + Objects.hashCode(walEdits) + Objects.hashCode(numUpdates);
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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.replication;

import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoEnvironmentImpl;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer.Client;
import org.apache.accumulo.core.replication.thrift.WalEdits;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes.Exec;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.singletons.SingletonReservation;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.replication.ReplicaSystem;
import org.apache.accumulo.server.replication.ReplicaSystemHelper;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.log.DfsLogger.LogHeaderIncompleteException;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

@Deprecated
public class AccumuloReplicaSystem implements ReplicaSystem {

  private static final Logger log = LoggerFactory.getLogger(AccumuloReplicaSystem.class);
  private static final String RFILE_SUFFIX = "." + RFile.EXTENSION;

  private String instanceName, zookeepers;
  private AccumuloConfiguration conf;
  private ServerContext context;

  protected void setConf(AccumuloConfiguration conf) {
    this.conf = conf;
  }

  /**
   * Generate the configuration string for this ReplicaSystem
   */
  public static String buildConfiguration(String instanceName, String zookeepers) {
    return instanceName + "," + zookeepers;
  }

  @Override
  public void configure(ServerContext context, String configuration) {
    requireNonNull(configuration);

    // instance_name,zookeepers
    int index = configuration.indexOf(',');
    if (index == -1) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalArgumentException("Expected comma in configuration string");
    }

    instanceName = configuration.substring(0, index);
    zookeepers = configuration.substring(index + 1);
    conf = context.getConfiguration();
    this.context = context;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by admin")
  @Override
  public Status replicate(final Path p, final Status status, final ReplicationTarget target,
      final ReplicaSystemHelper helper) {
    final AccumuloConfiguration localConf = conf;

    log.debug("Replication RPC timeout is {}", localConf.get(Property.REPLICATION_RPC_TIMEOUT));

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

    if (keytab != null) {
      try {
        final UserGroupInformation accumuloUgi = UserGroupInformation.getCurrentUser();
        // Get a UGI with the principal + keytab
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal,
            keytab.getAbsolutePath());

        // Run inside a doAs to avoid nuking the Tserver's user
        return ugi.doAs((PrivilegedAction<Status>) () -> {
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
  private Status _replicate(final Path p, final Status status, final ReplicationTarget target,
      final ReplicaSystemHelper helper, final AccumuloConfiguration localConf,
      final ClientContext peerContext, final UserGroupInformation accumuloUgi) {

    Span span = TraceUtil.startSpan(this.getClass(), "_replicate");
    try (Scope replicaScope = span.makeCurrent()) {

      // Remote identifier is an integer (table id) in this case.
      final String remoteTableId = target.getRemoteIdentifier();

      // Attempt the replication of this status a number of times before giving up and
      // trying to replicate it again later some other time.
      int numAttempts = localConf.getCount(Property.REPLICATION_WORK_ATTEMPTS);
      for (int i = 0; i < numAttempts; i++) {
        log.debug("Attempt {}", i);
        String peerTserverStr;
        log.debug("Fetching peer tserver address");
        Span span2 = TraceUtil.startSpan(this.getClass(), "_replicate::Fetch peer tserver");
        try (Scope scope = span2.makeCurrent()) {
          // Ask the manager on the remote what TServer we should talk with to replicate the data
          peerTserverStr = ThriftClientTypes.REPLICATION_COORDINATOR.execute(peerContext,
              client -> client.getServicerAddress(remoteTableId, peerContext.rpcCreds()));
        } catch (AccumuloException | AccumuloSecurityException e) {
          // No progress is made
          log.error(
              "Could not connect to manager at {}, cannot proceed with replication. Will retry",
              target, e);
          TraceUtil.setException(span2, e, false);
          continue;
        } catch (RuntimeException e) {
          TraceUtil.setException(span2, e, true);
          throw e;
        } finally {
          span2.end();
        }

        if (peerTserverStr == null) {
          // Something went wrong, and we didn't get a valid tserver from the remote for some reason
          log.warn("Did not receive tserver from manager at {}, cannot proceed"
              + " with replication. Will retry.", target);
          continue;
        }

        final HostAndPort peerTserver = HostAndPort.fromString(peerTserverStr);

        final long timeout = localConf.getTimeInMillis(Property.REPLICATION_RPC_TIMEOUT);

        // We have a tserver on the remote -- send the data its way.
        Status finalStatus;
        final long sizeLimit = conf.getAsBytes(Property.REPLICATION_MAX_UNIT_SIZE);
        try {
          if (p.getName().endsWith(RFILE_SUFFIX)) {
            Span span3 = TraceUtil.startSpan(this.getClass(), "_replicate::RFile replication");
            try (Scope scope = span3.makeCurrent()) {
              finalStatus = replicateRFiles(peerContext, peerTserver, target, p, status, timeout);
            } catch (RuntimeException e) {
              TraceUtil.setException(span3, e, true);
              throw e;
            } finally {
              span3.end();
            }
          } else {
            Span span4 = TraceUtil.startSpan(this.getClass(), "_replicate::WAL replication");
            try (Scope scope = span4.makeCurrent()) {
              finalStatus = replicateLogs(peerContext, peerTserver, target, p, status, sizeLimit,
                  remoteTableId, peerContext.rpcCreds(), helper, accumuloUgi, timeout);
            } catch (RuntimeException e) {
              TraceUtil.setException(span4, e, true);
              throw e;
            } finally {
              span4.end();
            }
          }

          log.debug("New status for {} after replicating to {} is {}", p,
              peerContext.getInstanceName(), ProtobufUtil.toString(finalStatus));

          return finalStatus;
        } catch (TTransportException | AccumuloException | AccumuloSecurityException e) {
          log.warn("Could not connect to remote server {}, will retry", peerTserverStr, e);
          TraceUtil.setException(span, e, false);
          sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
      }

      log.info("No progress was made after {} attempts to replicate {},"
          + " returning so file can be re-queued", numAttempts, p);

      // We made no status, punt on it for now, and let it re-queue itself for work
      return status;
    } catch (RuntimeException e) {
      TraceUtil.setException(span, e, true);
      throw e;
    } finally {
      span.end();
    }
  }

  protected Status replicateRFiles(ClientContext peerContext, final HostAndPort peerTserver,
      final ReplicationTarget target, final Path p, final Status status, long timeout)
      throws TTransportException, AccumuloException, AccumuloSecurityException {

    Status lastStatus = status, currentStatus = status;
    while (true) {
      // Read and send a batch of mutations
      ReplicationStats replResult =
          executeServicerWithReturn(peerContext, peerTserver, new RFileClientExecReturn(), timeout);

      // Catch the overflow
      long newBegin = currentStatus.getBegin() + replResult.entriesConsumed;
      if (newBegin < 0) {
        newBegin = Long.MAX_VALUE;
      }

      currentStatus = Status.newBuilder(currentStatus).setBegin(newBegin).build();

      log.debug("Sent batch for replication of {} to {}, with new Status {}", p, target,
          ProtobufUtil.toString(currentStatus));

      // If we got a different status
      if (currentStatus.equals(lastStatus)) {
        log.debug("Did not replicate any new data for {} to {}, (state was {})", p, target,
            ProtobufUtil.toString(lastStatus));

        // otherwise, we didn't actually replicate (likely because there was error sending the
        // data)
        // we can just not record any updates, and it will be picked up again by the work assigner
        return status;
      } else {
        // If we don't have any more work, just quit
        if (StatusUtil.isWorkRequired(currentStatus)) {
          // Otherwise, let it loop and replicate some more data
          lastStatus = currentStatus;
        } else {
          return currentStatus;
        }
      }
    }
  }

  protected Status replicateLogs(ClientContext peerContext, final HostAndPort peerTserver,
      final ReplicationTarget target, final Path p, final Status status, final long sizeLimit,
      final String remoteTableId, final TCredentials tcreds, final ReplicaSystemHelper helper,
      final UserGroupInformation accumuloUgi, long timeout)
      throws TTransportException, AccumuloException, AccumuloSecurityException {

    log.debug("Replication WAL to peer tserver");
    final Set<Integer> tids;
    try (final FSDataInputStream fsinput = context.getVolumeManager().open(p);
        final DataInputStream input = getWalStream(p, fsinput)) {
      log.debug("Skipping unwanted data in WAL");
      Span span = TraceUtil.startSpan(this.getClass(), "replicateLogs::Consume WAL prefix");
      try (Scope scope = span.makeCurrent()) {
        span.setAttribute("file", p.toString());
        // We want to read all records in the WAL up to the "begin" offset contained in the Status
        // message,
        // building a Set of tids from DEFINE_TABLET events which correspond to table ids for future
        // mutations
        tids = consumeWalPrefix(target, input, status);
      } catch (IOException e) {
        log.warn("Unexpected error consuming file.");
        TraceUtil.setException(span, e, false);
        return status;
      } catch (RuntimeException e) {
        TraceUtil.setException(span, e, true);
        throw e;
      } finally {
        span.end();
      }

      log.debug("Sending batches of data to peer tserver");

      Status lastStatus = status, currentStatus = status;
      final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
      while (true) {
        ReplicationStats replResult;
        Span span2 = TraceUtil.startSpan(this.getClass(), "replicateLogs::Replicate WAL batch");
        try (Scope scope = span2.makeCurrent()) {
          span2.setAttribute("Batch size (bytes)", Long.toString(sizeLimit));
          span2.setAttribute("File", p.toString());
          span2.setAttribute("Peer instance name", peerContext.getInstanceName());
          span2.setAttribute("Peer tserver", peerTserver.toString());
          span2.setAttribute("Remote table ID", remoteTableId);

          // Read and send a batch of mutations
          replResult = executeServicerWithReturn(peerContext, peerTserver,
              new WalClientExecReturn(this, target, input, p, currentStatus, sizeLimit,
                  remoteTableId, tcreds, tids),
              timeout);
        } catch (RuntimeException e) {
          log.error("Caught exception replicating data to {} at {}", peerContext.getInstanceName(),
              peerTserver, e);
          TraceUtil.setException(span2, e, true);
          throw e;
        } finally {
          span2.end();
        }

        // Catch the overflow
        long newBegin = currentStatus.getBegin() + replResult.entriesConsumed;
        if (newBegin < 0) {
          newBegin = Long.MAX_VALUE;
        }

        currentStatus = Status.newBuilder(currentStatus).setBegin(newBegin).build();

        log.debug("Sent batch for replication of {} to {}, with new Status {}", p, target,
            ProtobufUtil.toString(currentStatus));

        // If we got a different status
        if (currentStatus.equals(lastStatus)) {
          log.debug("Did not replicate any new data for {} to {}, (state was {})", p, target,
              ProtobufUtil.toString(lastStatus));

          // otherwise, we didn't actually replicate (likely because there was error sending the
          // data)
          // we can just not record any updates, and it will be picked up again by the work assigner
          return status;
        } else {
          Span span3 =
              TraceUtil.startSpan(this.getClass(), "replicateLogs::Update replication table");
          try (Scope scope = span3.makeCurrent()) {
            if (accumuloUgi != null) {
              final Status copy = currentStatus;
              accumuloUgi.doAs((PrivilegedAction<Void>) () -> {
                try {
                  helper.recordNewStatus(p, copy, target);
                } catch (TableNotFoundException | AccumuloException | RuntimeException e) {
                  exceptionRef.set(e);
                }
                return null;
              });
              Exception e = exceptionRef.get();
              if (e != null) {
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
            log.error(
                "Tried to update status in replication table for {} as"
                    + " {}, but the table did not exist",
                p, ProtobufUtil.toString(currentStatus), e);
            TraceUtil.setException(span3, e, true);
            throw new RuntimeException("Replication table did not exist, will retry", e);
          } catch (RuntimeException e) {
            TraceUtil.setException(span3, e, true);
            throw e;
          } finally {
            span3.end();
          }

          log.debug("Recorded updated status for {}: {}", p, ProtobufUtil.toString(currentStatus));

          // If we don't have any more work, just quit
          if (StatusUtil.isWorkRequired(currentStatus)) {
            // Otherwise, let it loop and replicate some more data
            lastStatus = currentStatus;
          } else {
            return currentStatus;
          }
        }
      }
    } catch (LogHeaderIncompleteException e) {
      log.warn("Could not read header from {}, assuming that there is no data"
          + " present in the WAL, therefore replication is complete", p);
      Status newStatus;
      // Bump up the begin to the (infinite) end, trying to be accurate
      if (status.getInfiniteEnd()) {
        newStatus = Status.newBuilder(status).setBegin(Long.MAX_VALUE).build();
      } else {
        newStatus = Status.newBuilder(status).setBegin(status.getEnd()).build();
      }
      Span span4 = TraceUtil.startSpan(this.getClass(), "replicateLogs::Update replication table");
      try (Scope scope = span4.makeCurrent()) {
        helper.recordNewStatus(p, newStatus, target);
      } catch (TableNotFoundException tnfe) {
        log.error(
            "Tried to update status in replication table for {} as {}, but the table did not exist",
            p, ProtobufUtil.toString(newStatus), e);
        TraceUtil.setException(span4, e, true);
        throw new RuntimeException("Replication table did not exist, will retry", e);
      } catch (RuntimeException ex) {
        TraceUtil.setException(span4, e, true);
        throw ex;
      } finally {
        span4.end();
      }
      return newStatus;
    } catch (IOException e) {
      log.error("Could not create stream for WAL", e);
      // No data sent (bytes nor records) and no progress made
      return status;
    }
  }

  @Deprecated
  protected static class RFileClientExecReturn
      implements Exec<ReplicationStats,ReplicationServicer.Client> {

    @Override
    public ReplicationStats execute(Client client) {
      // No data sent (bytes nor records) and no progress made
      return new ReplicationStats(0L, 0L, 0L);
    }
  }

  protected String getPassword(AccumuloConfiguration localConf, ReplicationTarget target) {
    requireNonNull(localConf);
    requireNonNull(target);

    Map<String,String> peerPasswords =
        localConf.getAllPropertiesWithPrefix(Property.REPLICATION_PEER_PASSWORD);
    String password =
        peerPasswords.get(Property.REPLICATION_PEER_PASSWORD.getKey() + target.getPeerName());
    if (password == null) {
      throw new IllegalArgumentException("Cannot get password for " + target.getPeerName());
    }
    return password;
  }

  protected String getKeytab(AccumuloConfiguration localConf, ReplicationTarget target) {
    requireNonNull(localConf);
    requireNonNull(target);

    Map<String,String> peerKeytabs =
        localConf.getAllPropertiesWithPrefix(Property.REPLICATION_PEER_KEYTAB);
    String keytab =
        peerKeytabs.get(Property.REPLICATION_PEER_KEYTAB.getKey() + target.getPeerName());
    if (keytab == null) {
      throw new IllegalArgumentException("Cannot get keytab for " + target.getPeerName());
    }
    return keytab;
  }

  protected String getPrincipal(AccumuloConfiguration localConf, ReplicationTarget target) {
    requireNonNull(localConf);
    requireNonNull(target);

    String peerName = target.getPeerName();
    String userKey = Property.REPLICATION_PEER_USER.getKey() + peerName;
    Map<String,String> peerUsers =
        localConf.getAllPropertiesWithPrefix(Property.REPLICATION_PEER_USER);

    String user = peerUsers.get(userKey);
    if (user == null) {
      throw new IllegalArgumentException("Cannot get user for " + target.getPeerName());
    }
    return user;
  }

  protected ClientContext getContextForPeer(AccumuloConfiguration localConf,
      ReplicationTarget target, String principal, AuthenticationToken token) {
    requireNonNull(localConf);
    requireNonNull(target);
    requireNonNull(principal);
    requireNonNull(token);

    Properties properties = new Properties();
    properties.setProperty(ClientProperty.INSTANCE_NAME.getKey(), instanceName);
    properties.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), zookeepers);
    properties.setProperty(ClientProperty.AUTH_PRINCIPAL.getKey(), principal);
    ClientProperty.setAuthenticationToken(properties, token);

    return new ClientContext(SingletonReservation.noop(), ClientInfo.from(properties, token),
        localConf, Threads.UEH);
  }

  protected Set<Integer> consumeWalPrefix(ReplicationTarget target, DataInputStream wal,
      Status status) throws IOException {
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
          if (target.getSourceTableId().equals(key.tablet.tableId())) {
            desiredTids.add(key.tabletId);
          }
          break;
        default:
          break;
      }
    }

    return desiredTids;
  }

  public DataInputStream getWalStream(Path p, FSDataInputStream input)
      throws LogHeaderIncompleteException, IOException {
    Span span = TraceUtil.startSpan(this.getClass(), "getWalStream::Read WAL header");
    try (Scope scope = span.makeCurrent()) {
      span.setAttribute("file", p.toString());
      CryptoEnvironment env = new CryptoEnvironmentImpl(CryptoEnvironment.Scope.WAL);
      return DfsLogger.getDecryptingStream(input,
          context.getCryptoFactory().getService(env, conf.getAllCryptoProperties()));
    } catch (RuntimeException e) {
      TraceUtil.setException(span, e, true);
      throw e;
    } finally {
      span.end();
    }
  }

  protected WalReplication getWalEdits(ReplicationTarget target, DataInputStream wal, Path p,
      Status status, long sizeLimit, Set<Integer> desiredTids) {
    WalEdits edits = new WalEdits();
    edits.edits = new ArrayList<>();
    long size = 0L;
    long entriesConsumed = 0L;
    long numUpdates = 0L;
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    while (size < sizeLimit) {
      try {
        key.readFields(wal);
        value.readFields(wal);
      } catch (EOFException e) {
        log.debug("Caught EOFException reading {}", p, e);
        if (status.getInfiniteEnd() && status.getClosed()) {
          log.debug("{} is closed and has unknown length, assuming entire file has been consumed",
              p);
          entriesConsumed = Long.MAX_VALUE;
        }
        break;
      } catch (IOException e) {
        log.debug("Unexpected IOException reading {}", p, e);
        throw new UncheckedIOException(e);
      }

      entriesConsumed++;

      switch (key.event) {
        case DEFINE_TABLET:
          // For new DEFINE_TABLETs, we also need to record the new tids we see
          if (target.getSourceTableId().equals(key.tablet.tableId())) {
            desiredTids.add(key.tabletId);
          }
          break;
        case MUTATION:
        case MANY_MUTATIONS:
          // Only write out mutations for tids that are for the desired tablet
          if (desiredTids.contains(key.tabletId)) {

            byte[] data;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
              key.write(out);

              // Only write out the mutations that don't have the given ReplicationTarget
              // as a replicate source (this prevents infinite replication loops: a->b, b->a,
              // repeat)
              numUpdates += writeValueAvoidingReplicationCycles(out, value, target);

              out.flush();
              data = baos.toByteArray();
            } catch (IOException e) {
              log.debug("Unexpected IOException writing to a byte array output stream", e);
              throw new UncheckedIOException(e);
            }
            size += data.length;
            edits.addToEdits(ByteBuffer.wrap(data));
          }
          break;
        default:
          log.trace("Ignoring WAL entry which doesn't contain mutations,"
              + " should not have received such entries");
          break;
      }
    }

    return new WalReplication(edits, size, entriesConsumed, numUpdates);
  }

  /**
   * Wrapper around {@link LogFileValue#write(java.io.DataOutput)} which does not serialize
   * {@link Mutation}s that do not need to be replicate to the given {@link ReplicationTarget}
   */
  protected long writeValueAvoidingReplicationCycles(DataOutputStream out, LogFileValue value,
      ReplicationTarget target) throws IOException {
    // TODO This works like LogFileValue, and needs to be parsable by it, which makes this
    // serialization brittle.
    // see matching TODO in BatchWriterReplicationReplayer

    int mutationsToSend = 0;
    for (Mutation m : value.mutations) {
      if (!m.getReplicationSources().contains(target.getPeerName())) {
        mutationsToSend++;
      }
    }

    int mutationsRemoved = value.mutations.size() - mutationsToSend;
    if (mutationsRemoved > 0) {
      log.debug("Removing {} mutations from WAL entry as they have already been replicated to {}",
          mutationsRemoved, target.getPeerName());
    }

    // Add our name, and send it
    final String name = conf.get(Property.REPLICATION_NAME);
    if (name.isBlank()) {
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

  private static <T> T executeServicerWithReturn(ClientContext context, HostAndPort tserver,
      Exec<T,ReplicationServicer.Client> exec, long timeout)
      throws AccumuloException, AccumuloSecurityException {
    ReplicationServicer.Client client = null;
    try {
      client =
          ThriftUtil.getClient(ThriftClientTypes.REPLICATION_SERVICER, tserver, context, timeout);
      return exec.execute(client);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (TException e) {
      throw new AccumuloException(e);
    } finally {
      if (client != null)
        ThriftUtil.close(client, context);
    }
  }

}

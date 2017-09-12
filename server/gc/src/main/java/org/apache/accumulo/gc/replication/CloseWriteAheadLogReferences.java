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
package org.apache.accumulo.gc.replication;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalMarkerException;
import org.apache.accumulo.server.log.WalStateManager.WalState;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * It's impossible to know when all references to a WAL have been removed from the metadata table as the references are potentially spread across the entire
 * tablets row-space.
 * <p>
 * This tool scans the metadata table to collect a set of WALs that are still referenced. Then, each {@link Status} record from the metadata and replication
 * tables that point to that WAL can be "closed", by writing a new Status to the same key with the closed member true.
 */
public class CloseWriteAheadLogReferences implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(CloseWriteAheadLogReferences.class);

  private static final String RFILE_SUFFIX = "." + RFile.EXTENSION;

  private final AccumuloServerContext context;

  public CloseWriteAheadLogReferences(AccumuloServerContext context) {
    this.context = context;
  }

  @Override
  public void run() {
    // As long as we depend on a newer Guava than Hadoop uses, we have to make sure we're compatible with
    // what the version they bundle uses.
    Stopwatch sw = new Stopwatch();

    Connector conn;
    try {
      conn = context.getConnector();
    } catch (Exception e) {
      log.error("Could not create connector", e);
      throw new RuntimeException(e);
    }

    if (!ReplicationTable.isOnline(conn)) {
      log.debug("Replication table isn't online, not attempting to clean up wals");
      return;
    }

    Span findWalsSpan = Trace.start("findReferencedWals");
    HashSet<String> closed = null;
    try {
      sw.start();
      closed = getClosedLogs(conn);
    } finally {
      sw.stop();
      findWalsSpan.stop();
    }

    log.info("Found {} WALs referenced in metadata in {}", closed.size(), sw.toString());
    sw.reset();

    Span updateReplicationSpan = Trace.start("updateReplicationTable");
    long recordsClosed = 0;
    try {
      sw.start();
      recordsClosed = updateReplicationEntries(conn, closed);
    } finally {
      sw.stop();
      updateReplicationSpan.stop();
    }

    log.info("Closed {} WAL replication references in replication table in {}", recordsClosed, sw.toString());
  }

  /**
   * Construct the set of referenced WALs from zookeeper
   *
   * @param conn
   *          Connector
   * @return The Set of WALs that are referenced in the metadata table
   */
  protected HashSet<String> getClosedLogs(Connector conn) {
    WalStateManager wals = new WalStateManager(conn.getInstance(), ZooReaderWriter.getInstance());

    HashSet<String> result = new HashSet<>();
    try {
      for (Entry<Path,WalState> entry : wals.getAllState().entrySet()) {
        if (entry.getValue() == WalState.UNREFERENCED || entry.getValue() == WalState.CLOSED) {
          Path path = entry.getKey();
          log.debug("Found closed WAL " + path.toString());
          result.add(path.toString());
        }
      }
    } catch (WalMarkerException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  /**
   * Given the set of WALs which have references in the metadata table, close any status messages with reference that WAL.
   *
   * @param conn
   *          Connector
   * @param closedWals
   *          {@link Set} of paths to WALs that marked as closed or unreferenced in zookeeper
   */
  protected long updateReplicationEntries(Connector conn, Set<String> closedWals) {
    BatchScanner bs = null;
    BatchWriter bw = null;
    long recordsClosed = 0;
    try {
      bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
      bs = conn.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 4);
      bs.setRanges(Collections.singleton(Range.prefix(ReplicationSection.getRowPrefix())));
      bs.fetchColumnFamily(ReplicationSection.COLF);

      Text replFileText = new Text();
      for (Entry<Key,Value> entry : bs) {
        Status status;
        try {
          status = Status.parseFrom(entry.getValue().get());
        } catch (InvalidProtocolBufferException e) {
          log.error("Could not parse Status protobuf for {}", entry.getKey(), e);
          continue;
        }

        // Ignore things that aren't completely replicated as we can't delete those anyways
        MetadataSchema.ReplicationSection.getFile(entry.getKey(), replFileText);
        String replFile = replFileText.toString();
        boolean isClosed = closedWals.contains(replFile);

        // We only want to clean up WALs (which is everything but rfiles) and only when
        // metadata doesn't have a reference to the given WAL
        if (!status.getClosed() && !replFile.endsWith(RFILE_SUFFIX) && isClosed) {
          try {
            closeWal(bw, entry.getKey());
            recordsClosed++;
          } catch (MutationsRejectedException e) {
            log.error("Failed to submit delete mutation for {}", entry.getKey());
            continue;
          }
        }
      }
    } catch (TableNotFoundException e) {
      log.error("Replication table was deleted", e);
    } finally {
      if (null != bs) {
        bs.close();
      }

      if (null != bw) {
        try {
          bw.close();
        } catch (MutationsRejectedException e) {
          log.error("Failed to write delete mutations for replication table", e);
        }
      }
    }

    return recordsClosed;
  }

  /**
   * Write a closed {@link Status} mutation for the given {@link Key} using the provided {@link BatchWriter}
   *
   * @param bw
   *          BatchWriter
   * @param k
   *          Key to create close mutation from
   */
  protected void closeWal(BatchWriter bw, Key k) throws MutationsRejectedException {
    log.debug("Closing unreferenced WAL ({}) in metadata table", k.toStringNoTruncate());
    Mutation m = new Mutation(k.getRow());
    m.put(k.getColumnFamily(), k.getColumnQualifier(), StatusUtil.fileClosedValue());
    bw.addMutation(m);
  }

  private HostAndPort getMasterAddress() {
    try {
      List<String> locations = context.getInstance().getMasterLocations();
      if (locations.size() == 0)
        return null;
      return HostAndPort.fromString(locations.get(0));
    } catch (Exception e) {
      log.warn("Failed to obtain master host", e);
    }

    return null;
  }

  private MasterClientService.Client getMasterConnection() {
    final HostAndPort address = getMasterAddress();
    try {
      if (address == null) {
        log.warn("Could not fetch Master address");
        return null;
      }
      return ThriftUtil.getClient(new MasterClientService.Client.Factory(), address, context);
    } catch (Exception e) {
      log.warn("Issue with masterConnection (" + address + ") " + e, e);
    }
    return null;
  }

  /**
   * Get the active tabletservers as seen by the master.
   *
   * @return The active tabletservers, null if they can't be computed.
   */
  protected List<String> getActiveTservers(TInfo tinfo) {
    MasterClientService.Client client = null;

    List<String> tservers = null;
    try {
      client = getMasterConnection();

      // Could do this through InstanceOperations, but that would set a bunch of new Watchers via ZK on every tserver
      // node. The master is already tracking all of this info, so hopefully this is less overall work.
      if (null != client) {
        tservers = client.getActiveTservers(tinfo, context.rpcCreds());
      }
    } catch (TException e) {
      // If we can't fetch the tabletservers, we can't fetch any active WALs
      log.warn("Failed to fetch active tabletservers from the master", e);
      return null;
    } finally {
      ThriftUtil.returnClient(client);
    }

    return tservers;
  }

  protected List<String> getActiveWalsForServer(TInfo tinfo, HostAndPort server) {
    TabletClientService.Client tserverClient = null;
    try {
      tserverClient = ThriftUtil.getClient(new TabletClientService.Client.Factory(), server, context);
      return tserverClient.getActiveLogs(tinfo, context.rpcCreds());
    } catch (TTransportException e) {
      log.warn("Failed to fetch active write-ahead logs from " + server, e);
      return null;
    } catch (TException e) {
      log.warn("Failed to fetch active write-ahead logs from " + server, e);
      return null;
    } finally {
      ThriftUtil.returnClient(tserverClient);
    }
  }
}

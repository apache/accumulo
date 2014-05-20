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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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

  private Instance instance;
  private Credentials creds;

  public CloseWriteAheadLogReferences(Instance inst, Credentials creds) {
    this.instance = inst;
    this.creds = creds;
  }

  @Override
  public void run() {
    // As long as we depend on a newer Guava than Hadoop uses, we have to make sure we're compatible with
    // what the version they bundle uses.
    @SuppressWarnings("deprecation")
    Stopwatch sw = new Stopwatch();

    Connector conn;
    try {
      conn = instance.getConnector(creds.getPrincipal(), creds.getToken());
    } catch (Exception e) {
      log.error("Could not create connector", e);
      throw new RuntimeException(e);
    }

    if (!conn.tableOperations().exists(ReplicationTable.NAME)) {
      log.debug("Replication table doesn't exist, not attempting to clean up wals");
      return;
    }

    Span findWalsSpan = Trace.start("findReferencedWals");
    HashSet<String> referencedWals = null;
    try {
      sw.start();
      referencedWals = getReferencedWals(conn);
    } finally {
      sw.stop();
      findWalsSpan.stop();
    }

    log.info("Found " + referencedWals.size() + " WALs referenced in metadata in " + sw.toString());
    sw.reset();

    Span updateReplicationSpan = Trace.start("updateReplicationTable");
    long recordsClosed = 0;
    try {
      sw.start();
      recordsClosed = updateReplicationEntries(conn, referencedWals);
    } finally {
      sw.stop();
      updateReplicationSpan.stop();
    }

    log.info("Closed " + recordsClosed + " WAL replication references in replication table in " + sw.toString());
  }

  /**
   * Construct the set of referenced WALs from the metadata table
   * 
   * @param conn
   *          Connector
   * @return The Set of WALs that are referenced in the metadata table
   */
  protected HashSet<String> getReferencedWals(Connector conn) {
    // Make a bounded cache to alleviate repeatedly creating the same Path object
    final LoadingCache<String,String> normalizedWalPaths = CacheBuilder.newBuilder().maximumSize(1024).concurrencyLevel(1)
        .build(new CacheLoader<String,String>() {

          @Override
          public String load(String key) {
            return new Path(key).toString();
          }

        });

    HashSet<String> referencedWals = new HashSet<>();
    BatchScanner bs = null;
    try {
      // TODO Configurable number of threads
      bs = conn.createBatchScanner(MetadataTable.NAME, new Authorizations(), 4);
      bs.setRanges(Collections.singleton(TabletsSection.getRange()));
      bs.fetchColumnFamily(LogColumnFamily.NAME);

      // For each log key/value in the metadata table
      for (Entry<Key,Value> entry : bs) {
        // The value may contain multiple WALs
        LogEntry logEntry = LogEntry.fromKeyValue(entry.getKey(), entry.getValue());

        // Normalize each log file (using Path) and add it to the set
        for (String logFile : logEntry.logSet) {
          referencedWals.add(normalizedWalPaths.get(logFile));
        }
      }
    } catch (TableNotFoundException e) {
      // uhhhh
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      log.error("Failed to normalize WAL file path", e);
      throw new RuntimeException(e);
    } finally {
      if (null != bs) {
        bs.close();
      }
    }

    return referencedWals;
  }

  /**
   * Given the set of WALs which have references in the metadata table, close any status messages with reference that WAL.
   * 
   * @param conn
   *          Connector
   * @param referencedWals
   *          {@link Set} of paths to WALs that are referenced in the tablets section of the metadata table
   */
  protected long updateReplicationEntries(Connector conn, Set<String> referencedWals) {
    BatchScanner bs = null;
    BatchWriter bw = null;
    long recordsClosed = 0;
    try {
      bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
      bs = conn.createBatchScanner(MetadataTable.NAME, new Authorizations(), 4);
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
        entry.getKey().getRow(replFileText);
        String replFile = replFileText.toString().substring(ReplicationSection.getRowPrefix().length());

        // We only want to clean up WALs (which is everything but rfiles) and only when
        // metadata doesn't have a reference to the given WAL
        if (!status.getClosed() && !replFile.endsWith(RFILE_SUFFIX) && !referencedWals.contains(replFile)) {
          try {
            closeWal(bw, entry.getKey());
            recordsClosed++;
          } catch (MutationsRejectedException e) {
            log.error("Failed to submit delete mutation for " + entry.getKey());
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
   * @throws MutationsRejectedException
   */
  protected void closeWal(BatchWriter bw, Key k) throws MutationsRejectedException {
    log.debug("Closing unreferenced WAL ({}) in metadata table", k.toStringNoTruncate());
    Mutation m = new Mutation(k.getRow());
    m.put(k.getColumnFamily(), k.getColumnQualifier(), StatusUtil.fileClosedValue());
    bw.addMutation(m);
  }

}

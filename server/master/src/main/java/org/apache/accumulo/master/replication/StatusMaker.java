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

import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTableOfflineException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Reads replication records from the metadata table and creates status records in the replication table. Deletes the record from the metadata table when it's
 * closed.
 */
public class StatusMaker {
  private static final Logger log = LoggerFactory.getLogger(StatusMaker.class);

  private final Connector conn;

  private BatchWriter replicationWriter, metadataWriter;
  private String sourceTableName = MetadataTable.NAME;

  public StatusMaker(Connector conn) {
    this.conn = conn;
  }

  /**
   * Not for public use -- visible only for testing
   * <p>
   * Used to read records from a table other than 'metadata'
   *
   * @param table
   *          The table to read from
   */
  public void setSourceTableName(String table) {
    this.sourceTableName = table;
  }

  public void run() {
    Span span = Trace.start("replicationStatusMaker");
    try {
      // Read from a source table (typically accumulo.metadata)
      final Scanner s;
      try {
        s = conn.createScanner(sourceTableName, Authorizations.EMPTY);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }

      // Only pull replication records
      s.fetchColumnFamily(ReplicationSection.COLF);
      s.setRange(ReplicationSection.getRange());

      Text file = new Text();
      for (Entry<Key,Value> entry : s) {
        // Get a writer to the replication table
        if (null == replicationWriter) {
          // Ensures table is online
          try {
            ReplicationTable.setOnline(conn);
            replicationWriter = ReplicationTable.getBatchWriter(conn);
          } catch (ReplicationTableOfflineException | AccumuloSecurityException | AccumuloException e) {
            log.warn("Replication table did not come online");
            replicationWriter = null;
            return;
          }
        }
        // Extract the useful bits from the status key
        MetadataSchema.ReplicationSection.getFile(entry.getKey(), file);
        Table.ID tableId = MetadataSchema.ReplicationSection.getTableId(entry.getKey());

        Status status;
        try {
          status = Status.parseFrom(entry.getValue().get());
        } catch (InvalidProtocolBufferException e) {
          log.warn("Could not deserialize protobuf for {}", file);
          continue;
        }

        log.debug("Creating replication status record for {} on table {} with {}.", file, tableId, ProtobufUtil.toString(status));

        Span workSpan = Trace.start("createStatusMutations");
        try {
          // Create entries in the replication table from the metadata table
          if (!addStatusRecord(file, tableId, entry.getValue())) {
            continue;
          }
        } finally {
          workSpan.stop();
        }

        if (status.getClosed()) {
          Span orderSpan = Trace.start("recordStatusOrder");
          try {
            if (!addOrderRecord(file, tableId, status, entry.getValue())) {
              continue;
            }
          } finally {
            orderSpan.stop();
          }

          Span deleteSpan = Trace.start("deleteClosedStatus");
          try {
            deleteStatusRecord(entry.getKey());
          } finally {
            deleteSpan.stop();
          }
        }
      }
    } finally {
      span.stop();
    }
  }

  protected void setBatchWriter(BatchWriter bw) {
    this.replicationWriter = bw;
  }

  /**
   * Create a status record in the replication table
   */
  protected boolean addStatusRecord(Text file, Table.ID tableId, Value v) {
    try {
      Mutation m = new Mutation(file);
      m.put(StatusSection.NAME, new Text(tableId.getUtf8()), v);

      try {
        replicationWriter.addMutation(m);
      } catch (MutationsRejectedException e) {
        log.warn("Failed to write work mutations for replication, will retry", e);
        return false;
      }
    } finally {
      try {
        replicationWriter.flush();
      } catch (MutationsRejectedException e) {
        log.warn("Failed to write work mutations for replication, will retry", e);
        return false;
      }
    }

    return true;
  }

  /**
   * Create a record to track when the file was closed to ensure that replication preference is given to files that have been closed the longest and allow the
   * work assigner to try to replicate in order that data was ingested (avoid replay in different order)
   *
   * @param file
   *          File being replicated
   * @param tableId
   *          Table ID the file was used by
   * @param stat
   *          Status msg
   * @param value
   *          Serialized version of the Status msg
   */
  protected boolean addOrderRecord(Text file, Table.ID tableId, Status stat, Value value) {
    try {
      if (!stat.hasCreatedTime()) {
        log.error("Status record ({}) for {} in table {} was written to metadata table which lacked createdTime", ProtobufUtil.toString(stat), file, tableId);
        return false;
      }

      log.info("Creating order record for {} for {} with {}", file, tableId, ProtobufUtil.toString(stat));

      Mutation m = OrderSection.createMutation(file.toString(), stat.getCreatedTime());
      OrderSection.add(m, tableId, value);

      try {
        replicationWriter.addMutation(m);
      } catch (MutationsRejectedException e) {
        log.warn("Failed to write order mutation for replication, will retry", e);
        return false;
      }
    } finally {
      try {
        replicationWriter.flush();
      } catch (MutationsRejectedException e) {
        log.warn("Failed to write order mutation for replication, will retry", e);
        return false;
      }
    }

    return true;
  }

  /**
   * Because there is only one active Master, and thus one active StatusMaker, the only safe time that we can issue the delete for a Status which is closed is
   * immediately after writing it to the replication table.
   * <p>
   * If we try to defer and delete these entries in another thread/process, we will have no assurance that the Status message was propagated to the replication
   * table. It is easiest, in terms of concurrency, to do this all in one step.
   *
   * @param k
   *          The Key to delete
   */
  protected void deleteStatusRecord(Key k) {
    log.debug("Deleting {} from metadata table as it's no longer needed", k.toStringNoTruncate());
    if (null == metadataWriter) {
      try {
        metadataWriter = conn.createBatchWriter(sourceTableName, new BatchWriterConfig());
      } catch (TableNotFoundException e) {
        throw new RuntimeException("Metadata table doesn't exist");
      }
    }

    try {
      Mutation m = new Mutation(k.getRow());
      m.putDelete(k.getColumnFamily(), k.getColumnQualifier());
      metadataWriter.addMutation(m);
      metadataWriter.flush();
    } catch (MutationsRejectedException e) {
      log.warn("Failed to delete status mutations for metadata table, will retry", e);
    }
  }
}

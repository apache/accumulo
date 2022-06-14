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
package org.apache.accumulo.manager.replication;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTableOfflineException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

/**
 * Reads replication records from the metadata table and creates status records in the replication
 * table. Deletes the record from the metadata table when it's closed.
 */
@Deprecated
public class StatusMaker {
  private static final Logger log = LoggerFactory.getLogger(StatusMaker.class);

  private final AccumuloClient client;
  private final VolumeManager fs;

  private BatchWriter replicationWriter, metadataWriter;
  private String sourceTableName = MetadataTable.NAME;

  public StatusMaker(AccumuloClient client, VolumeManager fs) {
    this.client = client;
    this.fs = fs;
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
    Span span = TraceUtil.startSpan(this.getClass(), "replicationStatusMaker");
    try (Scope scope = span.makeCurrent()) {
      // Read from a source table (typically accumulo.metadata)
      final Scanner s;
      try {
        s = client.createScanner(sourceTableName, Authorizations.EMPTY);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }

      // Only pull replication records
      s.fetchColumnFamily(ReplicationSection.COLF);
      s.setRange(ReplicationSection.getRange());

      Text file = new Text();
      for (Entry<Key,Value> entry : s) {
        // Get a writer to the replication table
        if (replicationWriter == null) {
          // Ensures table is online
          try {
            ReplicationTable.setOnline(client);
            replicationWriter = ReplicationTable.getBatchWriter(client);
          } catch (ReplicationTableOfflineException | AccumuloSecurityException
              | AccumuloException e) {
            log.warn("Replication table did not come online");
            replicationWriter = null;
            return;
          }
        }
        // Extract the useful bits from the status key
        ReplicationSection.getFile(entry.getKey(), file);
        TableId tableId = ReplicationSection.getTableId(entry.getKey());

        Status status;
        try {
          status = Status.parseFrom(entry.getValue().get());
        } catch (InvalidProtocolBufferException e) {
          log.warn("Could not deserialize protobuf for {}", file);
          continue;
        }

        log.debug("Creating replication status record for {} on table {} with {}.", file, tableId,
            ProtobufUtil.toString(status));

        Span childSpan = TraceUtil.startSpan(this.getClass(), "createStatusMutations");
        try (Scope childScope = span.makeCurrent()) {
          // Create entries in the replication table from the metadata table
          if (!addStatusRecord(file, tableId, entry.getValue())) {
            continue;
          }
        } catch (Exception e) {
          TraceUtil.setException(childSpan, e, true);
          throw e;
        } finally {
          childSpan.end();
        }

        if (status.getClosed()) {
          Span closedSpan = TraceUtil.startSpan(this.getClass(), "recordStatusOrder");
          try (Scope childScope = closedSpan.makeCurrent()) {
            if (!addOrderRecord(file, tableId, status, entry.getValue())) {
              continue;
            }
          } catch (Exception e) {
            TraceUtil.setException(closedSpan, e, true);
            throw e;
          } finally {
            closedSpan.end();
          }

          Span deleteSpan = TraceUtil.startSpan(this.getClass(), "deleteClosedStatus");
          try (Scope childScope = deleteSpan.makeCurrent()) {
            deleteStatusRecord(entry.getKey());
          } catch (Exception e) {
            TraceUtil.setException(deleteSpan, e, true);
            throw e;
          } finally {
            deleteSpan.end();
          }
        }
      }
    } catch (Exception e) {
      TraceUtil.setException(span, e, true);
      throw e;
    } finally {
      span.end();
    }
  }

  /**
   * Create a status record in the replication table
   */
  protected boolean addStatusRecord(Text file, TableId tableId, Value v) {
    try {
      Mutation m = new Mutation(file);
      m.put(StatusSection.NAME, new Text(tableId.canonical()), v);

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
   * Create a record to track when the file was closed to ensure that replication preference is
   * given to files that have been closed the longest and allow the work assigner to try to
   * replicate in order that data was ingested (avoid replay in different order)
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
  protected boolean addOrderRecord(Text file, TableId tableId, Status stat, Value value) {
    try {
      if (!stat.hasCreatedTime()) {
        try {
          // If the createdTime is not set, work around the issue by retrieving the WAL creation
          // time
          // from HDFS (or the current time if the WAL does not exist). See ACCUMULO-4751
          long createdTime = setAndGetCreatedTime(new Path(file.toString()), tableId.toString());
          stat = Status.newBuilder(stat).setCreatedTime(createdTime).build();
          value = ProtobufUtil.toValue(stat);
          log.debug("Status was lacking createdTime, set to {} for {}", createdTime, file);
        } catch (IOException e) {
          log.warn("Failed to get file status, will retry", e);
          return false;
        } catch (MutationsRejectedException e) {
          log.warn("Failed to write status mutation for replication, will retry", e);
          return false;
        }
      }

      log.info("Creating order record for {} for {} with {}", file, tableId,
          ProtobufUtil.toString(stat));

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
   * Because there is only one active Manager, and thus one active StatusMaker, the only safe time
   * that we can issue the delete for a Status which is closed is immediately after writing it to
   * the replication table.
   * <p>
   * If we try to defer and delete these entries in another thread/process, we will have no
   * assurance that the Status message was propagated to the replication table. It is easiest, in
   * terms of concurrency, to do this all in one step.
   *
   * @param k
   *          The Key to delete
   */
  protected void deleteStatusRecord(Key k) {
    log.debug("Deleting {} from metadata table as it's no longer needed", k.toStringNoTruncate());
    if (metadataWriter == null) {
      try {
        metadataWriter = client.createBatchWriter(sourceTableName);
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

  private long setAndGetCreatedTime(Path file, String tableId)
      throws IOException, MutationsRejectedException {
    long createdTime;
    if (fs.exists(file)) {
      createdTime = fs.getFileStatus(file).getModificationTime();
    } else {
      createdTime = System.currentTimeMillis();
    }

    Status status = Status.newBuilder().setCreatedTime(createdTime).build();
    Mutation m = new Mutation(new Text(ReplicationSection.getRowPrefix() + file));
    m.put(ReplicationSection.COLF, new Text(tableId), ProtobufUtil.toValue(status));
    replicationWriter.addMutation(m);
    replicationWriter.flush();

    return createdTime;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.replication;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTableOfflineException;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Delete replication entries from the replication table that are fully replicated and closed
 */
public class RemoveCompleteReplicationRecords implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(RemoveCompleteReplicationRecords.class);

  private AccumuloClient client;

  public RemoveCompleteReplicationRecords(AccumuloClient client) {
    this.client = client;
  }

  @Override
  public void run() {
    BatchScanner bs;
    BatchWriter bw;
    try {
      bs = ReplicationTable.getBatchScanner(client, 4);
      bw = ReplicationTable.getBatchWriter(client);

      if (bs == null || bw == null)
        throw new AssertionError("Inconceivable; an exception should have been"
            + " thrown, but 'bs' or 'bw' was null instead");
    } catch (ReplicationTableOfflineException e) {
      log.trace("Not attempting to remove complete replication records as the"
          + " table ({}) isn't yet online", ReplicationTable.NAME);
      return;
    }

    bs.setRanges(Collections.singleton(new Range()));
    IteratorSetting cfg = new IteratorSetting(50, WholeRowIterator.class);
    StatusSection.limit(bs);
    WorkSection.limit(bs);
    bs.addScanIterator(cfg);

    long recordsRemoved = 0;
    long startTime = System.nanoTime();
    Duration duration;
    try {
      recordsRemoved = removeCompleteRecords(client, bs, bw);
    } finally {
      bs.close();
      try {
        bw.close();
      } catch (MutationsRejectedException e) {
        log.error("Error writing mutations to {}, will retry", ReplicationTable.NAME, e);
      }
      duration = Duration.ofNanos(System.nanoTime() - startTime);
    }

    log.info("Removed {} complete replication entries from the table {} in {}", recordsRemoved,
        ReplicationTable.NAME, duration);
  }

  /**
   * Removes {@link Status} records read from the given {@code bs} and writes a delete, using the
   * given {@code bw}, when that {@link Status} is fully replicated and closed, as defined by
   * {@link StatusUtil#isSafeForRemoval(org.apache.accumulo.server.replication.proto.Replication.Status)}.
   *
   * @param client
   *          Accumulo client
   * @param bs
   *          A BatchScanner to read replication status records from
   * @param bw
   *          A BatchWriter to write deletes to
   * @return Number of records removed
   */
  protected long removeCompleteRecords(AccumuloClient client, BatchScanner bs, BatchWriter bw) {
    Text row = new Text(), colf = new Text(), colq = new Text();
    long recordsRemoved = 0;

    // For each row in the replication table
    for (Entry<Key,Value> rowEntry : bs) {
      SortedMap<Key,Value> columns;
      try {
        columns = WholeRowIterator.decodeRow(rowEntry.getKey(), rowEntry.getValue());
      } catch (IOException e) {
        log.error("Could not deserialize {} with WholeRowIterator", rowEntry.getKey().getRow(), e);
        continue;
      }

      rowEntry.getKey().getRow(row);

      // Try to remove the row (all or nothing)
      recordsRemoved += removeRowIfNecessary(bw, columns, row, colf, colq);
    }

    return recordsRemoved;
  }

  protected long removeRowIfNecessary(BatchWriter bw, SortedMap<Key,Value> columns, Text row,
      Text colf, Text colq) {
    long recordsRemoved = 0;
    if (columns.isEmpty()) {
      return recordsRemoved;
    }

    Mutation m = new Mutation(row);
    Map<TableId,Long> tableToTimeCreated = new HashMap<>();
    for (Entry<Key,Value> entry : columns.entrySet()) {
      Status status = null;
      try {
        status = Status.parseFrom(entry.getValue().get());
      } catch (InvalidProtocolBufferException e) {
        log.error("Encountered unparsable protobuf for key: {}",
            entry.getKey().toStringNoTruncate());
        continue;
      }

      // If a column in the row isn't ready for removal, we keep the whole row
      if (!StatusUtil.isSafeForRemoval(status)) {
        return 0L;
      }

      Key k = entry.getKey();
      k.getColumnFamily(colf);
      k.getColumnQualifier(colq);

      log.debug("Removing {} {}:{} from replication table", row, colf, colq);

      m.putDelete(colf, colq);

      TableId tableId;
      if (StatusSection.NAME.equals(colf)) {
        tableId = TableId.of(colq.toString());
      } else if (WorkSection.NAME.equals(colf)) {
        ReplicationTarget target = ReplicationTarget.from(colq);
        tableId = target.getSourceTableId();
      } else {
        throw new RuntimeException("Got unexpected column");
      }

      if (status.hasCreatedTime()) {
        Long timeClosed = tableToTimeCreated.get(tableId);
        if (timeClosed == null) {
          tableToTimeCreated.put(tableId, status.getCreatedTime());
        } else if (timeClosed != status.getCreatedTime()) {
          log.warn("Found multiple values for timeClosed for {}: {} and {}", row, timeClosed,
              status.getCreatedTime());
        }
      }

      recordsRemoved++;
    }

    List<Mutation> mutations = new ArrayList<>();
    mutations.add(m);
    for (Entry<TableId,Long> entry : tableToTimeCreated.entrySet()) {
      log.info("Removing order mutation for table {} at {} for {}", entry.getKey(),
          entry.getValue(), row);
      Mutation orderMutation = OrderSection.createMutation(row.toString(), entry.getValue());
      orderMutation.putDelete(OrderSection.NAME, new Text(entry.getKey().canonical()));
      mutations.add(orderMutation);
    }

    // Send the mutation deleting all the columns at once.
    // If we send them not as a single Mutation, we run the risk of having some of them be applied
    // which would mean that we might accidentally re-replicate data. We want to get rid of them all
    // at once
    // or not at all.
    try {
      bw.addMutations(mutations);
      bw.flush();
    } catch (MutationsRejectedException e) {
      log.error("Could not submit mutation to remove columns for {} in replication table", row, e);
      return 0L;
    }

    return recordsRemoved;
  }
}

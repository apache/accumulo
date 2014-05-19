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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Delete replication entries from {@link ReplicationTable#NAME} that are full replicated and closed
 */
public class RemoveCompleteReplicationRecords implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(RemoveCompleteReplicationRecords.class);

  private Connector conn;

  public RemoveCompleteReplicationRecords(Connector conn) {
    this.conn = conn;
  }

  @Override
  public void run() {
    BatchScanner bs;
    BatchWriter bw;
    try {
      bs = ReplicationTable.getBatchScanner(conn, 4);
      bw = ReplicationTable.getBatchWriter(conn);
    } catch (TableNotFoundException e) {
      log.debug("Not attempting to remove complete replication records as the table ({}) doesn't yet exist", ReplicationTable.NAME);
      return;
    }

    bs.setRanges(Collections.singleton(new Range()));
    IteratorSetting cfg = new IteratorSetting(50, WholeRowIterator.class);
    StatusSection.limit(bs);
    WorkSection.limit(bs);
    bs.addScanIterator(cfg);

    @SuppressWarnings("deprecation")
    Stopwatch sw = new Stopwatch();
    long recordsRemoved = 0;
    try {
      sw.start();
      recordsRemoved = removeCompleteRecords(conn, bs, bw);
    } finally {
      if (null != bs) {
        bs.close();
      }
      if (null != bw) {
        try {
          bw.close();
        } catch (MutationsRejectedException e) {
          log.error("Error writing mutations to {}, will retry", ReplicationTable.NAME, e);
        }
      }

      sw.stop();
    }

    log.info("Removed {} complete replication entries from the table {}", recordsRemoved, ReplicationTable.NAME);
  }

  /**
   * Removes {@link Status} records read from the given {@code bs} and writes a delete, using the given {@code bw}, when that {@link Status} is fully replicated
   * and closed, as defined by {@link StatusUtil#isSafeForRemoval(Status)}.
   * 
   * @param conn
   *          A Connector
   * @param bs
   *          A BatchScanner to read replication status records from
   * @param bw
   *          A BatchWriter to write deletes to
   * @return Number of records removed
   */
  protected long removeCompleteRecords(Connector conn, BatchScanner bs, BatchWriter bw) {
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

  protected long removeRowIfNecessary(BatchWriter bw, SortedMap<Key,Value> columns, Text row, Text colf, Text colq) {
    long recordsRemoved = 0;
    if (columns.isEmpty()) {
      return recordsRemoved;
    }

    Mutation m = new Mutation(row);
    Status status = null;
    for (Entry<Key,Value> entry : columns.entrySet()) {
      try {
        status = Status.parseFrom(entry.getValue().get());
      } catch (InvalidProtocolBufferException e) {
        log.error("Encountered unparsable protobuf for key: {}", entry.getKey().toStringNoTruncate());
        continue;
      }

      // If a column in the row isn't ready for removal, we keep the whole row
      if (!StatusUtil.isSafeForRemoval(status)) {
        return 0l;
      }

      Key k = entry.getKey();
      k.getColumnFamily(colf);
      k.getColumnQualifier(colq);

      m.putDelete(colf, colq);

      recordsRemoved++;
    }

    log.info("Removing {} from the replication table", row);

    ReplicationTarget target = ReplicationTarget.from(colq);

    Mutation orderMutation = OrderSection.createMutation(row.toString(), status.getClosedTime());
    orderMutation.putDelete(OrderSection.NAME, new Text(target.getSourceTableId()));

    // Send the mutation deleting all the columns at once.
    // If we send them not as a single Mutation, we run the risk of having some of them be applied
    // which would mean that we might accidentally re-replicate data. We want to get rid of them all at once
    // or not at all.
    try {
      bw.addMutations(Arrays.asList(m, orderMutation));
      bw.flush();
    } catch (MutationsRejectedException e) {
      log.error("Could not submit mutation to remove columns for {} in replication table", row, e);
      return 0l;
    }

    return recordsRemoved;
  }
}

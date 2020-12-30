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
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.accumulo.core.protobuf.ProtobufUtil;
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
 * Update the status record in the replication table with work that has been replicated to each
 * configured peer.
 */
public class FinishedWorkUpdater implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(FinishedWorkUpdater.class);

  private final AccumuloClient client;

  public FinishedWorkUpdater(AccumuloClient client) {
    this.client = client;
  }

  @Override
  public void run() {
    log.trace("Looking for finished replication work");

    if (!ReplicationTable.isOnline(client)) {
      log.trace("Replication table is not yet online, will retry");
      return;
    }

    BatchScanner bs;
    BatchWriter replBw;
    try {
      bs = ReplicationTable.getBatchScanner(client, 4);
      replBw = ReplicationTable.getBatchWriter(client);
    } catch (ReplicationTableOfflineException e) {
      log.debug("Table is no longer online, will retry");
      return;
    }

    IteratorSetting cfg = new IteratorSetting(50, WholeRowIterator.class);
    bs.addScanIterator(cfg);
    WorkSection.limit(bs);
    bs.setRanges(Collections.singleton(new Range()));

    try {
      for (Entry<Key,Value> serializedRow : bs) {
        SortedMap<Key,Value> wholeRow;
        try {
          wholeRow = WholeRowIterator.decodeRow(serializedRow.getKey(), serializedRow.getValue());
        } catch (IOException e) {
          log.warn("Could not deserialize whole row with key {}",
              serializedRow.getKey().toStringNoTruncate(), e);
          continue;
        }

        log.debug("Processing work progress for {} with {} columns",
            serializedRow.getKey().getRow(), wholeRow.size());

        Map<TableId,Long> tableIdToProgress = new HashMap<>();
        boolean error = false;
        Text buffer = new Text();

        // We want to determine what the minimum point that all Work entries have replicated to
        for (Entry<Key,Value> entry : wholeRow.entrySet()) {
          Status status;
          try {
            status = Status.parseFrom(entry.getValue().get());
          } catch (InvalidProtocolBufferException e) {
            log.warn("Could not deserialize protobuf for {}", entry.getKey(), e);
            error = true;
            break;
          }

          // Get the replication target for the work record
          entry.getKey().getColumnQualifier(buffer);
          ReplicationTarget target = ReplicationTarget.from(buffer);

          // Initialize the value in the map if we don't have one
          if (!tableIdToProgress.containsKey(target.getSourceTableId())) {
            tableIdToProgress.put(target.getSourceTableId(), Long.MAX_VALUE);
          }

          // Find the minimum value for begin (everyone has replicated up to this offset in the
          // file)
          tableIdToProgress.put(target.getSourceTableId(),
              Math.min(tableIdToProgress.get(target.getSourceTableId()), status.getBegin()));
        }

        if (error) {
          continue;
        }

        // Update the replication table for each source table we found work records for
        for (Entry<TableId,Long> entry : tableIdToProgress.entrySet()) {
          // If the progress is 0, then no one has replicated anything, and we don't need to update
          // anything
          if (entry.getValue() == 0) {
            continue;
          }

          serializedRow.getKey().getRow(buffer);

          log.debug("For {}, source table ID {} has replicated through {}",
              serializedRow.getKey().getRow(), entry.getKey(), entry.getValue());

          Mutation replMutation = new Mutation(buffer);

          // Set that we replicated at least this much data, ignoring the other fields
          Status updatedStatus = StatusUtil.replicated(entry.getValue());
          Value serializedUpdatedStatus = ProtobufUtil.toValue(updatedStatus);

          // Pull the sourceTableId into a Text
          TableId srcTableId = entry.getKey();

          // Make the mutation
          StatusSection.add(replMutation, srcTableId, serializedUpdatedStatus);

          log.debug("Updating replication status entry for {} with {}",
              serializedRow.getKey().getRow(), ProtobufUtil.toString(updatedStatus));

          try {
            replBw.addMutation(replMutation);
          } catch (MutationsRejectedException e) {
            log.error("Error writing mutations to update replication Status"
                + " messages in StatusSection, will retry", e);
            return;
          }
        }
      }
    } finally {
      log.debug("Finished updating files with completed replication work");

      bs.close();

      try {
        replBw.close();
      } catch (MutationsRejectedException e) {
        log.error("Error writing mutations to update replication Status"
            + " messages in StatusSection, will retry", e);
      }
    }
  }

}

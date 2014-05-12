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
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
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
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Delete replication entries that are full replicated and closed
 */
public class RemoveCompleteReplicationRecords implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(RemoveCompleteReplicationRecords.class);

  private Instance inst;

  public RemoveCompleteReplicationRecords(Instance inst) {
    this.inst = inst;
  }

  @Override
  public void run() {
    Credentials creds = SystemCredentials.get();
    Connector conn;
    try {
      conn = inst.getConnector(creds.getPrincipal(), creds.getToken());
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("Could not create connector", e);
      throw new RuntimeException(e);
    }

    BatchScanner bs;
    BatchWriter bw;
    // Run over the metadata table first to reduce the likelihood of the master re-creating 
    // status records that we are in the middle of cleaning up (does not imply double-duplication though)
    for (String table : new String[] {MetadataTable.NAME, ReplicationTable.NAME}) {
      try {
        bs = conn.createBatchScanner(table, Authorizations.EMPTY, 4);
        bw = conn.createBatchWriter(table, new BatchWriterConfig());
      } catch (TableNotFoundException e) {
        log.debug("Not attempting to remove complete replication records as the {} doesn't yet exist", table);
        return;
      }

      // Set batchscanner options correctly for the table
      if (table.equals(MetadataTable.NAME)) {
        bs.setRanges(Collections.singleton(ReplicationSection.getRange()));
        bs.fetchColumnFamily(ReplicationSection.COLF);
      } else if (table.equals(ReplicationTable.NAME)) {
        bs.setRanges(Collections.singleton(new Range()));
      } else {
        throw new RuntimeException();
      }
  
      @SuppressWarnings("deprecation")
      Stopwatch sw = new Stopwatch();
      long recordsRemoved = 0;
      try {
        sw.start();
        recordsRemoved = removeCompleteRecords(conn, table, bs, bw);
      } finally {
        if (null != bs) {
          bs.close();
        }
        if (null != bw) {
          try {
            bw.close();
          } catch (MutationsRejectedException e) {
            log.error("Error writing mutations to {}, will retry", table, e);
          }
        }
  
        sw.stop();
      }
  
      log.info("Removed {} complete replication entries from the table {}", recordsRemoved, table);
    }
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
  protected long removeCompleteRecords(Connector conn, String table, BatchScanner bs, BatchWriter bw) {
    // TODO Make a Status-not-safe-for-removal filter and just use the BatchDeleter
    Text row = new Text(), colf = new Text(), colq = new Text();
    long recordsRemoved = 0;
    for (Entry<Key,Value> entry : bs) {
      Status status;
      try {
        status = Status.parseFrom(entry.getValue().get());
      } catch (InvalidProtocolBufferException e) {
        log.error("Encountered unparsable protobuf for key: {}", entry.getKey().toStringNoTruncate());
        continue;
      }

      if (StatusUtil.isSafeForRemoval(status)) {
        Key k = entry.getKey();
        k.getRow(row);
        k.getColumnFamily(colf);
        k.getColumnQualifier(colq);

        log.debug("Issuing deletion to {} for {} ", table, row.toString());

        Mutation mutation = new Mutation(row);
        mutation.putDelete(colf, colq);
        try {
          bw.addMutation(mutation);
          bw.flush();
        } catch (MutationsRejectedException e) {
          log.error("Error writing mutations to {}, will retry", table, e);
          continue;
        }

        recordsRemoved++;
      }
    }

    return recordsRemoved;
  }
}

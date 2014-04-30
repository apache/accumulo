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
    BatchWriter metaBw, replBw;
    try {
      bs = conn.createBatchScanner(ReplicationTable.NAME, Authorizations.EMPTY, 4);
      metaBw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
      replBw = conn.createBatchWriter(ReplicationTable.NAME, new BatchWriterConfig());
    } catch (TableNotFoundException e) {
      log.error("Replication table was deleted", e);
      return;
    }

    @SuppressWarnings("deprecation")
    Stopwatch sw = new Stopwatch();
    long recordsRemoved = 0;
    try {
      sw.start();
      recordsRemoved = removeCompleteRecords(conn, bs, metaBw, replBw);
    } finally {
      if (null != bs) {
        bs.close();
      }
      if (null != metaBw) {
        try {
          metaBw.close();
        } catch (MutationsRejectedException e) {
          log.error("Error writing mutations to metadata, will retry", e);
        }
      }
      if (null != replBw) {
        try {
          replBw.close();
        } catch (MutationsRejectedException e) {
          log.error("Error writing mutations to replication, will retry", e);
        }
      }

      sw.stop();
    }

    log.info("Removed {} replication entries from the replication table", recordsRemoved);
  }

  /**
   * Removes {@link Status} records read from the given {@code bs} and writes a delete, using the given {@code bw}
   * when that {@link Status} is fully replicated and closed, as defined by {@link StatusUtil#isSafeForRemoval(Status)}.
   * @param conn A Connector
   * @param bs A BatchScanner to read replication status records from
   * @param bw A BatchWriter to write deletes to the metadata table
   * @param bw A BatchWriter to write deletes to the replication table
   * @return Number of records removed
   */
  protected long removeCompleteRecords(Connector conn, BatchScanner bs, BatchWriter metaBw, BatchWriter replBw) {
    if (!ReplicationTable.exists(conn)) {
      // Nothing to do
      return 0;
    }

    bs.setRanges(Collections.singleton(new Range()));

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

        log.debug("Issuing delete for {}", k.toStringNoTruncate());

        Mutation metaMutation = new Mutation(ReplicationSection.getRowPrefix() + row.toString());
        metaMutation.putDelete(ReplicationSection.COLF, colq);
        try {
          metaBw.addMutation(metaMutation);
          metaBw.flush();
        } catch (MutationsRejectedException e) {
          log.error("Error writing mutations to metadata, will retry", e);
          continue;
        }

        // *only* write to the replication table after we successfully wrote to metadata
        Mutation replMutation = new Mutation(row);
        replMutation.putDelete(colf, colq);
        try {
          replBw.addMutation(replMutation);
          replBw.flush();
          recordsRemoved++;
        } catch (MutationsRejectedException e) {
          log.error("Error writing mutations to replication, will retry", e);
        }
      }
    }

    return recordsRemoved;
  }
}

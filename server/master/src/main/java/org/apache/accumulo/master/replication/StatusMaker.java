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

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.replication.ReplicationSchema;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Reads replication records from the metadata table and creates status records in the replication table
 */
public class StatusMaker {
  private static final Logger log = Logger.getLogger(StatusMaker.class);

  private final Connector conn;

  private BatchWriter writer;

  public StatusMaker(Connector conn) {
    this.conn = conn;
  }

  public void run() {
    ReplicationTable.create(conn);

    Span span = Trace.start("replicationStatusMaker");
    try {
      final Scanner s;
      try {
        s = conn.createScanner(MetadataTable.NAME, new Authorizations());
        if (null == writer) {
          setBatchWriter(ReplicationTable.getBatchWriter(conn));
        }
      } catch (TableNotFoundException e) {
        log.warn("Replication table did exist, but does not anymore");
        writer = null;
        return;
      }
  
      // Only pull records about data that has been ingested and is ready for replication
      s.fetchColumnFamily(ReplicationSection.COLF);
      s.setRange(ReplicationSection.getRange());
  
      Text row = new Text(), tableId = new Text();
      for (Entry<Key,Value> entry : s) {
        // Extract the useful bits from the status key
        ReplicationSchema.StatusSection.getFile(entry.getKey(), row);
        ReplicationSchema.StatusSection.getTableId(entry.getKey(), tableId);

        String rowStr = row.toString(); 
        rowStr = rowStr.substring(ReplicationSection.getRowPrefix().length());

        log.info("Processing replication status record for " + row + " on table "+ tableId);
  
        Span workSpan = Trace.start("createStatusMutations");
        try {
          addStatusRecord(rowStr, tableId, entry.getValue());
        } finally {
          workSpan.stop();
        }
      }
    } finally {
      span.stop();
    }
  }

  protected void setBatchWriter(BatchWriter bw) {
    this.writer = bw;
  }

  protected void addStatusRecord(String file, Text tableId, Value v) {
    // TODO come up with something that tries to avoid creating a new BatchWriter all the time
    try {
      Mutation m = new Mutation(file);
      m.put(StatusSection.NAME, tableId, v);

      try {
        writer.addMutation(m);
      } catch (MutationsRejectedException e) {
        log.warn("Failed to write work mutations for replication, will retry", e);
      }
    } finally {
      try {
        writer.flush();
      } catch (MutationsRejectedException e) {
        log.warn("Failed to write work mutations for replication, will retry", e);
      }
    }
  }
}

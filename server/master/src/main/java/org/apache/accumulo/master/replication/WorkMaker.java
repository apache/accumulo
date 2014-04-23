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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.replication.ReplicationSchema;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Reads replication records from the replication table and creates work records which include
 * target replication system information.
 */
public class WorkMaker extends Daemon {
  private static final Logger log = Logger.getLogger(WorkMaker.class);

  final Connector conn;
  final AccumuloConfiguration conf;

  private BatchWriter writer;

  public WorkMaker(Connector conn, AccumuloConfiguration conf) {
    super("Replication Table Work Maker");
    this.conn = conn;
    this.conf = conf;
  }

  @Override
  public void run() {
    while (true) {
      if (!conn.tableOperations().exists(ReplicationTable.NAME)) {
        log.trace("Replication table does not yet exist");
        UtilWaitThread.sleep(5000);
      }


      final Scanner s;
      try {
        s = ReplicationTable.getScanner(conn);
        if (null == writer) {
          writer = conn.createBatchWriter(ReplicationTable.NAME, new BatchWriterConfig());
        }
      } catch (TableNotFoundException e) {
        log.warn("Replication table was deleted");
        writer = null;
        continue;
      }

      // Only pull records about data that has been ingested and is ready for replication
      StatusSection.limit(s);

      TableConfiguration tableConf;

      Text file = new Text(), tableId = new Text();
      for (Entry<Key,Value> entry : s) {
        // Extract the useful bits from the ~repl keys
        ReplicationSchema.StatusSection.getFile(entry.getKey(), file);
        ReplicationSchema.StatusSection.getTableId(entry.getKey(), tableId); 

        // Get the table configuration for the ~repl record
        tableConf = ServerConfiguration.getTableConfiguration(conn.getInstance(), tableId.toString());

        // Pull the relevant replication targets
        // TODO Cache this instead of pulling it every time
        Map<String,String> replicationTargets = tableConf.getAllPropertiesWithPrefix(Property.TABLE_REPLICATION_TARGETS);

        // If we have targets, we need to make a work record
        if (!replicationTargets.isEmpty()) {
          try {
            addWorkRecord(file, entry.getValue(), replicationTargets);
          } catch (TableNotFoundException e) {
            log.warn("Replication table was deleted");
            writer = null;
            continue;
          }
        }
      }
    }
  }

  protected void addWorkRecord(Text file, Value v, Map<String,String> targets) throws TableNotFoundException {
    // TODO come up with something that tries to avoid creating a new BatchWriter all the time
    try {
      Mutation m = new Mutation(file);

      ReplicationTarget target = new ReplicationTarget();
      DataOutputBuffer buffer = new DataOutputBuffer();
      Text t = new Text();
      for (Entry<String,String> entry : targets.entrySet()) {
        buffer.reset();

        // Set up the writable
        target.setRemoteName(entry.getKey());
        target.setRemoteIdentifier(entry.getValue());
        target.write(buffer);

        // Throw it in a text for the mutation
        t.set(buffer.getData(), 0, buffer.getLength());

        // Add it to the work section
        WorkSection.add(m, t, v);
      }
      try {
        writer.addMutation(m);
      } catch (MutationsRejectedException e) {
        log.warn("Failed to write work mutations for replication, will retry", e);
      }
    } catch (IOException e) {
      log.warn("Failed to serialize data to Text, will retry", e);
    } finally {
      try {
        writer.flush();
      } catch (MutationsRejectedException e) {
        log.warn("Failed to write work mutations for replication, will retry", e);
      }
    }
  }
}

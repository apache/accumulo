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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.replication.ReplicationSchema;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTableOfflineException;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Reads replication records from the replication table and creates work records which include target replication system information.
 */
public class WorkMaker {
  private static final Logger log = LoggerFactory.getLogger(WorkMaker.class);

  private final AccumuloServerContext context;
  private Connector conn;

  private BatchWriter writer;

  public WorkMaker(AccumuloServerContext context, Connector conn) {
    this.context = context;
    this.conn = conn;
  }

  public void run() {
    if (!ReplicationTable.isOnline(conn)) {
      log.debug("Replication table is not yet online");
      return;
    }

    Span span = Trace.start("replicationWorkMaker");
    try {
      final Scanner s;
      try {
        s = ReplicationTable.getScanner(conn);
        if (null == writer) {
          setBatchWriter(ReplicationTable.getBatchWriter(conn));
        }
      } catch (ReplicationTableOfflineException e) {
        log.warn("Replication table was online, but not anymore");
        writer = null;
        return;
      }

      // Only pull records about data that has been ingested and is ready for replication
      StatusSection.limit(s);

      TableConfiguration tableConf;

      Text file = new Text();
      for (Entry<Key,Value> entry : s) {
        // Extract the useful bits from the status key
        ReplicationSchema.StatusSection.getFile(entry.getKey(), file);
        Table.ID tableId = ReplicationSchema.StatusSection.getTableId(entry.getKey());
        log.debug("Processing replication status record for {} on table {}", file, tableId);

        Status status;
        try {
          status = Status.parseFrom(entry.getValue().get());
        } catch (InvalidProtocolBufferException e) {
          log.error("Could not parse protobuf for {} from table {}", file, tableId);
          continue;
        }

        // Don't create the record if we have nothing to do.
        // TODO put this into a filter on serverside
        if (!shouldCreateWork(status)) {
          log.debug("Not creating work: {}", status.toString());
          continue;
        }

        // Get the table configuration for the table specified by the status record
        tableConf = context.getServerConfigurationFactory().getTableConfiguration(tableId);

        // getTableConfiguration(String) returns null if the table no longer exists
        if (null == tableConf) {
          continue;
        }

        // Pull the relevant replication targets
        // TODO Cache this instead of pulling it every time
        Map<String,String> replicationTargets = getReplicationTargets(tableConf);

        // If we have targets, we need to make a work record
        // TODO Don't replicate if it's a only a newFile entry (nothing to replicate yet)
        // -- Another scanner over the WorkSection can make this relatively cheap
        if (!replicationTargets.isEmpty()) {
          Span workSpan = Trace.start("createWorkMutations");
          try {
            addWorkRecord(file, entry.getValue(), replicationTargets, tableId);
          } finally {
            workSpan.stop();
          }
        } else {
          log.warn("No configured targets for table with ID {}", tableId);
        }
      }
    } finally {
      span.stop();
    }
  }

  protected void setBatchWriter(BatchWriter bw) {
    this.writer = bw;
  }

  protected Map<String,String> getReplicationTargets(TableConfiguration tableConf) {
    final Map<String,String> props = tableConf.getAllPropertiesWithPrefix(Property.TABLE_REPLICATION_TARGET);
    final Map<String,String> targets = new HashMap<>();
    final int propKeyLength = Property.TABLE_REPLICATION_TARGET.getKey().length();

    for (Entry<String,String> prop : props.entrySet()) {
      targets.put(prop.getKey().substring(propKeyLength), prop.getValue());
    }

    return targets;
  }

  /**
   * @return Should a Work entry be created for this status
   */
  protected boolean shouldCreateWork(Status status) {
    // Only creating work when there is work to do (regardless of closed status) is safe
    // as long as the ReplicaSystem implementation is correctly observing
    // that a file is completely replicated only when the file is closed
    return StatusUtil.isWorkRequired(status);
  }

  protected void addWorkRecord(Text file, Value v, Map<String,String> targets, Table.ID sourceTableId) {
    log.info("Adding work records for {} to targets {}", file, targets);
    try {
      Mutation m = new Mutation(file);

      ReplicationTarget target = new ReplicationTarget();
      DataOutputBuffer buffer = new DataOutputBuffer();
      Text t = new Text();
      for (Entry<String,String> entry : targets.entrySet()) {
        buffer.reset();

        // Set up the writable
        target.setPeerName(entry.getKey());
        target.setRemoteIdentifier(entry.getValue());
        target.setSourceTableId(sourceTableId);
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

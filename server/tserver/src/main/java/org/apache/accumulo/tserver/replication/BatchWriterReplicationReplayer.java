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
package org.apache.accumulo.tserver.replication;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.replication.AccumuloReplicationReplayer;
import org.apache.accumulo.core.replication.thrift.KeyValues;
import org.apache.accumulo.core.replication.thrift.RemoteReplicationErrorCode;
import org.apache.accumulo.core.replication.thrift.RemoteReplicationException;
import org.apache.accumulo.core.replication.thrift.WalEdits;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use a BatchWriter to replay WAL entries to an Accumulo table. This assumes that all WAL entries are for this table. Pruning out undesired entries is expected
 * to be done by the sender.
 */
public class BatchWriterReplicationReplayer implements AccumuloReplicationReplayer {
  private static final Logger log = LoggerFactory.getLogger(BatchWriterReplicationReplayer.class);

  @Override
  public long replicateLog(ClientContext context, String tableName, WalEdits data) throws RemoteReplicationException, AccumuloException,
      AccumuloSecurityException {
    final LogFileKey key = new LogFileKey();
    final LogFileValue value = new LogFileValue();
    final long memoryInBytes = context.getConfiguration().getAsBytes(Property.TSERV_REPLICATION_BW_REPLAYER_MEMORY);

    BatchWriter bw = null;
    long mutationsApplied = 0l;
    try {
      for (ByteBuffer edit : data.getEdits()) {
        DataInputStream dis = new DataInputStream(ByteBufferUtil.toByteArrayInputStream(edit));
        try {
          key.readFields(dis);
          // TODO this is brittle because AccumuloReplicaSystem isn't actually calling LogFileValue.write, but we're expecting
          // what we receive to be readable by the LogFileValue.
          value.readFields(dis);
        } catch (IOException e) {
          log.error("Could not deserialize edit from stream", e);
          throw new RemoteReplicationException(RemoteReplicationErrorCode.COULD_NOT_DESERIALIZE, "Could not deserialize edit from stream");
        }

        // Create the batchScanner if we don't already have one.
        if (null == bw) {
          BatchWriterConfig bwConfig = new BatchWriterConfig();
          bwConfig.setMaxMemory(memoryInBytes);
          try {
            bw = context.getConnector().createBatchWriter(tableName, bwConfig);
          } catch (TableNotFoundException e) {
            throw new RemoteReplicationException(RemoteReplicationErrorCode.TABLE_DOES_NOT_EXIST, "Table " + tableName + " does not exist");
          }
        }

        log.info("Applying {} mutations to table {} as part of batch", value.mutations.size(), tableName);

        // If we got a ServerMutation, we have to make sure that we preserve the systemTimestamp otherwise
        // the local system will assign a new timestamp.
        List<Mutation> mutationsCopy = new ArrayList<>(value.mutations.size());
        long mutationsCopied = 0l;
        for (Mutation orig : value.mutations) {
          if (orig instanceof ServerMutation) {
            mutationsCopied++;

            ServerMutation origServer = (ServerMutation) orig;
            Mutation copy = new Mutation(orig.getRow());
            for (ColumnUpdate update : orig.getUpdates()) {
              long timestamp;

              // If the update doesn't have a timestamp, pull it from the ServerMutation
              if (!update.hasTimestamp()) {
                timestamp = origServer.getSystemTimestamp();
              } else {
                timestamp = update.getTimestamp();
              }

              // TODO ACCUMULO-2937 cache the CVs
              if (update.isDeleted()) {
                copy.putDelete(update.getColumnFamily(), update.getColumnQualifier(), new ColumnVisibility(update.getColumnVisibility()), timestamp);
              } else {
                copy.put(update.getColumnFamily(), update.getColumnQualifier(), new ColumnVisibility(update.getColumnVisibility()), timestamp,
                    update.getValue());
              }
            }

            // We also need to preserve the replicationSource information to prevent cycles
            Set<String> replicationSources = orig.getReplicationSources();
            if (null != replicationSources && !replicationSources.isEmpty()) {
              for (String replicationSource : replicationSources) {
                copy.addReplicationSource(replicationSource);
              }
            }

            mutationsCopy.add(copy);
          } else {
            mutationsCopy.add(orig);
          }
        }

        log.debug("Copied {} mutations to ensure server-assigned timestamps are propagated", mutationsCopied);

        try {
          bw.addMutations(mutationsCopy);
        } catch (MutationsRejectedException e) {
          log.error("Could not apply mutations to {}", tableName);
          throw new RemoteReplicationException(RemoteReplicationErrorCode.COULD_NOT_APPLY, "Could not apply mutations to " + tableName);
        }

        log.debug("{} mutations added to the BatchScanner", mutationsCopy.size());

        mutationsApplied += mutationsCopy.size();
      }
    } finally {
      if (null != bw) {
        try {
          bw.close();
        } catch (MutationsRejectedException e) {
          log.error("Could not apply mutations to {}", tableName);
          throw new RemoteReplicationException(RemoteReplicationErrorCode.COULD_NOT_APPLY, "Could not apply mutations to " + tableName);
        }
      }
    }

    log.info("Applied {} mutations in total to {}", mutationsApplied, tableName);

    return mutationsApplied;
  }

  @Override
  public long replicateKeyValues(ClientContext context, String tableName, KeyValues kvs) {
    // TODO Implement me
    throw new UnsupportedOperationException();
  }

}

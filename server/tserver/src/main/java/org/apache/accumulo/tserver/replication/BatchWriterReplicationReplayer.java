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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.replication.AccumuloReplicationReplayer;
import org.apache.accumulo.core.replication.RemoteReplicationErrorCode;
import org.apache.accumulo.core.replication.thrift.KeyValues;
import org.apache.accumulo.core.replication.thrift.RemoteReplicationException;
import org.apache.accumulo.core.replication.thrift.WalEdits;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use a BatchWriter to replay WAL entries to an Accumulo table. This assumes that all
 * WAL entries are for this table. Pruning out undesired entries is expected to be done by the sender.
 */
public class BatchWriterReplicationReplayer implements AccumuloReplicationReplayer {
  private static final Logger log = LoggerFactory.getLogger(BatchWriterReplicationReplayer.class);

  @Override
  public long replicateLog(Connector conn, String tableName, WalEdits data) throws RemoteReplicationException {
    final LogFileKey key = new LogFileKey();
    final LogFileValue value = new LogFileValue();

    BatchWriter bw = null;
    long mutationsApplied = 0l;
    try {
      for (ByteBuffer edit : data.getEdits()) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(edit.array()));
        try {
          key.readFields(dis);
          value.readFields(dis);
        } catch (IOException e) {
          log.error("Could not deserialize edit from stream", e);
          throw new RemoteReplicationException(RemoteReplicationErrorCode.COULD_NOT_DESERIALIZE.ordinal(), "Could not deserialize edit from stream");
        }
    
        // Create the batchScanner if we don't already have one.
        if (null == bw) {
          try {
            bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
          } catch (TableNotFoundException e) {
            throw new RemoteReplicationException(RemoteReplicationErrorCode.TABLE_DOES_NOT_EXIST.ordinal(), "Table " + tableName + " does not exist");
          }
        }

        log.info("Applying {} updates to table {} as part of batch", value.mutations.size(), tableName);

        try {
          bw.addMutations(value.mutations);
        } catch (MutationsRejectedException e) {
          log.error("Could not apply mutations to {}", tableName);
          throw new RemoteReplicationException(RemoteReplicationErrorCode.COULD_NOT_APPLY.ordinal(), "Could not apply mutations to " + tableName);
        }

        mutationsApplied += value.mutations.size();
      }
    } finally {
      if (null != bw) {
        try {
          bw.close();
        } catch (MutationsRejectedException e) {
          log.error("Could not apply mutations to {}", tableName);
          throw new RemoteReplicationException(RemoteReplicationErrorCode.COULD_NOT_APPLY.ordinal(), "Could not apply mutations to " + tableName);
        }
      }
    }

    log.info("Applied {} mutations in total to {}", mutationsApplied, tableName);

    return mutationsApplied;
  }

  @Override
  public long replicateKeyValues(Connector conn, String tableName, KeyValues kvs) {
    // TODO Implement me
    throw new UnsupportedOperationException();
  }

}

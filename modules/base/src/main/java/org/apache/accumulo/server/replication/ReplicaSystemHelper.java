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
package org.apache.accumulo.server.replication;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ReplicaSystemHelper {
  private static final Logger log = LoggerFactory.getLogger(ReplicaSystemHelper.class);

  private ClientContext context;

  public ReplicaSystemHelper(ClientContext context) {
    requireNonNull(context);
    this.context = context;
  }

  /**
   * Record the updated Status for this file and target
   *
   * @param filePath
   *          Path to file being replicated
   * @param status
   *          Updated Status after replication
   * @param target
   *          Peer that was replicated to
   */
  public void recordNewStatus(Path filePath, Status status, ReplicationTarget target) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    BatchWriter bw = context.getConnector().createBatchWriter(ReplicationTable.NAME, new BatchWriterConfig());
    try {
      log.debug("Recording new status for {}, {}", filePath.toString(), ProtobufUtil.toString(status));
      Mutation m = new Mutation(filePath.toString());
      WorkSection.add(m, target.toText(), ProtobufUtil.toValue(status));
      bw.addMutation(m);
    } finally {
      bw.close();
    }
  }
}

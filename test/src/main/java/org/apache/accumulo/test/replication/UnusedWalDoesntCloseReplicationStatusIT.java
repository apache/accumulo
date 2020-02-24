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
package org.apache.accumulo.test.replication;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.DataOutputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.replication.ReplicaSystemFactory;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Iterables;

@Ignore("Replication ITs are not stable and not currently maintained")
public class UnusedWalDoesntCloseReplicationStatusIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    cfg.setNumTservers(1);
  }

  @Test
  public void test() throws Exception {
    File accumuloDir = this.getCluster().getConfig().getAccumuloDir();
    final AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build();
    final String tableName = getUniqueNames(1)[0];

    client.securityOperations().grantTablePermission("root", MetadataTable.NAME,
        TablePermission.WRITE);
    client.tableOperations().create(tableName);

    final TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
    final int numericTableId = Integer.parseInt(tableId.canonical());
    final int fakeTableId = numericTableId + 1;

    assertNotNull("Did not find table ID", tableId);

    client.tableOperations().setProperty(tableName, Property.TABLE_REPLICATION.getKey(), "true");
    client.tableOperations().setProperty(tableName,
        Property.TABLE_REPLICATION_TARGET.getKey() + "cluster1", "1");
    // just sleep
    client.instanceOperations().setProperty(Property.REPLICATION_PEERS.getKey() + "cluster1",
        ReplicaSystemFactory.getPeerConfigurationValue(MockReplicaSystem.class, "50000"));

    FileSystem fs = FileSystem.getLocal(new Configuration());
    File tserverWalDir =
        new File(accumuloDir, ServerConstants.WAL_DIR + Path.SEPARATOR + "faketserver+port");
    File tserverWal = new File(tserverWalDir, UUID.randomUUID().toString());
    fs.mkdirs(new Path(tserverWalDir.getAbsolutePath()));

    // Make a fake WAL with no data in it for our real table
    FSDataOutputStream out = fs.create(new Path(tserverWal.getAbsolutePath()));

    out.write(DfsLogger.LOG_FILE_HEADER_V4.getBytes(UTF_8));

    DataOutputStream dos = new DataOutputStream(out);
    dos.writeUTF("NullCryptoModule");

    // Fake a single update WAL that has a mutation for another table
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    key.event = OPEN;
    key.tserverSession = tserverWal.getAbsolutePath();
    key.filename = tserverWal.getAbsolutePath();
    key.write(out);
    value.write(out);

    key.event = LogEvents.DEFINE_TABLET;
    key.tablet = new KeyExtent(TableId.of(Integer.toString(fakeTableId)), null, null);
    key.seq = 1L;
    key.tabletId = 1;

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.filename = tserverWal.getAbsolutePath();
    value.mutations = Arrays.asList(new ServerMutation(new Text("row")));

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.COMPACTION_START;
    key.filename =
        accumuloDir.getAbsolutePath() + "/tables/" + fakeTableId + "/t-000001/A000001.rf";
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.COMPACTION_FINISH;
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    dos.close();

    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      Mutation m = new Mutation("m");
      m.put("m", "m", "M");
      bw.addMutation(m);
    }

    log.info("State of metadata table after inserting a record");

    try (Scanner s = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {

      s.setRange(MetadataSchema.TabletsSection.getRange(tableId));
      for (Entry<Key,Value> entry : s) {
        System.out.println(entry.getKey().toStringNoTruncate() + " " + entry.getValue());
      }
    }

    try (Scanner s = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.setRange(MetadataSchema.ReplicationSection.getRange());
      for (Entry<Key,Value> entry : s) {
        System.out.println(entry.getKey().toStringNoTruncate() + " "
            + ProtobufUtil.toString(Status.parseFrom(entry.getValue().get())));
      }

      log.info("Offline'ing table");

      client.tableOperations().offline(tableName, true);

      // Add our fake WAL to the log column for this table
      String walUri = tserverWal.toURI().toString();
      KeyExtent extent = new KeyExtent(tableId, null, null);
      try (BatchWriter bw = client.createBatchWriter(MetadataTable.NAME)) {
        Mutation m = new Mutation(extent.getMetadataEntry());
        m.put(MetadataSchema.TabletsSection.LogColumnFamily.NAME,
            new Text("localhost:12345/" + walUri), new Value(walUri + "|1"));
        bw.addMutation(m);

        // Add a replication entry for our fake WAL
        m = new Mutation(MetadataSchema.ReplicationSection.getRowPrefix() + new Path(walUri));
        m.put(MetadataSchema.ReplicationSection.COLF, new Text(tableId.canonical()),
            new Value(StatusUtil.fileCreated(System.currentTimeMillis()).toByteArray()));
        bw.addMutation(m);
      }

      log.info("State of metadata after injecting WAL manually");
    }

    try (Scanner s = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.setRange(MetadataSchema.TabletsSection.getRange(tableId));
      for (Entry<Key,Value> entry : s) {
        log.info("{} {}", entry.getKey().toStringNoTruncate(), entry.getValue());
      }
    }

    try (Scanner s = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.setRange(MetadataSchema.ReplicationSection.getRange());
      for (Entry<Key,Value> entry : s) {
        log.info("{} {}", entry.getKey().toStringNoTruncate(),
            ProtobufUtil.toString(Status.parseFrom(entry.getValue().get())));
      }

      log.info("Bringing table online");
      client.tableOperations().online(tableName, true);

      assertEquals(1, Iterables.size(client.createScanner(tableName, Authorizations.EMPTY)));

      log.info("Table has performed recovery, state of metadata:");
    }

    try (Scanner s = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.setRange(MetadataSchema.TabletsSection.getRange(tableId));
      for (Entry<Key,Value> entry : s) {
        log.info("{} {}", entry.getKey().toStringNoTruncate(), entry.getValue());
      }
    }

    try (Scanner s = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.setRange(MetadataSchema.ReplicationSection.getRange());
      for (Entry<Key,Value> entry : s) {
        Status status = Status.parseFrom(entry.getValue().get());
        log.info("{} {}", entry.getKey().toStringNoTruncate(), ProtobufUtil.toString(status));
        assertFalse("Status record was closed and it should not be", status.getClosed());
      }
    }
  }
}

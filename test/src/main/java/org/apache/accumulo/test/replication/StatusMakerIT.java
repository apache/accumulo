/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.manager.replication.StatusMaker;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

@Disabled("Replication ITs are not stable and not currently maintained")
@Deprecated
public class StatusMakerIT extends ConfigurableMacBase {

  private AccumuloClient client;
  private VolumeManager fs;

  @BeforeEach
  public void setupInstance() throws Exception {
    client = Accumulo.newClient().from(getClientProperties()).build();
    ReplicationTable.setOnline(client);
    client.securityOperations().grantTablePermission(client.whoami(), ReplicationTable.NAME,
        TablePermission.WRITE);
    client.securityOperations().grantTablePermission(client.whoami(), ReplicationTable.NAME,
        TablePermission.READ);
    fs = EasyMock.mock(VolumeManager.class);
  }

  @Test
  public void statusRecordsCreated() throws Exception {
    String sourceTable = testName();
    client.tableOperations().create(sourceTable);
    ReplicationTableUtil.configureMetadataTable(client, sourceTable);

    BatchWriter bw = client.createBatchWriter(sourceTable);
    String walPrefix = "hdfs://localhost:8020/accumulo/wals/tserver+port/";
    Set<String> files =
        Sets.newHashSet(walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(),
            walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID());
    Map<String,Integer> fileToTableId = new HashMap<>();

    int index = 1;
    long timeCreated = 0;
    Map<String,Long> fileToTimeCreated = new HashMap<>();
    for (String file : files) {
      Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
      m.put(ReplicationSection.COLF, new Text(Integer.toString(index)),
          StatusUtil.fileCreatedValue(timeCreated));
      fileToTimeCreated.put(file, timeCreated);
      bw.addMutation(m);
      fileToTableId.put(file, index);
      index++;
      timeCreated++;
    }

    bw.close();

    StatusMaker statusMaker = new StatusMaker(client, fs);
    statusMaker.setSourceTableName(sourceTable);

    statusMaker.run();

    try (Scanner s = ReplicationTable.getScanner(client)) {
      StatusSection.limit(s);
      Text file = new Text();
      for (Entry<Key,Value> entry : s) {
        StatusSection.getFile(entry.getKey(), file);
        TableId tableId = StatusSection.getTableId(entry.getKey());

        assertTrue(files.contains(file.toString()), "Found unexpected file: " + file);
        assertEquals(fileToTableId.get(file.toString()), Integer.valueOf(tableId.canonical()));
        timeCreated = fileToTimeCreated.get(file.toString());
        assertNotNull(timeCreated);
        assertEquals(StatusUtil.fileCreated(timeCreated), Status.parseFrom(entry.getValue().get()));
      }
    }
  }

  @Test
  public void openMessagesAreNotDeleted() throws Exception {
    String sourceTable = testName();
    client.tableOperations().create(sourceTable);
    ReplicationTableUtil.configureMetadataTable(client, sourceTable);

    BatchWriter bw = client.createBatchWriter(sourceTable);
    String walPrefix = "hdfs://localhost:8020/accumulo/wals/tserver+port/";
    Set<String> files =
        Sets.newHashSet(walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(),
            walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID());
    Map<String,Integer> fileToTableId = new HashMap<>();

    int index = 1;
    long timeCreated = 0;
    for (String file : files) {
      Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
      m.put(ReplicationSection.COLF, new Text(Integer.toString(index)),
          StatusUtil.fileCreatedValue(timeCreated));
      bw.addMutation(m);
      fileToTableId.put(file, index);
      index++;
      timeCreated++;
    }

    bw.close();

    StatusMaker statusMaker = new StatusMaker(client, fs);
    statusMaker.setSourceTableName(sourceTable);

    statusMaker.run();

    try (Scanner s = client.createScanner(sourceTable, Authorizations.EMPTY)) {
      s.setRange(ReplicationSection.getRange());
      s.fetchColumnFamily(ReplicationSection.COLF);
      assertEquals(files.size(), Iterables.size(s));
    }
  }

  @Test
  public void closedMessagesAreDeleted() throws Exception {
    String sourceTable = testName();
    client.tableOperations().create(sourceTable);
    ReplicationTableUtil.configureMetadataTable(client, sourceTable);

    BatchWriter bw = client.createBatchWriter(sourceTable);
    String walPrefix = "hdfs://localhost:8020/accumulo/wals/tserver+port/";
    Set<String> files =
        Sets.newHashSet(walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(),
            walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID());
    Map<String,Integer> fileToTableId = new HashMap<>();

    Status stat = Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(true)
        .setCreatedTime(System.currentTimeMillis()).build();

    int index = 1;
    for (String file : files) {
      Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
      m.put(ReplicationSection.COLF, new Text(Integer.toString(index)), ProtobufUtil.toValue(stat));
      bw.addMutation(m);
      fileToTableId.put(file, index);
      index++;
    }

    bw.close();

    StatusMaker statusMaker = new StatusMaker(client, fs);
    statusMaker.setSourceTableName(sourceTable);

    statusMaker.run();

    try (Scanner s = client.createScanner(sourceTable, Authorizations.EMPTY)) {
      s.setRange(ReplicationSection.getRange());
      s.fetchColumnFamily(ReplicationSection.COLF);
      for (Entry<Key,Value> e : s) {
        System.out.println(e.getKey().toStringNoTruncate() + " " + e.getValue());
      }
    }

    try (Scanner s = client.createScanner(sourceTable, Authorizations.EMPTY)) {
      s.setRange(ReplicationSection.getRange());
      s.fetchColumnFamily(ReplicationSection.COLF);
      assertEquals(0, Iterables.size(s));
    }
  }

  @Test
  public void closedMessagesCreateOrderRecords() throws Exception {
    String sourceTable = testName();
    client.tableOperations().create(sourceTable);
    ReplicationTableUtil.configureMetadataTable(client, sourceTable);

    BatchWriter bw = client.createBatchWriter(sourceTable);
    String walPrefix = "hdfs://localhost:8020/accumulo/wals/tserver+port/";
    List<String> files = Arrays.asList(walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(),
        walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID());
    Map<String,Integer> fileToTableId = new HashMap<>();

    Status.Builder statBuilder =
        Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(true);

    int index = 1;
    long time = System.currentTimeMillis();
    for (String file : files) {
      statBuilder.setCreatedTime(time++);
      Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
      m.put(ReplicationSection.COLF, new Text(Integer.toString(index)),
          ProtobufUtil.toValue(statBuilder.build()));
      bw.addMutation(m);
      fileToTableId.put(file, index);
      index++;
    }

    bw.close();

    StatusMaker statusMaker = new StatusMaker(client, fs);
    statusMaker.setSourceTableName(sourceTable);

    statusMaker.run();

    Iterator<Entry<Key,Value>> iter;
    Iterator<String> expectedFiles;
    try (Scanner s = client.createScanner(sourceTable, Authorizations.EMPTY)) {
      s.setRange(ReplicationSection.getRange());
      s.fetchColumnFamily(ReplicationSection.COLF);
      assertEquals(0, Iterables.size(s));
    }

    try (Scanner s = ReplicationTable.getScanner(client)) {
      OrderSection.limit(s);
      iter = s.iterator();
      assertTrue(iter.hasNext(), "Found no order records in replication table");

      expectedFiles = files.iterator();
      Text buff = new Text();
      while (expectedFiles.hasNext() && iter.hasNext()) {
        String file = expectedFiles.next();
        Entry<Key,Value> entry = iter.next();

        assertEquals(file, OrderSection.getFile(entry.getKey(), buff));
        OrderSection.getTableId(entry.getKey(), buff);
        assertEquals(fileToTableId.get(file).intValue(), Integer.parseInt(buff.toString()));
      }
    }
    assertFalse(expectedFiles.hasNext(), "Found more files unexpectedly");
    assertFalse(iter.hasNext(), "Found more entries in replication table unexpectedly");
  }

  @Test
  public void orderRecordsCreatedWithNoCreatedTime() throws Exception {
    String sourceTable = testName();
    client.tableOperations().create(sourceTable);
    ReplicationTableUtil.configureMetadataTable(client, sourceTable);

    BatchWriter bw = client.createBatchWriter(sourceTable);
    String walPrefix = "hdfs://localhost:8020/accumulo/wals/tserver+port/";
    List<String> files = Arrays.asList(walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(),
        walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID());
    Map<String,Long> fileToTableId = new HashMap<>();

    Status.Builder statBuilder =
        Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(true);

    Map<String,Long> statuses = new HashMap<>();
    long index = 1;
    for (String file : files) {
      Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
      m.put(ReplicationSection.COLF, new Text(Long.toString(index)),
          ProtobufUtil.toValue(statBuilder.build()));
      bw.addMutation(m);
      fileToTableId.put(file, index);

      FileStatus status = EasyMock.mock(FileStatus.class);
      EasyMock.expect(status.getModificationTime()).andReturn(index);
      EasyMock.replay(status);
      statuses.put(file, index);

      EasyMock.expect(fs.exists(new Path(file))).andReturn(true);
      EasyMock.expect(fs.getFileStatus(new Path(file))).andReturn(status);

      index++;
    }

    EasyMock.replay(fs);

    bw.close();

    StatusMaker statusMaker = new StatusMaker(client, fs);
    statusMaker.setSourceTableName(sourceTable);

    statusMaker.run();

    Scanner s = client.createScanner(sourceTable, Authorizations.EMPTY);
    s.setRange(ReplicationSection.getRange());
    s.fetchColumnFamily(ReplicationSection.COLF);
    assertEquals(0, Iterables.size(s));

    s = ReplicationTable.getScanner(client);
    OrderSection.limit(s);
    Iterator<Entry<Key,Value>> iter = s.iterator();
    assertTrue(iter.hasNext(), "Found no order records in replication table");

    Iterator<String> expectedFiles = files.iterator();
    Text buff = new Text();
    while (expectedFiles.hasNext() && iter.hasNext()) {
      String file = expectedFiles.next();
      Entry<Key,Value> entry = iter.next();

      assertEquals(file, OrderSection.getFile(entry.getKey(), buff));
      OrderSection.getTableId(entry.getKey(), buff);
      assertEquals(fileToTableId.get(file).intValue(), Integer.parseInt(buff.toString()));
      Status status = Status.parseFrom(entry.getValue().get());
      assertTrue(status.hasCreatedTime());
      assertEquals((long) statuses.get(file), status.getCreatedTime());
    }

    assertFalse(expectedFiles.hasNext(), "Found more files unexpectedly");
    assertFalse(iter.hasNext(), "Found more entries in replication table unexpectedly");

    s = client.createScanner(sourceTable, Authorizations.EMPTY);
    s.setRange(ReplicationSection.getRange());
    s.fetchColumnFamily(ReplicationSection.COLF);
    assertEquals(0, Iterables.size(s));

    s = ReplicationTable.getScanner(client);
    s.setRange(ReplicationSection.getRange());
    iter = s.iterator();
    assertTrue(iter.hasNext(), "Found no stat records in replication table");

    Collections.sort(files);
    expectedFiles = files.iterator();
    while (expectedFiles.hasNext() && iter.hasNext()) {
      String file = expectedFiles.next();
      Entry<Key,Value> entry = iter.next();
      Status status = Status.parseFrom(entry.getValue().get());
      assertTrue(status.hasCreatedTime());
      assertEquals((long) statuses.get(file), status.getCreatedTime());
    }

    assertFalse(expectedFiles.hasNext(), "Found more files unexpectedly");
    assertFalse(iter.hasNext(), "Found more entries in replication table unexpectedly");
  }
}

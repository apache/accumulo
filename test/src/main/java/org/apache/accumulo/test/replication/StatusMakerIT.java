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
package org.apache.accumulo.test.replication;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.master.replication.StatusMaker;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class StatusMakerIT extends ConfigurableMacBase {

  private Connector conn;
  private VolumeManager fs;

  @Before
  public void setupInstance() throws Exception {
    conn = getConnector();
    ReplicationTable.setOnline(conn);
    conn.securityOperations().grantTablePermission(conn.whoami(), ReplicationTable.NAME, TablePermission.WRITE);
    conn.securityOperations().grantTablePermission(conn.whoami(), ReplicationTable.NAME, TablePermission.READ);
    fs = EasyMock.mock(VolumeManager.class);
  }

  @Test
  public void statusRecordsCreated() throws Exception {
    String sourceTable = testName.getMethodName();
    conn.tableOperations().create(sourceTable);
    ReplicationTableUtil.configureMetadataTable(conn, sourceTable);

    BatchWriter bw = conn.createBatchWriter(sourceTable, new BatchWriterConfig());
    String walPrefix = "hdfs://localhost:8020/accumulo/wals/tserver+port/";
    Set<String> files = Sets.newHashSet(walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(),
        walPrefix + UUID.randomUUID());
    Map<String,Integer> fileToTableId = new HashMap<>();

    int index = 1;
    long timeCreated = 0;
    Map<String,Long> fileToTimeCreated = new HashMap<>();
    for (String file : files) {
      Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
      m.put(ReplicationSection.COLF, new Text(Integer.toString(index)), StatusUtil.fileCreatedValue(timeCreated));
      fileToTimeCreated.put(file, timeCreated);
      bw.addMutation(m);
      fileToTableId.put(file, index);
      index++;
      timeCreated++;
    }

    bw.close();

    StatusMaker statusMaker = new StatusMaker(conn, fs);
    statusMaker.setSourceTableName(sourceTable);

    statusMaker.run();

    try (Scanner s = ReplicationTable.getScanner(conn)) {
      StatusSection.limit(s);
      Text file = new Text();
      for (Entry<Key,Value> entry : s) {
        StatusSection.getFile(entry.getKey(), file);
        Table.ID tableId = StatusSection.getTableId(entry.getKey());

        Assert.assertTrue("Found unexpected file: " + file, files.contains(file.toString()));
        Assert.assertEquals(fileToTableId.get(file.toString()), new Integer(tableId.canonicalID()));
        timeCreated = fileToTimeCreated.get(file.toString());
        Assert.assertNotNull(timeCreated);
        Assert.assertEquals(StatusUtil.fileCreated(timeCreated), Status.parseFrom(entry.getValue().get()));
      }
    }
  }

  @Test
  public void openMessagesAreNotDeleted() throws Exception {
    String sourceTable = testName.getMethodName();
    conn.tableOperations().create(sourceTable);
    ReplicationTableUtil.configureMetadataTable(conn, sourceTable);

    BatchWriter bw = conn.createBatchWriter(sourceTable, new BatchWriterConfig());
    String walPrefix = "hdfs://localhost:8020/accumulo/wals/tserver+port/";
    Set<String> files = Sets.newHashSet(walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(),
        walPrefix + UUID.randomUUID());
    Map<String,Integer> fileToTableId = new HashMap<>();

    int index = 1;
    long timeCreated = 0;
    for (String file : files) {
      Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
      m.put(ReplicationSection.COLF, new Text(Integer.toString(index)), StatusUtil.fileCreatedValue(timeCreated));
      bw.addMutation(m);
      fileToTableId.put(file, index);
      index++;
      timeCreated++;
    }

    bw.close();

    StatusMaker statusMaker = new StatusMaker(conn, fs);
    statusMaker.setSourceTableName(sourceTable);

    statusMaker.run();

    try (Scanner s = conn.createScanner(sourceTable, Authorizations.EMPTY)) {
      s.setRange(ReplicationSection.getRange());
      s.fetchColumnFamily(ReplicationSection.COLF);
      Assert.assertEquals(files.size(), Iterables.size(s));
    }
  }

  @Test
  public void closedMessagesAreDeleted() throws Exception {
    String sourceTable = testName.getMethodName();
    conn.tableOperations().create(sourceTable);
    ReplicationTableUtil.configureMetadataTable(conn, sourceTable);

    BatchWriter bw = conn.createBatchWriter(sourceTable, new BatchWriterConfig());
    String walPrefix = "hdfs://localhost:8020/accumulo/wals/tserver+port/";
    Set<String> files = Sets.newHashSet(walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(),
        walPrefix + UUID.randomUUID());
    Map<String,Integer> fileToTableId = new HashMap<>();

    Status stat = Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(true).setCreatedTime(System.currentTimeMillis()).build();

    int index = 1;
    for (String file : files) {
      Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
      m.put(ReplicationSection.COLF, new Text(Integer.toString(index)), ProtobufUtil.toValue(stat));
      bw.addMutation(m);
      fileToTableId.put(file, index);
      index++;
    }

    bw.close();

    StatusMaker statusMaker = new StatusMaker(conn, fs);
    statusMaker.setSourceTableName(sourceTable);

    statusMaker.run();

    try (Scanner s = conn.createScanner(sourceTable, Authorizations.EMPTY)) {
      s.setRange(ReplicationSection.getRange());
      s.fetchColumnFamily(ReplicationSection.COLF);
      for (Entry<Key,Value> e : s) {
        System.out.println(e.getKey().toStringNoTruncate() + " " + e.getValue());
      }
    }

    try (Scanner s = conn.createScanner(sourceTable, Authorizations.EMPTY)) {
      s.setRange(ReplicationSection.getRange());
      s.fetchColumnFamily(ReplicationSection.COLF);
      Assert.assertEquals(0, Iterables.size(s));
    }
  }

  @Test
  public void closedMessagesCreateOrderRecords() throws Exception {
    String sourceTable = testName.getMethodName();
    conn.tableOperations().create(sourceTable);
    ReplicationTableUtil.configureMetadataTable(conn, sourceTable);

    BatchWriter bw = conn.createBatchWriter(sourceTable, new BatchWriterConfig());
    String walPrefix = "hdfs://localhost:8020/accumulo/wals/tserver+port/";
    List<String> files = Arrays.asList(walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(),
        walPrefix + UUID.randomUUID());
    Map<String,Integer> fileToTableId = new HashMap<>();

    Status.Builder statBuilder = Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(true);

    int index = 1;
    long time = System.currentTimeMillis();
    for (String file : files) {
      statBuilder.setCreatedTime(time++);
      Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
      m.put(ReplicationSection.COLF, new Text(Integer.toString(index)), ProtobufUtil.toValue(statBuilder.build()));
      bw.addMutation(m);
      fileToTableId.put(file, index);
      index++;
    }

    bw.close();

    StatusMaker statusMaker = new StatusMaker(conn, fs);
    statusMaker.setSourceTableName(sourceTable);

    statusMaker.run();

    Iterator<Entry<Key,Value>> iter;
    Iterator<String> expectedFiles;
    try (Scanner s = conn.createScanner(sourceTable, Authorizations.EMPTY)) {
      s.setRange(ReplicationSection.getRange());
      s.fetchColumnFamily(ReplicationSection.COLF);
      Assert.assertEquals(0, Iterables.size(s));
    }

    try (Scanner s = ReplicationTable.getScanner(conn)) {
      OrderSection.limit(s);
      iter = s.iterator();
      Assert.assertTrue("Found no order records in replication table", iter.hasNext());

      expectedFiles = files.iterator();
      Text buff = new Text();
      while (expectedFiles.hasNext() && iter.hasNext()) {
        String file = expectedFiles.next();
        Entry<Key,Value> entry = iter.next();

        Assert.assertEquals(file, OrderSection.getFile(entry.getKey(), buff));
        OrderSection.getTableId(entry.getKey(), buff);
        Assert.assertEquals(fileToTableId.get(file).intValue(), Integer.parseInt(buff.toString()));
      }
    }
    Assert.assertFalse("Found more files unexpectedly", expectedFiles.hasNext());
    Assert.assertFalse("Found more entries in replication table unexpectedly", iter.hasNext());
  }

  @Test
  public void orderRecordsCreatedWithNoCreatedTime() throws Exception {
    String sourceTable = testName.getMethodName();
    conn.tableOperations().create(sourceTable);
    ReplicationTableUtil.configureMetadataTable(conn, sourceTable);

    BatchWriter bw = conn.createBatchWriter(sourceTable, new BatchWriterConfig());
    String walPrefix = "hdfs://localhost:8020/accumulo/wals/tserver+port/";
    List<String> files = Arrays.asList(walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(),
        walPrefix + UUID.randomUUID());
    Map<String,Long> fileToTableId = new HashMap<>();

    Status.Builder statBuilder = Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(true);

    Map<String,Long> statuses = new HashMap<>();
    long index = 1;
    for (String file : files) {
      Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
      m.put(ReplicationSection.COLF, new Text(Long.toString(index)), ProtobufUtil.toValue(statBuilder.build()));
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

    StatusMaker statusMaker = new StatusMaker(conn, fs);
    statusMaker.setSourceTableName(sourceTable);

    statusMaker.run();

    Scanner s = conn.createScanner(sourceTable, Authorizations.EMPTY);
    s.setRange(ReplicationSection.getRange());
    s.fetchColumnFamily(ReplicationSection.COLF);
    Assert.assertEquals(0, Iterables.size(s));

    s = ReplicationTable.getScanner(conn);
    OrderSection.limit(s);
    Iterator<Entry<Key,Value>> iter = s.iterator();
    Assert.assertTrue("Found no order records in replication table", iter.hasNext());

    Iterator<String> expectedFiles = files.iterator();
    Text buff = new Text();
    while (expectedFiles.hasNext() && iter.hasNext()) {
      String file = expectedFiles.next();
      Entry<Key,Value> entry = iter.next();

      Assert.assertEquals(file, OrderSection.getFile(entry.getKey(), buff));
      OrderSection.getTableId(entry.getKey(), buff);
      Assert.assertEquals(fileToTableId.get(file).intValue(), Integer.parseInt(buff.toString()));
      Status status = Status.parseFrom(entry.getValue().get());
      Assert.assertTrue(status.hasCreatedTime());
      Assert.assertEquals((long) statuses.get(file), status.getCreatedTime());
    }

    Assert.assertFalse("Found more files unexpectedly", expectedFiles.hasNext());
    Assert.assertFalse("Found more entries in replication table unexpectedly", iter.hasNext());

    s = conn.createScanner(sourceTable, Authorizations.EMPTY);
    s.setRange(ReplicationSection.getRange());
    s.fetchColumnFamily(ReplicationSection.COLF);
    Assert.assertEquals(0, Iterables.size(s));

    s = ReplicationTable.getScanner(conn);
    s.setRange(ReplicationSection.getRange());
    iter = s.iterator();
    Assert.assertTrue("Found no stat records in replication table", iter.hasNext());

    Collections.sort(files);
    expectedFiles = files.iterator();
    while (expectedFiles.hasNext() && iter.hasNext()) {
      String file = expectedFiles.next();
      Entry<Key,Value> entry = iter.next();
      Status status = Status.parseFrom(entry.getValue().get());
      Assert.assertTrue(status.hasCreatedTime());
      Assert.assertEquals((long) statuses.get(file), status.getCreatedTime());
    }

    Assert.assertFalse("Found more files unexpectedly", expectedFiles.hasNext());
    Assert.assertFalse("Found more entries in replication table unexpectedly", iter.hasNext());
  }
}

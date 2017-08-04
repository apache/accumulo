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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.TabletIterator;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class CloneIT extends AccumuloClusterHarness {

  @Test
  public void testNoFiles() throws Exception {
    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    KeyExtent ke = new KeyExtent(Table.ID.of("0"), null, null);
    Mutation mut = ke.getPrevRowUpdateMutation();

    TabletsSection.ServerColumnFamily.TIME_COLUMN.put(mut, new Value("M0".getBytes()));
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut, new Value("/default_tablet".getBytes()));

    BatchWriter bw1 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    bw1.addMutation(mut);

    bw1.close();

    BatchWriter bw2 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    MetadataTableUtil.initializeClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    int rc = MetadataTableUtil.checkClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    assertEquals(0, rc);

    // scan tables metadata entries and confirm the same

  }

  @Test
  public void testFilesChange() throws Exception {
    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    KeyExtent ke = new KeyExtent(Table.ID.of("0"), null, null);
    Mutation mut = ke.getPrevRowUpdateMutation();

    TabletsSection.ServerColumnFamily.TIME_COLUMN.put(mut, new Value("M0".getBytes()));
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut, new Value("/default_tablet".getBytes()));
    mut.put(DataFileColumnFamily.NAME.toString(), "/default_tablet/0_0.rf", new DataFileValue(1, 200).encodeAsString());

    BatchWriter bw1 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    bw1.addMutation(mut);

    bw1.flush();

    BatchWriter bw2 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    MetadataTableUtil.initializeClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    Mutation mut2 = new Mutation(ke.getMetadataEntry());
    mut2.putDelete(DataFileColumnFamily.NAME.toString(), "/default_tablet/0_0.rf");
    mut2.put(DataFileColumnFamily.NAME.toString(), "/default_tablet/1_0.rf", new DataFileValue(2, 300).encodeAsString());

    bw1.addMutation(mut2);
    bw1.flush();

    int rc = MetadataTableUtil.checkClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    assertEquals(1, rc);

    rc = MetadataTableUtil.checkClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    assertEquals(0, rc);

    Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
    scanner.setRange(new KeyExtent(Table.ID.of("1"), null, null).toMetadataRange());

    HashSet<String> files = new HashSet<>();

    for (Entry<Key,Value> entry : scanner) {
      if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME))
        files.add(entry.getKey().getColumnQualifier().toString());
    }

    assertEquals(1, files.size());
    assertTrue(files.contains("../0/default_tablet/1_0.rf"));

  }

  // test split where files of children are the same
  @Test
  public void testSplit1() throws Exception {
    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    BatchWriter bw1 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    bw1.addMutation(createTablet("0", null, null, "/default_tablet", "/default_tablet/0_0.rf"));

    bw1.flush();

    BatchWriter bw2 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    MetadataTableUtil.initializeClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    bw1.addMutation(createTablet("0", "m", null, "/default_tablet", "/default_tablet/0_0.rf"));
    bw1.addMutation(createTablet("0", null, "m", "/t-1", "/default_tablet/0_0.rf"));

    bw1.flush();

    int rc = MetadataTableUtil.checkClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    assertEquals(0, rc);

    Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
    scanner.setRange(new KeyExtent(Table.ID.of("1"), null, null).toMetadataRange());

    HashSet<String> files = new HashSet<>();

    int count = 0;
    for (Entry<Key,Value> entry : scanner) {
      if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
        files.add(entry.getKey().getColumnQualifier().toString());
        count++;
      }
    }

    assertEquals(1, count);
    assertEquals(1, files.size());
    assertTrue(files.contains("../0/default_tablet/0_0.rf"));
  }

  // test split where files of children differ... like majc and split occurred
  @Test
  public void testSplit2() throws Exception {
    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    BatchWriter bw1 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    bw1.addMutation(createTablet("0", null, null, "/default_tablet", "/default_tablet/0_0.rf"));

    bw1.flush();

    BatchWriter bw2 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    MetadataTableUtil.initializeClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    bw1.addMutation(createTablet("0", "m", null, "/default_tablet", "/default_tablet/1_0.rf"));
    Mutation mut3 = createTablet("0", null, "m", "/t-1", "/default_tablet/1_0.rf");
    mut3.putDelete(DataFileColumnFamily.NAME.toString(), "/default_tablet/0_0.rf");
    bw1.addMutation(mut3);

    bw1.flush();

    int rc = MetadataTableUtil.checkClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    assertEquals(1, rc);

    rc = MetadataTableUtil.checkClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    assertEquals(0, rc);

    Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
    scanner.setRange(new KeyExtent(Table.ID.of("1"), null, null).toMetadataRange());

    HashSet<String> files = new HashSet<>();

    int count = 0;

    for (Entry<Key,Value> entry : scanner) {
      if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
        files.add(entry.getKey().getColumnQualifier().toString());
        count++;
      }
    }

    assertEquals(1, files.size());
    assertEquals(2, count);
    assertTrue(files.contains("../0/default_tablet/1_0.rf"));
  }

  private static Mutation deleteTablet(String tid, String endRow, String prevRow, String dir, String file) throws Exception {
    KeyExtent ke = new KeyExtent(Table.ID.of(tid), endRow == null ? null : new Text(endRow), prevRow == null ? null : new Text(prevRow));
    Mutation mut = new Mutation(ke.getMetadataEntry());
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.putDelete(mut);
    TabletsSection.ServerColumnFamily.TIME_COLUMN.putDelete(mut);
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.putDelete(mut);
    mut.putDelete(DataFileColumnFamily.NAME.toString(), file);

    return mut;
  }

  private static Mutation createTablet(String tid, String endRow, String prevRow, String dir, String file) throws Exception {
    KeyExtent ke = new KeyExtent(Table.ID.of(tid), endRow == null ? null : new Text(endRow), prevRow == null ? null : new Text(prevRow));
    Mutation mut = ke.getPrevRowUpdateMutation();

    TabletsSection.ServerColumnFamily.TIME_COLUMN.put(mut, new Value("M0".getBytes()));
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut, new Value(dir.getBytes()));
    mut.put(DataFileColumnFamily.NAME.toString(), file, new DataFileValue(10, 200).encodeAsString());

    return mut;
  }

  // test two tablets splitting into four
  @Test
  public void testSplit3() throws Exception {
    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    BatchWriter bw1 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    bw1.addMutation(createTablet("0", "m", null, "/d1", "/d1/file1"));
    bw1.addMutation(createTablet("0", null, "m", "/d2", "/d2/file2"));

    bw1.flush();

    BatchWriter bw2 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    MetadataTableUtil.initializeClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    bw1.addMutation(createTablet("0", "f", null, "/d1", "/d1/file3"));
    bw1.addMutation(createTablet("0", "m", "f", "/d3", "/d1/file1"));
    bw1.addMutation(createTablet("0", "s", "m", "/d2", "/d2/file2"));
    bw1.addMutation(createTablet("0", null, "s", "/d4", "/d2/file2"));

    bw1.flush();

    int rc = MetadataTableUtil.checkClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    assertEquals(0, rc);

    Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
    scanner.setRange(new KeyExtent(Table.ID.of("1"), null, null).toMetadataRange());

    HashSet<String> files = new HashSet<>();

    int count = 0;
    for (Entry<Key,Value> entry : scanner) {
      if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
        files.add(entry.getKey().getColumnQualifier().toString());
        count++;
      }
    }

    assertEquals(2, count);
    assertEquals(2, files.size());
    assertTrue(files.contains("../0/d1/file1"));
    assertTrue(files.contains("../0/d2/file2"));
  }

  // test cloned marker
  @Test
  public void testClonedMarker() throws Exception {
    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    BatchWriter bw1 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    bw1.addMutation(createTablet("0", "m", null, "/d1", "/d1/file1"));
    bw1.addMutation(createTablet("0", null, "m", "/d2", "/d2/file2"));

    bw1.flush();

    BatchWriter bw2 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    MetadataTableUtil.initializeClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    bw1.addMutation(deleteTablet("0", "m", null, "/d1", "/d1/file1"));
    bw1.addMutation(deleteTablet("0", null, "m", "/d2", "/d2/file2"));

    bw1.flush();

    bw1.addMutation(createTablet("0", "f", null, "/d1", "/d1/file3"));
    bw1.addMutation(createTablet("0", "m", "f", "/d3", "/d1/file1"));
    bw1.addMutation(createTablet("0", "s", "m", "/d2", "/d2/file3"));
    bw1.addMutation(createTablet("0", null, "s", "/d4", "/d4/file3"));

    bw1.flush();

    int rc = MetadataTableUtil.checkClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    assertEquals(1, rc);

    bw1.addMutation(deleteTablet("0", "m", "f", "/d3", "/d1/file1"));

    bw1.flush();

    bw1.addMutation(createTablet("0", "m", "f", "/d3", "/d1/file3"));

    bw1.flush();

    rc = MetadataTableUtil.checkClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    assertEquals(0, rc);

    Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
    scanner.setRange(new KeyExtent(Table.ID.of("1"), null, null).toMetadataRange());

    HashSet<String> files = new HashSet<>();

    int count = 0;
    for (Entry<Key,Value> entry : scanner) {
      if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
        files.add(entry.getKey().getColumnQualifier().toString());
        count++;
      }
    }

    assertEquals(3, count);
    assertEquals(3, files.size());
    assertTrue(files.contains("../0/d1/file1"));
    assertTrue(files.contains("../0/d2/file3"));
    assertTrue(files.contains("../0/d4/file3"));
  }

  // test two tablets splitting into four
  @Test
  public void testMerge() throws Exception {
    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    BatchWriter bw1 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    bw1.addMutation(createTablet("0", "m", null, "/d1", "/d1/file1"));
    bw1.addMutation(createTablet("0", null, "m", "/d2", "/d2/file2"));

    bw1.flush();

    BatchWriter bw2 = conn.createBatchWriter(tableName, new BatchWriterConfig());

    MetadataTableUtil.initializeClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);

    bw1.addMutation(deleteTablet("0", "m", null, "/d1", "/d1/file1"));
    Mutation mut = createTablet("0", null, null, "/d2", "/d2/file2");
    mut.put(DataFileColumnFamily.NAME.toString(), "/d1/file1", new DataFileValue(10, 200).encodeAsString());
    bw1.addMutation(mut);

    bw1.flush();

    try {
      MetadataTableUtil.checkClone(tableName, Table.ID.of("0"), Table.ID.of("1"), conn, bw2);
      assertTrue(false);
    } catch (TabletIterator.TabletDeletedException tde) {}

  }

}

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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletDeletedException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class CloneIT extends AccumuloClusterHarness {

  @Test
  public void testNoFiles() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      KeyExtent ke = new KeyExtent(TableId.of("0"), null, null);
      Mutation mut = TabletColumnFamily.createPrevRowMutation(ke);

      ServerColumnFamily.TIME_COLUMN.put(mut, new Value("M0"));
      ServerColumnFamily.DIRECTORY_COLUMN.put(mut, new Value("/default_tablet"));

      try (BatchWriter bw1 = client.createBatchWriter(tableName)) {
        bw1.addMutation(mut);
      }

      try (BatchWriter bw2 = client.createBatchWriter(tableName)) {
        MetadataTableUtil.initializeClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);
        int rc =
            MetadataTableUtil.checkClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);
        assertEquals(0, rc);
      }

      // scan tables metadata entries and confirm the same
    }
  }

  @ParameterizedTest
  @ArgumentsSource(RangeArgumentsProvider.class)
  public void testFilesChange(Range range1, Range range2) throws Exception {
    String filePrefix = "hdfs://nn:8000/accumulo/tables/0";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      KeyExtent ke = new KeyExtent(TableId.of("0"), null, null);
      Mutation mut = TabletColumnFamily.createPrevRowMutation(ke);

      ServerColumnFamily.TIME_COLUMN.put(mut, new Value("M0"));
      ServerColumnFamily.DIRECTORY_COLUMN.put(mut, new Value("/default_tablet"));
      mut.put(DataFileColumnFamily.NAME.toString(),
          getMetadata(filePrefix + "/default_tablet/0_0.rf", range1),
          new DataFileValue(1, 200).encodeAsString());

      try (BatchWriter bw1 = client.createBatchWriter(tableName);
          BatchWriter bw2 = client.createBatchWriter(tableName)) {
        bw1.addMutation(mut);

        bw1.flush();

        MetadataTableUtil.initializeClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        Mutation mut2 = new Mutation(ke.toMetaRow());
        mut2.putDelete(DataFileColumnFamily.NAME.toString(),
            getMetadata(filePrefix + "/default_tablet/0_0.rf", range1));
        mut2.put(DataFileColumnFamily.NAME.toString(),
            getMetadata(filePrefix + "/default_tablet/1_0.rf", range2),
            new DataFileValue(2, 300).encodeAsString());

        bw1.addMutation(mut2);
        bw1.flush();

        int rc =
            MetadataTableUtil.checkClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        assertEquals(1, rc);

        rc = MetadataTableUtil.checkClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        assertEquals(0, rc);
      }

      HashSet<String> files = new HashSet<>();

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new KeyExtent(TableId.of("1"), null, null).toMetaRange());
        for (Entry<Key,Value> entry : scanner) {
          if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
            files.add(entry.getKey().getColumnQualifier().toString());
          }
        }
      }
      assertEquals(1, files.size());
      assertTrue(files.contains(getMetadata(filePrefix + "/default_tablet/1_0.rf", range2)));
    }
  }

  // test split where files of children are the same
  @ParameterizedTest
  @ArgumentsSource(RangeArgumentsProvider.class)
  public void testSplit1(Range range) throws Exception {
    String filePrefix = "hdfs://nn:8000/accumulo/tables/0";

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      try (BatchWriter bw1 = client.createBatchWriter(tableName);
          BatchWriter bw2 = client.createBatchWriter(tableName)) {
        bw1.addMutation(createTablet("0", null, null, "/default_tablet",
            filePrefix + "/default_tablet/0_0.rf", range));

        bw1.flush();

        MetadataTableUtil.initializeClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        bw1.addMutation(createTablet("0", "m", null, "/default_tablet",
            filePrefix + "/default_tablet/0_0.rf", range));
        bw1.addMutation(
            createTablet("0", null, "m", "/t-1", filePrefix + "/default_tablet/0_0.rf", range));

        bw1.flush();

        int rc =
            MetadataTableUtil.checkClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        assertEquals(0, rc);
      }
      HashSet<String> files = new HashSet<>();
      int count = 0;

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new KeyExtent(TableId.of("1"), null, null).toMetaRange());
        for (Entry<Key,Value> entry : scanner) {
          if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
            files.add(entry.getKey().getColumnQualifier().toString());
            count++;
          }
        }
      }
      assertEquals(1, count);
      assertEquals(1, files.size());
      assertTrue(files.contains(getMetadata(filePrefix + "/default_tablet/0_0.rf", range)));
    }
  }

  // test split where files of children differ... like majc and split occurred
  @ParameterizedTest
  @ArgumentsSource(RangeArgumentsProvider.class)
  public void testSplit2(Range range) throws Exception {
    String filePrefix = "hdfs://nn:8000/accumulo/tables/0";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      try (BatchWriter bw1 = client.createBatchWriter(tableName);
          BatchWriter bw2 = client.createBatchWriter(tableName)) {
        bw1.addMutation(createTablet("0", null, null, "/default_tablet",
            filePrefix + "/default_tablet/0_0.rf", range));

        bw1.flush();

        MetadataTableUtil.initializeClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        bw1.addMutation(createTablet("0", "m", null, "/default_tablet",
            filePrefix + "/default_tablet/1_0.rf", range));
        Mutation mut3 =
            createTablet("0", null, "m", "/t-1", filePrefix + "/default_tablet/1_0.rf", range);
        mut3.putDelete(DataFileColumnFamily.NAME.toString(),
            getMetadata(filePrefix + "/default_tablet/0_0.rf", range));
        bw1.addMutation(mut3);

        bw1.flush();

        int rc =
            MetadataTableUtil.checkClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        assertEquals(1, rc);

        rc = MetadataTableUtil.checkClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        assertEquals(0, rc);
      }
      HashSet<String> files = new HashSet<>();
      int count = 0;

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new KeyExtent(TableId.of("1"), null, null).toMetaRange());
        for (Entry<Key,Value> entry : scanner) {
          if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
            files.add(entry.getKey().getColumnQualifier().toString());
            count++;
          }
        }
      }
      assertEquals(1, files.size());
      assertEquals(2, count);
      assertTrue(files.contains(getMetadata(filePrefix + "/default_tablet/1_0.rf", range)));
    }
  }

  private static Mutation deleteTablet(String tid, String endRow, String prevRow, String file,
      Range range) {
    KeyExtent ke = new KeyExtent(TableId.of(tid), endRow == null ? null : new Text(endRow),
        prevRow == null ? null : new Text(prevRow));
    Mutation mut = new Mutation(ke.toMetaRow());
    TabletColumnFamily.PREV_ROW_COLUMN.putDelete(mut);
    ServerColumnFamily.TIME_COLUMN.putDelete(mut);
    ServerColumnFamily.DIRECTORY_COLUMN.putDelete(mut);
    mut.putDelete(DataFileColumnFamily.NAME.toString(), getMetadata(file, range));

    return mut;
  }

  private static Mutation createTablet(String tid, String endRow, String prevRow, String dir,
      String file, Range range) {
    KeyExtent ke = new KeyExtent(TableId.of(tid), endRow == null ? null : new Text(endRow),
        prevRow == null ? null : new Text(prevRow));
    Mutation mut = TabletColumnFamily.createPrevRowMutation(ke);

    ServerColumnFamily.TIME_COLUMN.put(mut, new Value("M0"));
    ServerColumnFamily.DIRECTORY_COLUMN.put(mut, new Value(dir));
    mut.put(DataFileColumnFamily.NAME.toString(), getMetadata(file, range),
        new DataFileValue(10, 200).encodeAsString());

    return mut;
  }

  // test two tablets splitting into four
  @ParameterizedTest
  @ArgumentsSource(RangeArgumentsProvider.class)
  public void testSplit3(Range range1, Range range2, Range range3) throws Exception {
    String filePrefix = "hdfs://nn:8000/accumulo/tables/0";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      try (BatchWriter bw1 = client.createBatchWriter(tableName);
          BatchWriter bw2 = client.createBatchWriter(tableName)) {
        bw1.addMutation(createTablet("0", "m", null, "/d1", filePrefix + "/d1/file1.rf", range1));
        bw1.addMutation(createTablet("0", null, "m", "/d2", filePrefix + "/d2/file2.rf", range2));

        bw1.flush();

        MetadataTableUtil.initializeClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        bw1.addMutation(createTablet("0", "f", null, "/d1", filePrefix + "/d1/file3.rf", range3));
        bw1.addMutation(createTablet("0", "m", "f", "/d3", filePrefix + "/d1/file1.rf", range1));
        bw1.addMutation(createTablet("0", "s", "m", "/d2", filePrefix + "/d2/file2.rf", range2));
        bw1.addMutation(createTablet("0", null, "s", "/d4", filePrefix + "/d2/file2.rf", range2));

        bw1.flush();

        int rc =
            MetadataTableUtil.checkClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        assertEquals(0, rc);
      }

      HashSet<String> files = new HashSet<>();
      int count = 0;

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new KeyExtent(TableId.of("1"), null, null).toMetaRange());
        for (Entry<Key,Value> entry : scanner) {
          if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
            files.add(entry.getKey().getColumnQualifier().toString());
            count++;
          }
        }
      }
      assertEquals(2, count);
      assertEquals(2, files.size());
      assertTrue(files.contains(getMetadata(filePrefix + "/d1/file1.rf", range1)));
      assertTrue(files.contains(getMetadata(filePrefix + "/d2/file2.rf", range2)));
    }
  }

  // test cloned marker
  @ParameterizedTest
  @ArgumentsSource(RangeArgumentsProvider.class)
  public void testClonedMarker(Range range1, Range range2, Range range3) throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      String filePrefix = "hdfs://nn:8000/accumulo/tables/0";

      try (BatchWriter bw1 = client.createBatchWriter(tableName);
          BatchWriter bw2 = client.createBatchWriter(tableName)) {
        bw1.addMutation(createTablet("0", "m", null, "/d1", filePrefix + "/d1/file1.rf", range1));
        bw1.addMutation(createTablet("0", null, "m", "/d2", filePrefix + "/d2/file2.rf", range2));

        bw1.flush();

        MetadataTableUtil.initializeClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        bw1.addMutation(deleteTablet("0", "m", null, filePrefix + "/d1/file1.rf", range1));
        bw1.addMutation(deleteTablet("0", null, "m", filePrefix + "/d2/file2.rf", range2));

        bw1.flush();

        bw1.addMutation(createTablet("0", "f", null, "/d1", filePrefix + "/d1/file3.rf", range3));
        bw1.addMutation(createTablet("0", "m", "f", "/d3", filePrefix + "/d1/file1.rf", range1));
        bw1.addMutation(createTablet("0", "s", "m", "/d2", filePrefix + "/d2/file3.rf", range3));
        bw1.addMutation(createTablet("0", null, "s", "/d4", filePrefix + "/d4/file3.rf", range3));

        bw1.flush();

        int rc =
            MetadataTableUtil.checkClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        assertEquals(1, rc);

        bw1.addMutation(deleteTablet("0", "m", "f", filePrefix + "/d1/file1.rf", range1));

        bw1.flush();

        bw1.addMutation(createTablet("0", "m", "f", "/d3", filePrefix + "/d1/file3.rf", range3));

        bw1.flush();

        rc = MetadataTableUtil.checkClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        assertEquals(0, rc);
      }
      HashSet<String> files = new HashSet<>();
      int count = 0;

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new KeyExtent(TableId.of("1"), null, null).toMetaRange());
        for (Entry<Key,Value> entry : scanner) {
          if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
            files.add(entry.getKey().getColumnQualifier().toString());
            count++;
          }
        }
      }
      assertEquals(3, count);
      assertEquals(3, files.size());
      assertTrue(
          files.contains(getMetadata("hdfs://nn:8000/accumulo/tables/0/d1/file1.rf", range1)));
      assertTrue(
          files.contains(getMetadata("hdfs://nn:8000/accumulo/tables/0/d2/file3.rf", range3)));
      assertTrue(
          files.contains(getMetadata("hdfs://nn:8000/accumulo/tables/0/d4/file3.rf", range3)));
    }
  }

  // test two tablets splitting into four
  @ParameterizedTest
  @ArgumentsSource(RangeArgumentsProvider.class)
  public void testMerge(Range range1, Range range2) throws Exception {
    String filePrefix = "hdfs://nn:8000/accumulo/tables/0";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      try (BatchWriter bw1 = client.createBatchWriter(tableName);
          BatchWriter bw2 = client.createBatchWriter(tableName)) {
        bw1.addMutation(createTablet("0", "m", null, "/d1", filePrefix + "/d1/file1.rf", range1));
        bw1.addMutation(createTablet("0", null, "m", "/d2", filePrefix + "/d2/file2.rf", range2));

        bw1.flush();

        MetadataTableUtil.initializeClone(tableName, TableId.of("0"), TableId.of("1"), client, bw2);

        bw1.addMutation(deleteTablet("0", "m", null, filePrefix + "/d1/file1.rf", range1));
        Mutation mut = createTablet("0", null, null, "/d2", filePrefix + "/d2/file2.rf", range2);
        mut.put(DataFileColumnFamily.NAME.toString(),
            getMetadata(filePrefix + "/d1/file1.rf", range1),
            new DataFileValue(10, 200).encodeAsString());
        bw1.addMutation(mut);

        bw1.flush();

        assertThrows(TabletDeletedException.class, () -> MetadataTableUtil.checkClone(tableName,
            TableId.of("0"), TableId.of("1"), client, bw2));
      }
    }
  }

  private static String getMetadata(String file, Range range) {
    return StoredTabletFile.of(new Path(file), range).getMetadata();
  }

  static class RangeArgumentsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return Stream.of(
          // Pass in up to 3 arguments of infinite ranges to test non-ranged files
          Arguments.of(new Range(), new Range(), new Range()),
          // For second run pass in up to 3 arguments with the first two non-infinite ranges
          Arguments.of(new Range(null, false, "row_0", true),
              new Range("row_0", false, "row_1", true), new Range()));
    }
  }
}

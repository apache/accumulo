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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.test.functional.BulkNewIT.hash;
import static org.apache.accumulo.test.functional.BulkNewIT.row;
import static org.apache.accumulo.test.functional.BulkNewIT.verifyMetadata;
import static org.apache.accumulo.test.functional.BulkNewIT.writeData;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.LoadPlan;
import org.apache.accumulo.core.data.LoadPlan.RangeType;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * This test creates a table with 1000 splits and then imports files into a sparse set of the
 * tablets. This test also splits the metadata table such that the tablet metadata for each tablet
 * of the test table is in its own metadata tablet. The test then runs with different values for the
 * TABLE_BULK_SKIP_THRESHOLD property starting with zero (disabled) then increasing.
 *
 * This test uses AccumuloClusterHarness instead of SharedMiniClusterBase so that we don't have to
 * re-merge the metadata table and delete the test table. Doing these two things, and then waiting
 * for balancing, takes a long time. It's faster to just start with a clean instance for each test
 * run.
 */
public class BulkNewMetadataSkipIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setMemory(ServerType.TABLET_SERVER, 512, MemoryUnit.MEGABYTE);
    cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "3s");
    cfg.setProperty(Property.TSERV_ASSIGNMENT_MAXCONCURRENT, "25");
    cfg.setNumTservers(1);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  public static String writeNonContiguousData(FileSystem fs, String file,
      AccumuloConfiguration aconf, int[] rows) throws Exception {
    String filename = file + RFile.EXTENSION;
    try (FileSKVWriter writer = FileOperations.getInstance().newWriterBuilder()
        .forFile(filename, fs, fs.getConf(), NoCryptoServiceFactory.NONE)
        .withTableConfiguration(aconf).build()) {
      writer.startDefaultLocalityGroup();
      for (int i : rows) {
        writer.append(new Key(new Text(row(i))), new Value(Integer.toString(i)));
      }
    }
    return hash(filename);
  }

  @BeforeEach
  @Override
  public void setupCluster() throws Exception {
    super.setupCluster();
    // prime the zk connection
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {}
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 0, 2, 4, 8, 16, 32, 64, 128})
  public void test(int skipDistance) throws Exception {

    final String tableName = getUniqueNames(1)[0] + "_" + skipDistance;
    final AccumuloConfiguration aconf = getCluster().getServerContext().getConfiguration();
    final FileSystem fs = getCluster().getFileSystem();
    final String rootPath = getCluster().getTemporaryPath().toString();
    final String dir = rootPath + "/" + tableName;

    fs.delete(new Path(dir), true);

    final SortedSet<Text> splits = new TreeSet<>();
    IntStream.rangeClosed(0, 1000).forEach(i -> splits.add(new Text(String.format("%04d", i))));

    final NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.setProperties(
        Map.of(Property.TABLE_BULK_SKIP_THRESHOLD.getKey(), Integer.toString(skipDistance)));
    ntc.withSplits(splits);

    final Map<String,Set<String>> hashes = new HashMap<>();
    IntStream.rangeClosed(0, 1000).forEach(i -> hashes.put(row(i) + "", new HashSet<>()));
    hashes.put("null", new HashSet<>());

    String h1 = writeData(fs, dir + "/f1.", aconf, 0, 11);
    IntStream.rangeClosed(0, 11).forEach(i -> hashes.get(row(i)).add(h1));

    int[] h2Rows = new int[] {11, 199, 200, 204};
    String h2 = writeNonContiguousData(fs, dir + "/f2.", aconf, h2Rows);
    for (int i : h2Rows) {
      hashes.get(row(i)).add(h2);
    }

    int[] h3Rows = new int[] {13, 200, 272, 273};
    String h3 = writeNonContiguousData(fs, dir + "/f3.", aconf, h3Rows);
    for (int i : h3Rows) {
      hashes.get(row(i)).add(h3);
    }

    int[] h4Rows = new int[] {300, 301, 672, 998};
    String h4 = writeNonContiguousData(fs, dir + "/f4.", aconf, h4Rows);
    for (int i : h4Rows) {
      hashes.get(row(i)).add(h4);
    }

    final LoadPlan loadPlan =
        LoadPlan.builder().loadFileTo("f1.rf", RangeType.FILE, row(0), row(11))
            .loadFileTo("f2.rf", RangeType.TABLE, row(10), row(11))
            .loadFileTo("f2.rf", RangeType.FILE, row(199), row(200))
            .loadFileTo("f2.rf", RangeType.TABLE, row(203), row(204))
            .loadFileTo("f3.rf", RangeType.TABLE, row(12), row(13))
            .loadFileTo("f3.rf", RangeType.TABLE, row(199), row(200))
            .loadFileTo("f3.rf", RangeType.FILE, row(272), row(273))
            .loadFileTo("f4.rf", RangeType.FILE, row(300), row(301))
            .loadFileTo("f4.rf", RangeType.TABLE, row(671), row(672))
            .loadFileTo("f4.rf", RangeType.TABLE, row(997), row(998)).build();

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      c.tableOperations().create(tableName, ntc);
      TableId tid = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      final SortedSet<Text> metadataSplits = new TreeSet<>();
      Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      final String mdTablePrefix = tid.canonical() + ";";
      s.forEach(e -> {
        final String row = e.getKey().getRow().toString();
        if (row.startsWith(mdTablePrefix)) {
          metadataSplits.add(new Text(row + "\\x00"));
        }
      });
      c.tableOperations().addSplits(MetadataTable.NAME, metadataSplits);

      c.tableOperations().importDirectory(dir).to(tableName).plan(loadPlan).load();

      verifyData(c, tableName, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 199, 200, 204,
          272, 273, 300, 301, 672, 998}, false);
      verifyMetadata(c, tableName, hashes);
    }
  }

  public static void verifyData(AccumuloClient client, String table, int[] expectedRows,
      boolean setTime) throws Exception {
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {

      Iterator<Entry<Key,Value>> iter = scanner.iterator();

      int count = 0;
      while (iter.hasNext()) {
        Entry<Key,Value> entry = iter.next();

        String expectedRow = String.format("%04d", expectedRows[count]);

        if (!entry.getKey().getRow().equals(new Text(expectedRow))) {
          throw new Exception("unexpected row " + entry.getKey() + " " + expectedRow);
        }

        if (Integer.parseInt(entry.getValue().toString()) != expectedRows[count]) {
          throw new Exception("unexpected value " + entry + " " + expectedRows[count]);
        }

        if (setTime) {
          assertEquals(1L, entry.getKey().getTimestamp());
        }

        count++;
      }
      if (iter.hasNext()) {
        throw new Exception("found more than expected " + iter.next());
      }
    }
  }

}

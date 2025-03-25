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
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class BulkNewMetadataSkipIT extends SharedMiniClusterBase {

  private static class ConfigurationCallback implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration conf) {
      cfg.setMemory(ServerType.TABLET_SERVER, 512, MemoryUnit.MEGABYTE);

      // use raw local file system
      conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
    }
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new ConfigurationCallback());
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private String getDir(FileSystem fs, String rootPath, String testName) throws Exception {
    String dir = rootPath + testName + getUniqueNames(1)[0];
    fs.delete(new Path(dir), true);
    return dir;
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

  @ParameterizedTest
  @ValueSource(ints = {0, 2, 4, 8, 16, 32, 64, 128})
  public void test(int skipDistance) throws Exception {

    String tableName = getUniqueNames(1)[0];

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      try {
        AccumuloConfiguration aconf = getCluster().getServerContext().getConfiguration();
        FileSystem fs = getCluster().getFileSystem();
        String rootPath = getCluster().getTemporaryPath().toString();

        SortedSet<Text> splits = new TreeSet<>();
        IntStream.rangeClosed(0, 1000).forEach(i -> splits.add(new Text(String.format("%04d", i))));
        NewTableConfiguration ntc = new NewTableConfiguration();
        ntc.setProperties(
            Map.of(Property.TABLE_BULK_SKIP_THRESHOLD.getKey(), Integer.toString(skipDistance)));
        ntc.withSplits(splits);
        c.tableOperations().create(tableName, ntc);

        String dir = getDir(fs, rootPath, "/testMetadataSkipIT-");

        Map<String,Set<String>> hashes = new HashMap<>();
        IntStream.rangeClosed(0, 1000).forEach(i -> hashes.put(row(i) + "", new HashSet<>()));
        hashes.put("null", new HashSet<>());

        String h1 = writeData(fs, dir + "/f1.", aconf, 0, 11);
        hashes.get(row(0)).add(h1);
        hashes.get(row(1)).add(h1);
        hashes.get(row(2)).add(h1);
        hashes.get(row(3)).add(h1);
        hashes.get(row(4)).add(h1);
        hashes.get(row(5)).add(h1);
        hashes.get(row(6)).add(h1);
        hashes.get(row(7)).add(h1);
        hashes.get(row(8)).add(h1);
        hashes.get(row(9)).add(h1);
        hashes.get(row(10)).add(h1);
        hashes.get(row(11)).add(h1);

        String h2 = writeNonContiguousData(fs, dir + "/f2.", aconf, new int[] {11, 199, 200, 204});
        hashes.get(row(11)).add(h2);
        hashes.get(row(199)).add(h2);
        hashes.get(row(200)).add(h2);
        hashes.get(row(204)).add(h2);

        String h3 = writeNonContiguousData(fs, dir + "/f3.", aconf, new int[] {13, 200, 272, 273});
        hashes.get(row(13)).add(h3);
        hashes.get(row(200)).add(h3);
        hashes.get(row(272)).add(h3);
        hashes.get(row(273)).add(h3);

        String h4 = writeNonContiguousData(fs, dir + "/f4.", aconf, new int[] {300, 301, 672, 998});
        hashes.get(row(300)).add(h4);
        hashes.get(row(301)).add(h4);
        hashes.get(row(672)).add(h4);
        hashes.get(row(998)).add(h4);

        LoadPlan loadPlan = LoadPlan.builder().loadFileTo("f1.rf", RangeType.FILE, row(0), row(11))
            .loadFileTo("f2.rf", RangeType.TABLE, row(10), row(11))
            .loadFileTo("f2.rf", RangeType.FILE, row(199), row(200))
            .loadFileTo("f2.rf", RangeType.TABLE, row(203), row(204))
            .loadFileTo("f3.rf", RangeType.TABLE, row(12), row(13))
            .loadFileTo("f3.rf", RangeType.TABLE, row(199), row(200))
            .loadFileTo("f3.rf", RangeType.FILE, row(272), row(273))
            .loadFileTo("f4.rf", RangeType.FILE, row(300), row(301))
            .loadFileTo("f4.rf", RangeType.TABLE, row(671), row(672))
            .loadFileTo("f4.rf", RangeType.TABLE, row(997), row(998)).build();

        c.tableOperations().importDirectory(dir).to(tableName).plan(loadPlan).load();

        verifyData(c, tableName, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 199, 200, 204,
            272, 273, 300, 301, 672, 998}, false);
        verifyMetadata(c, tableName, hashes);
      } finally {
        c.tableOperations().delete(tableName);
      }
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

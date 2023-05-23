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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportTableCommandWitlMultipleVolumes extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(ImportExportIT.class);

  Path v1, v2;

  public static String[] row_numbers = "1,2,3,4,5,6,7,8,9,10".split(",");

  String baseDirStr = "";
  String baseDir2Str = "";

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    File baseDir = cfg.getDir();
    baseDirStr = baseDir.toString();

    // get first volume name
    String[] baseDirArray = baseDirStr.split("/");
    String originalVolume = baseDirArray[2];

    // get second volume name
    String[] baseDir2Array = baseDirArray;
    baseDir2Array[2] = baseDir2Array[2] + "2";
    String secondVolume = baseDir2Array[2];

    // make second volume base directory
    for (String element : baseDir2Array) {
      baseDir2Str = baseDir2Str + "/" + element;
    }
    File baseDir2 = new File(baseDir2Str);

    File v1f = new File(baseDir, "volumes/v1");
    File v2f = new File(baseDir2, "volumes/v2");

    v1 = new Path("file://" + v1f.getAbsolutePath());
    v2 = new Path("file://" + v2f.getAbsolutePath());

    // Run MAC on two locations in the local file system
    cfg.setProperty(Property.INSTANCE_VOLUMES, v1 + "," + v2);

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test
  public void testExportCommand() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      final String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      // add splits to table
      SortedSet<Text> partitions = new TreeSet<>();
      for (String s : row_numbers) {
        partitions.add(new Text(s));
      }
      client.tableOperations().addSplits(tableName, partitions);

      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        for (int i = 1; i <= 5000000; i++) {
          Mutation m = new Mutation(Integer.toString(i));
          if (i % 2 != 0) {
            m.put(Integer.toString(i), "", String
                .format("file://localhost:8020/accumulo/tables/1/default_tablet/I00000%dp.rf", i));
          } else {
            m.put(Integer.toString(i), "", String
                .format("file://localhost:8020/accumulo/tables/2/default_tablet/I00000%dp.rf", i));
          }
          bw.addMutation(m);
        }
      }

      client.tableOperations().compact(tableName, null, null, true, true);
      client.tableOperations().flush(tableName, null, null, true);

      Path outputDir = new Path(cluster.getTemporaryPath(), getClass().getName());
      Path exportDir = new Path(outputDir, "export");
      client.tableOperations().offline(tableName, true);
      client.tableOperations().exportTable(tableName, exportDir.toString());

      try (Scanner scanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        scanner.setRange(new Range("1", "1<"));
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);

        for (Map.Entry<Key,Value> entry : scanner) {
          log.info("Key is: " + entry.getKey());
          log.info("Value is: " + entry.getValue());
          boolean inV1 = entry.getKey().getColumnQualifier().toString().contains(v1.toString());
          boolean inV2 = entry.getKey().getColumnQualifier().toString().contains(v2.toString());
          assertTrue(inV1 || inV2);
        }
      }

      FileSystem fs = cluster.getFileSystem();
      fs.deleteOnExit(v1);
      fs.deleteOnExit(v2);
    }
  }
}

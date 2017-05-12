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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class BulkFileIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration conf) {
    cfg.setMemory(ServerType.TABLET_SERVER, 128 * 4, MemoryUnit.MEGABYTE);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void testBulkFile() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    SortedSet<Text> splits = new TreeSet<>();
    for (String split : "0333 0666 0999 1333 1666".split(" "))
      splits.add(new Text(split));
    c.tableOperations().addSplits(tableName, splits);
    Configuration conf = new Configuration();
    AccumuloConfiguration aconf = new ServerConfigurationFactory(c.getInstance()).getSystemConfiguration();
    FileSystem fs = getCluster().getFileSystem();

    String rootPath = cluster.getTemporaryPath().toString();

    String dir = rootPath + "/bulk_test_diff_files_89723987592_" + getUniqueNames(1)[0];

    fs.delete(new Path(dir), true);

    FileSKVWriter writer1 = FileOperations.getInstance().newWriterBuilder().forFile(dir + "/f1." + RFile.EXTENSION, fs, conf).withTableConfiguration(aconf)
        .build();
    writer1.startDefaultLocalityGroup();
    writeData(writer1, 0, 333);
    writer1.close();

    FileSKVWriter writer2 = FileOperations.getInstance().newWriterBuilder().forFile(dir + "/f2." + RFile.EXTENSION, fs, conf).withTableConfiguration(aconf)
        .build();
    writer2.startDefaultLocalityGroup();
    writeData(writer2, 334, 999);
    writer2.close();

    FileSKVWriter writer3 = FileOperations.getInstance().newWriterBuilder().forFile(dir + "/f3." + RFile.EXTENSION, fs, conf).withTableConfiguration(aconf)
        .build();
    writer3.startDefaultLocalityGroup();
    writeData(writer3, 1000, 1999);
    writer3.close();

    FunctionalTestUtils.bulkImport(c, fs, tableName, dir);

    FunctionalTestUtils.checkRFiles(c, tableName, 6, 6, 1, 1);

    verifyData(tableName, 0, 1999);

  }

  private void verifyData(String table, int s, int e) throws Exception {
    Scanner scanner = getConnector().createScanner(table, Authorizations.EMPTY);

    Iterator<Entry<Key,Value>> iter = scanner.iterator();

    for (int i = s; i <= e; i++) {
      if (!iter.hasNext())
        throw new Exception("row " + i + " not found");

      Entry<Key,Value> entry = iter.next();

      String row = String.format("%04d", i);

      if (!entry.getKey().getRow().equals(new Text(row)))
        throw new Exception("unexpected row " + entry.getKey() + " " + i);

      if (Integer.parseInt(entry.getValue().toString()) != i)
        throw new Exception("unexpected value " + entry + " " + i);
    }

    if (iter.hasNext())
      throw new Exception("found more than expected " + iter.next());
  }

  private void writeData(FileSKVWriter w, int s, int e) throws Exception {
    for (int i = s; i <= e; i++) {
      w.append(new Key(new Text(String.format("%04d", i))), new Value(Integer.toString(i).getBytes(UTF_8)));
    }
  }

}

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
package org.apache.accumulo.test.performance.metadata;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.mrit.IntegrationTestMapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Test;

// ACCUMULO-3327
public class FastBulkImportIT extends ConfigurableMacBase {

  @BeforeClass
  static public void checkMR() {
    assumeFalse(IntegrationTestMapReduce.isMapReduce());
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 120;
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(3);
    cfg.setProperty(Property.TSERV_BULK_ASSIGNMENT_THREADS, "5");
    cfg.setProperty(Property.TSERV_BULK_PROCESS_THREADS, "5");
    cfg.setProperty(Property.TABLE_MAJC_RATIO, "9999");
    cfg.setProperty(Property.TABLE_FILE_MAX, "9999");
    cfg.setProperty(Property.TABLE_DURABILITY, "none");
  }

  @Test
  public void test() throws Exception {
    log.info("Creating table");
    final String tableName = getUniqueNames(1)[0];
    final Connector c = getConnector();
    c.tableOperations().create(tableName);
    log.info("Adding splits");
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 1; i < 0xfff; i += 7) {
      splits.add(new Text(Integer.toHexString(i)));
    }
    c.tableOperations().addSplits(tableName, splits);

    log.info("Creating lots of bulk import files");
    FileSystem fs = getCluster().getFileSystem();
    Path basePath = getCluster().getTemporaryPath();
    CachedConfiguration.setInstance(fs.getConf());

    Path base = new Path(basePath, "testBulkFail_" + tableName);
    fs.delete(base, true);
    fs.mkdirs(base);
    Path bulkFailures = new Path(base, "failures");
    Path files = new Path(base, "files");
    fs.mkdirs(bulkFailures);
    fs.mkdirs(files);
    for (int i = 0; i < 100; i++) {
      FileSKVWriter writer = FileOperations.getInstance().newWriterBuilder().forFile(files.toString() + "/bulk_" + i + "." + RFile.EXTENSION, fs, fs.getConf())
          .withTableConfiguration(DefaultConfiguration.getInstance()).build();
      writer.startDefaultLocalityGroup();
      for (int j = 0x100; j < 0xfff; j += 3) {
        writer.append(new Key(Integer.toHexString(j)), new Value(new byte[0]));
      }
      writer.close();
    }
    log.info("Waiting for balance");
    c.instanceOperations().waitForBalance();

    log.info("Bulk importing files");
    long now = System.currentTimeMillis();
    c.tableOperations().importDirectory(tableName, files.toString(), bulkFailures.toString(), true);
    double diffSeconds = (System.currentTimeMillis() - now) / 1000.;
    log.info(String.format("Import took %.2f seconds", diffSeconds));
    assertTrue(diffSeconds < 30);
  }

}

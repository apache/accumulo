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

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class BulkImportMonitoringIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.useMiniDFS(true);
    cfg.setProperty(Property.GC_FILE_ARCHIVE, "false");
  }

  @Test
  public void test() throws Exception {
    getCluster().getClusterControl().start(ServerType.MONITOR);
    final Connector c = getConnector();
    final String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "1");
    // splits to slow down bulk import
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 1; i < 0xf; i++) {
      splits.add(new Text(Integer.toHexString(i)));
    }
    c.tableOperations().addSplits(tableName, splits);

    MasterMonitorInfo stats = getCluster().getMasterMonitorInfo();
    assertEquals(1, stats.tServerInfo.size());
    assertEquals(0, stats.bulkImports.size());
    assertEquals(0, stats.tServerInfo.get(0).bulkImports.size());

    log.info("Creating lots of bulk import files");
    final FileSystem fs = getCluster().getFileSystem();
    final Path basePath = getCluster().getTemporaryPath();
    CachedConfiguration.setInstance(fs.getConf());

    final Path base = new Path(basePath, "testBulkLoad" + tableName);
    fs.delete(base, true);
    fs.mkdirs(base);

    ExecutorService es = Executors.newFixedThreadPool(5);
    List<Future<Pair<String,String>>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final int which = i;
      futures.add(es.submit(new Callable<Pair<String,String>>() {
        @Override
        public Pair<String,String> call() throws Exception {
          Path bulkFailures = new Path(base, "failures" + which);
          Path files = new Path(base, "files" + which);
          fs.mkdirs(bulkFailures);
          fs.mkdirs(files);
          for (int i = 0; i < 10; i++) {
            FileSKVWriter writer = FileOperations.getInstance().newWriterBuilder()
                .forFile(files.toString() + "/bulk_" + i + "." + RFile.EXTENSION, fs, fs.getConf()).withTableConfiguration(DefaultConfiguration.getInstance())
                .build();
            writer.startDefaultLocalityGroup();
            for (int j = 0x100; j < 0xfff; j += 3) {
              writer.append(new Key(Integer.toHexString(j)), new Value(new byte[0]));
            }
            writer.close();
          }
          return new Pair<>(files.toString(), bulkFailures.toString());
        }
      }));
    }
    List<Pair<String,String>> dirs = new ArrayList<>();
    for (Future<Pair<String,String>> f : futures) {
      dirs.add(f.get());
    }
    log.info("Importing");
    long now = System.currentTimeMillis();
    List<Future<Object>> errs = new ArrayList<>();
    for (Pair<String,String> entry : dirs) {
      final String dir = entry.getFirst();
      final String err = entry.getSecond();
      errs.add(es.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          c.tableOperations().importDirectory(tableName, dir, err, false);
          return null;
        }
      }));
    }
    es.shutdown();
    while (!es.isTerminated() && stats.bulkImports.size() + stats.tServerInfo.get(0).bulkImports.size() == 0) {
      es.awaitTermination(10, TimeUnit.MILLISECONDS);
      stats = getCluster().getMasterMonitorInfo();
    }
    log.info(stats.bulkImports.toString());
    assertTrue(stats.bulkImports.size() > 0);
    // look for exception
    for (Future<Object> err : errs) {
      err.get();
    }
    es.awaitTermination(2, TimeUnit.MINUTES);
    assertTrue(es.isTerminated());
    log.info(String.format("Completed in %.2f seconds", (System.currentTimeMillis() - now) / 1000.));
  }
}

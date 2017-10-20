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

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.gson.Gson;

// ACCUMULO-3949, ACCUMULO-3953
public class GetFileInfoBulkIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.useMiniDFS(true);
    cfg.setProperty(Property.GC_FILE_ARCHIVE, "false");
  }

  @SuppressWarnings("unchecked")
  long getOpts() throws Exception {
    String uri = getCluster().getMiniDfs().getHttpUri(0);
    URL url = new URL(uri + "/jmx");
    log.debug("Fetching web page " + url);
    String jsonString = FunctionalTestUtils.readAll(url.openStream());
    Gson gson = new Gson();
    Map<Object,Object> jsonObject = (Map<Object,Object>) gson.fromJson(jsonString, Object.class);
    List<Object> beans = (List<Object>) jsonObject.get("beans");
    for (Object bean : beans) {
      Map<Object,Object> map = (Map<Object,Object>) bean;
      if (map.get("name").toString().equals("Hadoop:service=NameNode,name=NameNodeActivity")) {
        return (long) Double.parseDouble(map.get("FileInfoOps").toString());
      }
    }
    return 0;
  }

  @Test
  public void test() throws Exception {
    final Connector c = getConnector();
    getCluster().getClusterControl().kill(ServerType.GARBAGE_COLLECTOR, "localhost");
    final String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    // turn off compactions
    c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "2000");
    c.tableOperations().setProperty(tableName, Property.TABLE_FILE_MAX.getKey(), "2000");
    // splits to slow down bulk import
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 1; i < 0xf; i++) {
      splits.add(new Text(Integer.toHexString(i)));
    }
    c.tableOperations().addSplits(tableName, splits);

    MasterMonitorInfo stats = getCluster().getMasterMonitorInfo();
    assertEquals(1, stats.tServerInfo.size());

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
          for (int i = 0; i < 100; i++) {
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
    long startOps = getOpts();
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
    for (Future<Object> err : errs) {
      err.get();
    }
    es.shutdown();
    es.awaitTermination(2, TimeUnit.MINUTES);
    log.info(String.format("Completed in %.2f seconds", (System.currentTimeMillis() - now) / 1000.));
    sleepUninterruptibly(30, TimeUnit.SECONDS);
    long getFileInfoOpts = getOpts() - startOps;
    log.info("# opts: {}", getFileInfoOpts);
    assertTrue("unexpected number of getFileOps", getFileInfoOpts < 2100 && getFileInfoOpts > 1000);
  }

}

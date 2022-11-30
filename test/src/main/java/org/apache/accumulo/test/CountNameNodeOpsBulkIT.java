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

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Originally written for ACCUMULO-3949 and ACCUMULO-3953 to count the number of FileInfo calls to
 * the NameNode. Updated in 2.0 to count the calls for new bulk import comparing it to the old.
 */
public class CountNameNodeOpsBulkIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.useMiniDFS(true);
  }

  @SuppressFBWarnings(value = {"PATH_TRAVERSAL_IN", "URLCONNECTION_SSRF_FD"},
      justification = "path provided by test; url provided by test")
  private Map<?,?> getStats() throws Exception {
    String uri = getCluster().getMiniDfs().getHttpUri(0);
    URL url = new URL(uri + "/jmx");
    log.debug("Fetching web page " + url);
    String jsonString = FunctionalTestUtils.readWebPage(url).body();
    Map<?,?> jsonObject = new Gson().fromJson(jsonString, Map.class);
    List<?> beans = (List<?>) jsonObject.get("beans");
    for (Object bean : beans) {
      Map<?,?> map = (Map<?,?>) bean;
      if (map.get("name").toString().equals("Hadoop:service=NameNode,name=NameNodeActivity")) {
        return map;
      }
    }
    return new HashMap<>(0);
  }

  private long getStat(Map<?,?> map, String stat) {
    return (long) Double.parseDouble(map.get(stat).toString());
  }

  @Test
  public void compareOldNewBulkImportTest() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      getCluster().getClusterControl().kill(ServerType.GARBAGE_COLLECTOR, "localhost");

      final String tableName = getUniqueNames(1)[0];
      // disable compactions
      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_MAJC_RATIO.getKey(), "2000");
      props.put(Property.TABLE_FILE_MAX.getKey(), "2000");
      // splits to slow down bulk import
      SortedSet<Text> splits = new TreeSet<>();
      for (int i = 1; i < 0xf; i++) {
        splits.add(new Text(Integer.toHexString(i)));
      }

      var ntc = new NewTableConfiguration().setProperties(props).withSplits(splits);
      c.tableOperations().create(tableName, ntc);

      ManagerMonitorInfo stats = getCluster().getManagerMonitorInfo();
      assertEquals(1, stats.tServerInfo.size());

      log.info("Creating lots of bulk import files");
      final FileSystem fs = getCluster().getFileSystem();
      final Path basePath = getCluster().getTemporaryPath();

      final Path base = new Path(basePath, "testBulkLoad" + tableName);
      fs.delete(base, true);
      fs.mkdirs(base);

      ExecutorService es = Executors.newFixedThreadPool(5);
      List<Future<String>> futures = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        final int which = i;
        futures.add(es.submit(() -> {
          Path files = new Path(base, "files" + which);
          fs.mkdirs(files);
          for (int i1 = 0; i1 < 100; i1++) {
            FileSKVWriter writer = FileOperations.getInstance().newWriterBuilder()
                .forFile(files + "/bulk_" + i1 + "." + RFile.EXTENSION, fs, fs.getConf(),
                    NoCryptoServiceFactory.NONE)
                .withTableConfiguration(DefaultConfiguration.getInstance()).build();
            writer.startDefaultLocalityGroup();
            for (int j = 0x100; j < 0xfff; j += 3) {
              writer.append(new Key(Integer.toHexString(j)), new Value());
            }
            writer.close();
          }
          return files.toString();
        }));
      }
      List<String> dirs = new ArrayList<>();
      for (Future<String> f : futures) {
        dirs.add(f.get());
      }
      log.info("Importing");
      long startOps = getStat(getStats(), "FileInfoOps");
      long now = System.currentTimeMillis();
      List<Future<Object>> errs = new ArrayList<>();
      for (String dir : dirs) {
        errs.add(es.submit(() -> {
          c.tableOperations().importDirectory(dir).to(tableName).load();
          return null;
        }));
      }
      for (Future<Object> err : errs) {
        err.get();
      }
      es.shutdown();
      es.awaitTermination(2, TimeUnit.MINUTES);
      log.info(
          String.format("Completed in %.2f seconds", (System.currentTimeMillis() - now) / 1000.));
      sleepUninterruptibly(30, TimeUnit.SECONDS);
      Map<?,?> map = getStats();
      map.forEach((k, v) -> {
        try {
          if (v != null && Double.parseDouble(v.toString()) > 0.0) {
            log.debug("{}:{}", k, v);
          }
        } catch (NumberFormatException e) {
          // only looking for numbers
        }
      });
      long getFileInfoOpts = getStat(map, "FileInfoOps") - startOps;
      log.info("New bulk import used {} opts, vs old using 2060", getFileInfoOpts);
      // counts for old bulk import:
      // Expected number of FileInfoOps was between 1000 and 2100
      // new bulk import is way better :)
      assertEquals(20, getFileInfoOpts, "unexpected number of FileInfoOps");
    }
  }
}

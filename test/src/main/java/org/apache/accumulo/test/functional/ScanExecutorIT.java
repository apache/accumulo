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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.scan.IdleRatioScanPrioritizer;
import org.apache.accumulo.core.spi.scan.ScanDispatcher;
import org.apache.accumulo.core.spi.scan.ScanExecutor;
import org.apache.accumulo.core.spi.scan.ScanInfo;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ScanExecutorIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {

    Map<String,String> siteCfg = new HashMap<>();

    siteCfg.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "se1.threads", "3");
    siteCfg.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "se1.prioritizer",
        IdleRatioScanPrioritizer.class.getName());

    siteCfg.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "se2.threads", "1");

    cfg.setSiteConfig(siteCfg);
  }

  public static class TestScanDispatcher implements ScanDispatcher {
    @Override
    public String dispatch(ScanInfo scanInfo, Map<String,ScanExecutor> scanExecutors) {
      switch (scanInfo.getScanType()) {
        case MULTI:
          return "se1";
        case SINGLE:
          return "se2";
        default:
          return "default";
      }
    }
  }

  private static void batchScan(String tableName, Connector c) {
    try (BatchScanner bscanner = c.createBatchScanner(tableName, Authorizations.EMPTY, 3)) {
      bscanner
          .setRanges(Arrays.asList(Range.exact("r001"), Range.exact("r006"), Range.exact("r008")));
      for (Entry<Key,Value> entry : bscanner) {
        System.out.println(entry.getKey() + " " + entry.getValue());
      }

    } catch (TableNotFoundException e) {
      e.printStackTrace();
    }
  }

  private static void scan(String tableName, Connector c) {
    try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
      for (Entry<Key,Value> entry : scanner) {
        System.out.println(entry.getKey() + " " + entry.getValue());
      }
    } catch (TableNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();

    String tableName = "dumpty";

    Map<String,String> props = new HashMap<>();
    props.put(Property.TABLE_SCAN_DISPATCHER.getKey(), TestScanDispatcher.class.getName());

    c.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));
    SortedSet<Text> splits = new TreeSet<>();
    splits.add(new Text("r003"));
    splits.add(new Text("r007"));
    c.tableOperations().addSplits(tableName, splits);

    try (BatchWriter writer = c.createBatchWriter(tableName)) {
      Mutation m = new Mutation("r001");
      m.put("f003", "q009", "v001");
      writer.addMutation(m);

      m = new Mutation("r006");
      m.put("f003", "q005", "v002");
      writer.addMutation(m);

      m = new Mutation("r008");
      m.put("f003", "q007", "v003");
      writer.addMutation(m);
    }

    ExecutorService es = Executors.newFixedThreadPool(20);
    List<Future<?>> futures = new ArrayList<>();

    System.out.println("Batch Scanner");

    for (int i = 0; i < 100; i++) {
      futures.add(es.submit(() -> {
        batchScan(tableName, c);
      }));
    }

    for (Future<?> future : futures) {
      future.get();
    }

    futures.clear();

    System.out.println();
    System.out.println("Scanner");

    for (int i = 0; i < 100; i++) {
      futures.add(es.submit(() -> {
        scan(tableName, c);
      }));
    }

    for (Future<?> future : futures) {
      future.get();
    }

    es.shutdown();
  }
}

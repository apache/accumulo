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
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.apache.accumulo.core.util.Stat;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ScanExecutorIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {

    Map<String,String> siteCfg = new HashMap<>();

    siteCfg.put(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), "200");
    siteCfg.put(Property.TSERV_MINTHREADS.getKey(), "200");
    siteCfg.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "se1.threads", "2");
   siteCfg.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "se1.prioritizer",
       IdleRatioScanPrioritizer.class.getName());

    siteCfg.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "se2.threads", "2");
//    siteCfg.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "se2.prioritizer",
//        IdleRatioScanPrioritizer.class.getName());
    cfg.setSiteConfig(siteCfg);
  }

  public static class TestScanDispatcher implements ScanDispatcher {
    @Override
    public String dispatch(ScanInfo scanInfo, Map<String,ScanExecutor> scanExecutors) {
      switch (scanInfo.getScanType()) {
        case MULTI:
          return "se2";
        case SINGLE:
          return "se1";
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

  private static long scan(String tableName, Connector c, String row, String fam) {
    long t1 = System.currentTimeMillis();
    int count = 0;
    try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setRange(Range.exact(row));
      scanner.fetchColumnFamily(new Text(fam));
      for (Entry<Key,Value> entry : scanner) {
        count++;
      }
    } catch (TableNotFoundException e) {
      e.printStackTrace();
    }

    return System.currentTimeMillis() - t1;
  }

  private long scan(String tableName, Connector c, AtomicBoolean stop) {
    long count = 0;
    while (!stop.get()) {
      try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
        for (Entry<Key,Value> entry : scanner) {
          count++;
          if (stop.get()) {
            return count;
          }
        }
      } catch (TableNotFoundException e) {
        e.printStackTrace();
      }
    }

    return count;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();

    String tableName = "dumpty";

    Map<String,String> props = new HashMap<>();
    props.put(Property.TABLE_SCAN_DISPATCHER.getKey(), TestScanDispatcher.class.getName());

    c.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

    try (BatchWriter writer = c.createBatchWriter(tableName)) {

      int v = 0;
      for (int r = 0; r < 10000; r++) {
        String row = String.format("%06d", r);
        Mutation m = new Mutation(row);
        for (int f = 0; f < 10; f++) {
          String fam = String.format("%03d", f);
          for (int q = 0; q < 10; q++) {
            String qual = String.format("%03d", q);
            String val = String.format("%09d", v++);
            m.put(fam, qual, val);
          }
        }

        writer.addMutation(m);
      }
    }

    c.tableOperations().compact(tableName, null, null, true, true);

    AtomicBoolean stop = new AtomicBoolean(false);

    int numLongScans = 50;
    ExecutorService longScans = Executors.newFixedThreadPool(numLongScans);

    for (int i = 0; i < numLongScans; i++) {
      longScans.execute(() -> scan(tableName, c, stop));
    }

    int numLookups = 3000;
    ExecutorService shortScans = Executors.newFixedThreadPool(5);

    List<Future<Long>> futures = new ArrayList<>();

    Random rand = new Random();

    for (int i = 0; i < numLookups; i++) {
      String row = String.format("%06d", rand.nextInt(10000));
      String fam = String.format("%03d", rand.nextInt(10));
      futures.add(shortScans.submit(() -> scan(tableName, c, row,fam)));
    }

    Stat stat = new Stat();
    for (Future<Long> future : futures) {
      stat.addStat(future.get());
    }

    System.out.println(stat);

    stop.set(true);
    longScans.shutdown();
    shortScans.shutdown();
  }
}

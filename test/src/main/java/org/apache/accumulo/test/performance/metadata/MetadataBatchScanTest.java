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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.Stat;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

/**
 * This little program can be used to write a lot of metadata entries and measure the performance of varying numbers of threads doing metadata lookups using the
 * batch scanner.
 *
 *
 */

public class MetadataBatchScanTest {

  private static final Logger log = LoggerFactory.getLogger(MetadataBatchScanTest.class);

  public static void main(String[] args) throws Exception {

    ClientOpts opts = new ClientOpts();
    opts.parseArgs(MetadataBatchScanTest.class.getName(), args);
    Instance inst = new ZooKeeperInstance(ClientConfiguration.create().withInstance("acu14").withZkHosts("localhost"));
    final Connector connector = inst.getConnector(opts.getPrincipal(), opts.getToken());

    TreeSet<Long> splits = new TreeSet<>();
    Random r = new Random(42);

    while (splits.size() < 99999) {
      splits.add((r.nextLong() & 0x7fffffffffffffffl) % 1000000000000l);
    }

    Table.ID tid = Table.ID.of("8");
    Text per = null;

    ArrayList<KeyExtent> extents = new ArrayList<>();

    for (Long split : splits) {
      Text er = new Text(String.format("%012d", split));
      KeyExtent ke = new KeyExtent(tid, er, per);
      per = er;

      extents.add(ke);
    }

    extents.add(new KeyExtent(tid, null, per));

    if (args[0].equals("write")) {

      BatchWriter bw = connector.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());

      for (KeyExtent extent : extents) {
        Mutation mut = extent.getPrevRowUpdateMutation();
        new TServerInstance(HostAndPort.fromParts("192.168.1.100", 4567), "DEADBEEF").putLocation(mut);
        bw.addMutation(mut);
      }

      bw.close();
    } else if (args[0].equals("writeFiles")) {
      BatchWriter bw = connector.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());

      for (KeyExtent extent : extents) {

        Mutation mut = new Mutation(extent.getMetadataEntry());

        String dir = "/t-" + UUID.randomUUID();

        TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut, new Value(dir.getBytes(UTF_8)));

        for (int i = 0; i < 5; i++) {
          mut.put(DataFileColumnFamily.NAME, new Text(dir + "/00000_0000" + i + ".map"), new DataFileValue(10000, 1000000).encodeAsValue());
        }

        bw.addMutation(mut);
      }

      bw.close();
    } else if (args[0].equals("scan")) {

      int numThreads = Integer.parseInt(args[1]);
      final int numLoop = Integer.parseInt(args[2]);
      int numLookups = Integer.parseInt(args[3]);

      HashSet<Integer> indexes = new HashSet<>();
      while (indexes.size() < numLookups) {
        indexes.add(r.nextInt(extents.size()));
      }

      final List<Range> ranges = new ArrayList<>();
      for (Integer i : indexes) {
        ranges.add(extents.get(i).toMetadataRange());
      }

      Thread threads[] = new Thread[numThreads];

      for (int i = 0; i < threads.length; i++) {
        threads[i] = new Thread(new Runnable() {

          @Override
          public void run() {
            try {
              System.out.println(runScanTest(connector, numLoop, ranges));
            } catch (Exception e) {
              log.error("Exception while running scan test.", e);
            }
          }
        });
      }

      long t1 = System.currentTimeMillis();

      for (Thread thread : threads) {
        thread.start();
      }

      for (Thread thread : threads) {
        thread.join();
      }

      long t2 = System.currentTimeMillis();

      System.out.printf("tt : %6.2f%n", (t2 - t1) / 1000.0);

    } else {
      throw new IllegalArgumentException();
    }

  }

  private static ScanStats runScanTest(Connector connector, int numLoop, List<Range> ranges) throws Exception {
    ScanStats stats = new ScanStats();

    try (BatchScanner bs = connector.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 1)) {
      bs.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);
      TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(bs);

      bs.setRanges(ranges);

      // System.out.println(ranges);
      for (int i = 0; i < numLoop; i++) {
        ScanStat ss = scan(bs, ranges, null);
        stats.merge(ss);
      }
    }
    return stats;
  }

  private static class ScanStat {
    long delta1;
    long delta2;
    int count1;
    int count2;
  }

  private static class ScanStats {
    Stat delta1 = new Stat();
    Stat delta2 = new Stat();
    Stat count1 = new Stat();
    Stat count2 = new Stat();

    void merge(ScanStat ss) {
      delta1.addStat(ss.delta1);
      delta2.addStat(ss.delta2);
      count1.addStat(ss.count1);
      count2.addStat(ss.count2);
    }

    @Override
    public String toString() {
      return "[" + delta1 + "] [" + delta2 + "]";
    }
  }

  private static ScanStat scan(BatchScanner bs, List<Range> ranges, Scanner scanner) {

    // System.out.println("ranges : "+ranges);

    ScanStat ss = new ScanStat();

    long t1 = System.currentTimeMillis();
    int count = Iterators.size(bs.iterator());
    bs.close();
    long t2 = System.currentTimeMillis();

    ss.delta1 = t2 - t1;
    ss.count1 = count;

    count = 0;
    t1 = System.currentTimeMillis();
    /*
     * for (Range range : ranges) { scanner.setRange(range); for (Entry<Key, Value> entry : scanner) { count++; } }
     */

    t2 = System.currentTimeMillis();

    ss.delta2 = t2 - t1;
    ss.count2 = count;

    return ss;
  }

}

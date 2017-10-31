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

import static org.junit.Assert.assertFalse;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.AdminUtil.FateStatus;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl.LogWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriterFactory;
import org.apache.accumulo.test.TestIngest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;

import com.google.common.collect.Iterators;

public class FunctionalTestUtils {

  public static int countRFiles(Connector c, String tableName) throws Exception {
    Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    Table.ID tableId = Table.ID.of(c.tableOperations().tableIdMap().get(tableName));
    scanner.setRange(MetadataSchema.TabletsSection.getRange(tableId));
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);

    return Iterators.size(scanner.iterator());
  }

  static void checkRFiles(Connector c, String tableName, int minTablets, int maxTablets, int minRFiles, int maxRFiles) throws Exception {
    Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    String tableId = c.tableOperations().tableIdMap().get(tableName);
    scanner.setRange(new Range(new Text(tableId + ";"), true, new Text(tableId + "<"), true));
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
    MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);

    HashMap<Text,Integer> tabletFileCounts = new HashMap<>();

    for (Entry<Key,Value> entry : scanner) {

      Text row = entry.getKey().getRow();

      Integer count = tabletFileCounts.get(row);
      if (count == null)
        count = 0;
      if (entry.getKey().getColumnFamily().equals(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME)) {
        count = count + 1;
      }

      tabletFileCounts.put(row, count);
    }

    if (tabletFileCounts.size() < minTablets || tabletFileCounts.size() > maxTablets) {
      throw new Exception("Did not find expected number of tablets " + tabletFileCounts.size());
    }

    Set<Entry<Text,Integer>> es = tabletFileCounts.entrySet();
    for (Entry<Text,Integer> entry : es) {
      if (entry.getValue() > maxRFiles || entry.getValue() < minRFiles) {
        throw new Exception("tablet " + entry.getKey() + " has " + entry.getValue() + " map files");
      }
    }
  }

  static public void bulkImport(Connector c, FileSystem fs, String table, String dir) throws Exception {
    String failDir = dir + "_failures";
    Path failPath = new Path(failDir);
    fs.delete(failPath, true);
    fs.mkdirs(failPath);

    // Ensure server can read/modify files
    c.tableOperations().importDirectory(table, dir, failDir, false);

    if (fs.listStatus(failPath).length > 0) {
      throw new Exception("Some files failed to bulk import");
    }

  }

  static public void checkSplits(Connector c, String table, int min, int max) throws Exception {
    Collection<Text> splits = c.tableOperations().listSplits(table);
    if (splits.size() < min || splits.size() > max) {
      throw new Exception("# of table splits points out of range, #splits=" + splits.size() + " table=" + table + " min=" + min + " max=" + max);
    }
  }

  static public void createRFiles(final Connector c, final FileSystem fs, String path, int rows, int splits, int threads) throws Exception {
    fs.delete(new Path(path), true);
    ExecutorService threadPool = Executors.newFixedThreadPool(threads);
    final AtomicBoolean fail = new AtomicBoolean(false);
    for (int i = 0; i < rows; i += rows / splits) {
      final TestIngest.Opts opts = new TestIngest.Opts();
      opts.outputFile = String.format("%s/mf%s", path, i);
      opts.random = 56;
      opts.timestamp = 1;
      opts.dataSize = 50;
      opts.rows = rows / splits;
      opts.startRow = i;
      opts.cols = 1;
      threadPool.execute(new Runnable() {
        @Override
        public void run() {
          try {
            TestIngest.ingest(c, fs, opts, new BatchWriterOpts());
          } catch (Exception e) {
            fail.set(true);
          }
        }
      });
    }
    threadPool.shutdown();
    threadPool.awaitTermination(1, TimeUnit.HOURS);
    assertFalse(fail.get());
  }

  static public String readAll(InputStream is) throws IOException {
    byte[] buffer = new byte[4096];
    StringBuilder result = new StringBuilder();
    while (true) {
      int n = is.read(buffer);
      if (n <= 0)
        break;
      result.append(new String(buffer, 0, n));
    }
    return result.toString();
  }

  public static String readAll(MiniAccumuloClusterImpl c, Class<?> klass, Process p) throws Exception {
    for (LogWriter writer : c.getLogWriters())
      writer.flush();
    return readAll(new FileInputStream(c.getConfig().getLogDir() + "/" + klass.getSimpleName() + "_" + p.hashCode() + ".out"));
  }

  static Mutation nm(String row, String cf, String cq, Value value) {
    Mutation m = new Mutation(new Text(row));
    m.put(new Text(cf), new Text(cq), value);
    return m;
  }

  static Mutation nm(String row, String cf, String cq, String value) {
    return nm(row, cf, cq, new Value(value.getBytes()));
  }

  public static SortedSet<Text> splits(String[] splits) {
    SortedSet<Text> result = new TreeSet<>();
    for (String split : splits)
      result.add(new Text(split));
    return result;
  }

  public static void assertNoDanglingFateLocks(Instance instance, AccumuloCluster cluster) {
    FateStatus fateStatus = getFateStatus(instance, cluster);
    Assert.assertEquals("Dangling FATE locks : " + fateStatus.getDanglingHeldLocks(), 0, fateStatus.getDanglingHeldLocks().size());
    Assert.assertEquals("Dangling FATE locks : " + fateStatus.getDanglingWaitingLocks(), 0, fateStatus.getDanglingWaitingLocks().size());
  }

  private static FateStatus getFateStatus(Instance instance, AccumuloCluster cluster) {
    try {
      AdminUtil<String> admin = new AdminUtil<>(false);
      String secret = cluster.getSiteConfiguration().get(Property.INSTANCE_SECRET);
      IZooReaderWriter zk = new ZooReaderWriterFactory().getZooReaderWriter(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut(), secret);
      ZooStore<String> zs = new ZooStore<>(ZooUtil.getRoot(instance) + Constants.ZFATE, zk);
      FateStatus fateStatus = admin.getStatus(zs, zk, ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS, null, null);
      return fateStatus;
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}

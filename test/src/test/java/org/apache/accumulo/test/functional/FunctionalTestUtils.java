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

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl.LogWriter;
import org.apache.accumulo.test.TestIngest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class FunctionalTestUtils {

  static void checkRFiles(Connector c, String tableName, int minTablets, int maxTablets, int minRFiles, int maxRFiles) throws Exception {
    Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    String tableId = c.tableOperations().tableIdMap().get(tableName);
    scanner.setRange(new Range(new Text(tableId + ";"), true, new Text(tableId + "<"), true));
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
    MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);

    HashMap<Text,Integer> tabletFileCounts = new HashMap<Text,Integer>();

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

  static public void createRFiles(final Connector c, FileSystem fs, String path, int rows, int splits, int threads) throws Exception {
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
            TestIngest.ingest(c, opts, new BatchWriterOpts());
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
    StringBuffer result = new StringBuffer();
    while (true) {
      int n = is.read(buffer);
      if (n <= 0)
        break;
      result.append(new String(buffer, 0, n));
    }
    return result.toString();
  }

  static String readAll(MiniAccumuloClusterImpl c, Class<?> klass, Process p) throws Exception {
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
    SortedSet<Text> result = new TreeSet<Text>();
    for (String split : splits)
      result.add(new Text(split));
    return result;
  }

  public static int count(Iterable<?> i) {
    int count = 0;
    for (@SuppressWarnings("unused")
    Object entry : i)
      count++;
    return count;
  }

}

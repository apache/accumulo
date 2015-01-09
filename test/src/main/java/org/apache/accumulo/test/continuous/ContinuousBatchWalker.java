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
package org.apache.accumulo.test.continuous;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.cli.BatchScannerOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.validators.PositiveInteger;

public class ContinuousBatchWalker {

  static class Opts extends ContinuousWalk.Opts {
    @Parameter(names = "--numToScan", description = "Number rows to scan between sleeps", required = true, validateWith = PositiveInteger.class)
    long numToScan = 0;
  }

  public static void main(String[] args) throws Exception {

    Opts opts = new Opts();
    ScannerOpts scanOpts = new ScannerOpts();
    BatchScannerOpts bsOpts = new BatchScannerOpts();
    opts.parseArgs(ContinuousBatchWalker.class.getName(), args, scanOpts, bsOpts);

    Random r = new Random();
    Authorizations auths = opts.randomAuths.getAuths(r);

    Connector conn = opts.getConnector();
    Scanner scanner = ContinuousUtil.createScanner(conn, opts.getTableName(), auths);
    scanner.setBatchSize(scanOpts.scanBatchSize);

    BatchScanner bs = conn.createBatchScanner(opts.getTableName(), auths, bsOpts.scanThreads);
    bs.setTimeout(bsOpts.scanTimeout, TimeUnit.MILLISECONDS);

    while (true) {
      Set<Text> batch = getBatch(scanner, opts.min, opts.max, scanOpts.scanBatchSize, r);
      List<Range> ranges = new ArrayList<Range>(batch.size());

      for (Text row : batch) {
        ranges.add(new Range(row));
      }

      runBatchScan(scanOpts.scanBatchSize, bs, batch, ranges);

      UtilWaitThread.sleep(opts.sleepTime);
    }

  }

  /*
   * private static void runSequentialScan(Scanner scanner, List<Range> ranges) { Set<Text> srowsSeen = new HashSet<Text>(); long st1 =
   * System.currentTimeMillis(); int scount = 0; for (Range range : ranges) { scanner.setRange(range);
   *
   * for (Entry<Key,Value> entry : scanner) { srowsSeen.add(entry.getKey().getRow()); scount++; } }
   *
   *
   * long st2 = System.currentTimeMillis(); System.out.println("SRQ "+(st2 - st1)+" "+srowsSeen.size() +" "+scount); }
   */

  private static void runBatchScan(int batchSize, BatchScanner bs, Set<Text> batch, List<Range> ranges) {
    bs.setRanges(ranges);

    Set<Text> rowsSeen = new HashSet<Text>();

    int count = 0;

    long t1 = System.currentTimeMillis();

    for (Entry<Key,Value> entry : bs) {
      ContinuousWalk.validate(entry.getKey(), entry.getValue());

      rowsSeen.add(entry.getKey().getRow());

      addRow(batchSize, entry.getValue());

      count++;
    }
    bs.close();

    long t2 = System.currentTimeMillis();

    if (!rowsSeen.equals(batch)) {
      HashSet<Text> copy1 = new HashSet<Text>(rowsSeen);
      HashSet<Text> copy2 = new HashSet<Text>(batch);

      copy1.removeAll(batch);
      copy2.removeAll(rowsSeen);

      System.out.printf("DIF %d %d %d%n", t1, copy1.size(), copy2.size());
      System.err.printf("DIF %d %d %d%n", t1, copy1.size(), copy2.size());
      System.err.println("Extra seen : " + copy1);
      System.err.println("Not seen   : " + copy2);
    } else {
      System.out.printf("BRQ %d %d %d %d %d%n", t1, (t2 - t1), rowsSeen.size(), count, (int) (rowsSeen.size() / ((t2 - t1) / 1000.0)));
    }

  }

  private static void addRow(int batchSize, Value v) {
    byte[] val = v.get();

    int offset = ContinuousWalk.getPrevRowOffset(val);
    if (offset > 1) {
      Text prevRow = new Text();
      prevRow.set(val, offset, 16);
      if (rowsToQuery.size() < 3 * batchSize) {
        rowsToQuery.add(prevRow);
      }
    }
  }

  private static HashSet<Text> rowsToQuery = new HashSet<Text>();

  private static Set<Text> getBatch(Scanner scanner, long min, long max, int batchSize, Random r) {

    while (rowsToQuery.size() < batchSize) {
      byte[] scanStart = ContinuousIngest.genRow(min, max, r);
      scanner.setRange(new Range(new Text(scanStart), null));

      int count = 0;

      long t1 = System.currentTimeMillis();

      Iterator<Entry<Key,Value>> iter = scanner.iterator();
      while (iter.hasNext() && rowsToQuery.size() < 3 * batchSize) {
        Entry<Key,Value> entry = iter.next();
        ContinuousWalk.validate(entry.getKey(), entry.getValue());
        addRow(batchSize, entry.getValue());
        count++;
      }

      long t2 = System.currentTimeMillis();

      System.out.println("FSB " + t1 + " " + (t2 - t1) + " " + count);

      UtilWaitThread.sleep(100);
    }

    HashSet<Text> ret = new HashSet<Text>();

    Iterator<Text> iter = rowsToQuery.iterator();

    for (int i = 0; i < batchSize; i++) {
      ret.add(iter.next());
      iter.remove();
    }

    return ret;
  }

}

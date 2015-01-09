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

import static com.google.common.base.Charsets.UTF_8;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.tserver.NativeMap;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class NativeMapConcurrencyTest {

  private static final byte ROW_PREFIX[] = new byte[] {'r'};
  private static final byte COL_PREFIX[] = new byte[] {'c'};

  static Mutation nm(int r) {
    return new Mutation(new Text(FastFormat.toZeroPaddedString(r, 6, 10, ROW_PREFIX)));
  }

  private static final Text ET = new Text();

  private static void pc(Mutation m, int c, Value v) {
    m.put(new Text(FastFormat.toZeroPaddedString(c, 3, 10, COL_PREFIX)), ET, v);
  }

  static NativeMap create(int numRows, int numCols) {

    NativeMap nm = new NativeMap();

    populate(0, numRows, numCols, nm);

    return nm;

  }

  private static void populate(int start, int numRows, int numCols, NativeMap nm) {
    long t1 = System.currentTimeMillis();
    int mc = 1;
    for (int i = 0; i < numRows; i++) {
      Mutation m = nm(i + start);
      for (int j = 0; j < numCols; j++) {
        Value val = new Value("test".getBytes(UTF_8));
        pc(m, j, val);
      }
      nm.mutate(m, mc++);
    }
    long t2 = System.currentTimeMillis();

    System.out.printf("inserted %,d in %,d %,d %,6.2f%n", (numRows * numCols), (t2 - t1), nm.size(), rate((numRows * numCols), (t2 - t1)));
  }

  private static double rate(int num, long ms) {
    return num / (ms / 1000.0);
  }

  static class Opts {
    @Parameter(names = "--rows", description = "rows", required = true)
    int rows = 0;
    @Parameter(names = "--cols", description = "cols")
    int cols = 1;
    @Parameter(names = "--threads", description = "threads")
    int threads = 1;
    @Parameter(names = "--writeThreads", description = "write threads")
    int writeThreads = 1;
    @Parameter(names = "-help", help = true)
    boolean help = false;
  }

  public static void main(String[] args) {
    Opts opts = new Opts();
    JCommander jc = new JCommander(opts);
    jc.setProgramName(NativeMapConcurrencyTest.class.getName());
    jc.parse(args);
    if (opts.help) {
      jc.usage();
      return;
    }
    NativeMap nm = create(opts.rows, opts.cols);
    runTest(nm, opts.rows, opts.cols, opts.threads, opts.writeThreads);
    nm.delete();
  }

  static class ScanTask implements Runnable {

    private NativeMap nm;

    ScanTask(NativeMap nm) {
      this.nm = nm;
    }

    @Override
    public void run() {

      for (int i = 0; i < 10; i++) {

        Iterator<Entry<Key,Value>> iter = nm.iterator();

        long t1 = System.currentTimeMillis();

        int count = 0;

        while (iter.hasNext()) {
          count++;
          iter.next();
        }

        long t2 = System.currentTimeMillis();

        System.out.printf("%d %,d %,d %,d %,d %,6.2f%n", Thread.currentThread().getId(), (t2 - t1), t1, t2, count, rate(count, (t2 - t1)));
      }
    }

  }

  static class WriteTask implements Runnable {

    private int start;
    private int rows;
    private int cols;
    private NativeMap nm;

    WriteTask(int start, int rows, int cols, NativeMap nm) {
      this.start = start;
      this.rows = rows;
      this.cols = cols;
      this.nm = nm;
    }

    @Override
    public void run() {
      populate(start, rows, cols, nm);
    }

  }

  private static void runTest(NativeMap nm, int rows, int cols, int numReadThreads, int writeThreads) {

    Thread threads[] = new Thread[numReadThreads + writeThreads];

    for (int i = 0; i < numReadThreads; i++) {
      threads[i] = new Thread(new ScanTask(nm));
    }

    int start = 0;
    for (int i = numReadThreads; i < writeThreads + numReadThreads; i++) {
      threads[i] = new Thread(new WriteTask(start, rows, cols, nm));
      // start += rows;
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }

}

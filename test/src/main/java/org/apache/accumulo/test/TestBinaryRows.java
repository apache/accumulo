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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.server.cli.ClientOnRequiredTable;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

public class TestBinaryRows {
  private static final long byteOnes;

  static {
    // safely build Byte.SIZE number of 1s as a long; not that I think Byte.SIZE will ever be anything but 8, but just for fun
    long b = 1;
    for (int i = 0; i < Byte.SIZE; ++i)
      b |= (1L << i);
    byteOnes = b;
  }

  static byte[] encodeLong(long l) {
    byte[] ba = new byte[Long.SIZE / Byte.SIZE];

    // parse long into a sequence of bytes
    for (int i = 0; i < ba.length; ++i)
      ba[i] = (byte) (byteOnes & (l >>> (Byte.SIZE * (ba.length - i - 1))));

    return ba;
  }

  static long decodeLong(byte ba[]) {
    // validate byte array
    if (ba.length > Long.SIZE / Byte.SIZE)
      throw new IllegalArgumentException("Byte array of size " + ba.length + " is too big to hold a long");

    // build the long from the bytes
    long l = 0;
    for (int i = 0; i < ba.length; ++i)
      l |= (byteOnes & ba[i]) << (Byte.SIZE * (ba.length - i - 1));

    return l;
  }

  public static class Opts extends ClientOnRequiredTable {
    @Parameter(names = "--mode", description = "either 'ingest', 'delete', 'randomLookups', 'split', 'verify', 'verifyDeleted'", required = true)
    public String mode;
    @Parameter(names = "--start", description = "the lowest numbered row")
    public long start = 0;
    @Parameter(names = "--count", description = "number of rows to ingest", required = true)
    public long num = 0;
  }

  public static void runTest(Connector connector, Opts opts, BatchWriterOpts bwOpts, ScannerOpts scanOpts) throws Exception {

    final Text CF = new Text("cf"), CQ = new Text("cq");
    final byte[] CF_BYTES = "cf".getBytes(UTF_8), CQ_BYTES = "cq".getBytes(UTF_8);
    if (opts.mode.equals("ingest") || opts.mode.equals("delete")) {
      BatchWriter bw = connector.createBatchWriter(opts.getTableName(), bwOpts.getBatchWriterConfig());
      boolean delete = opts.mode.equals("delete");

      for (long i = 0; i < opts.num; i++) {
        byte[] row = encodeLong(i + opts.start);
        String value = "" + (i + opts.start);

        Mutation m = new Mutation(new Text(row));
        if (delete) {
          m.putDelete(CF, CQ);
        } else {
          m.put(CF, CQ, new Value(value.getBytes(UTF_8)));
        }
        bw.addMutation(m);
      }

      bw.close();
    } else if (opts.mode.equals("verifyDeleted")) {
      try (Scanner s = connector.createScanner(opts.getTableName(), opts.auths)) {
        s.setBatchSize(scanOpts.scanBatchSize);
        Key startKey = new Key(encodeLong(opts.start), CF_BYTES, CQ_BYTES, new byte[0], Long.MAX_VALUE);
        Key stopKey = new Key(encodeLong(opts.start + opts.num - 1), CF_BYTES, CQ_BYTES, new byte[0], 0);
        s.setBatchSize(50000);
        s.setRange(new Range(startKey, stopKey));

        for (Entry<Key,Value> entry : s) {
          throw new Exception("ERROR : saw entries in range that should be deleted ( first value : " + entry.getValue().toString() + ")");
        }
      }
    } else if (opts.mode.equals("verify")) {
      long t1 = System.currentTimeMillis();

      try (Scanner s = connector.createScanner(opts.getTableName(), opts.auths)) {
        Key startKey = new Key(encodeLong(opts.start), CF_BYTES, CQ_BYTES, new byte[0], Long.MAX_VALUE);
        Key stopKey = new Key(encodeLong(opts.start + opts.num - 1), CF_BYTES, CQ_BYTES, new byte[0], 0);
        s.setBatchSize(scanOpts.scanBatchSize);
        s.setRange(new Range(startKey, stopKey));

        long i = opts.start;

        for (Entry<Key,Value> e : s) {
          Key k = e.getKey();
          Value v = e.getValue();

          checkKeyValue(i, k, v);

          i++;
        }

        if (i != opts.start + opts.num) {
          throw new Exception("ERROR : did not see expected number of rows, saw " + (i - opts.start) + " expected " + opts.num);
        }

        long t2 = System.currentTimeMillis();

        System.out.printf("time : %9.2f secs%n", ((t2 - t1) / 1000.0));
        System.out.printf("rate : %9.2f entries/sec%n", opts.num / ((t2 - t1) / 1000.0));
      }
    } else if (opts.mode.equals("randomLookups")) {
      int numLookups = 1000;

      Random r = new Random();

      long t1 = System.currentTimeMillis();

      for (int i = 0; i < numLookups; i++) {
        long row = ((r.nextLong() & 0x7fffffffffffffffl) % opts.num) + opts.start;

        try (Scanner s = connector.createScanner(opts.getTableName(), opts.auths)) {
          s.setBatchSize(scanOpts.scanBatchSize);
          Key startKey = new Key(encodeLong(row), CF_BYTES, CQ_BYTES, new byte[0], Long.MAX_VALUE);
          Key stopKey = new Key(encodeLong(row), CF_BYTES, CQ_BYTES, new byte[0], 0);
          s.setRange(new Range(startKey, stopKey));

          Iterator<Entry<Key,Value>> si = s.iterator();

          if (si.hasNext()) {
            Entry<Key,Value> e = si.next();
            Key k = e.getKey();
            Value v = e.getValue();

            checkKeyValue(row, k, v);

            if (si.hasNext()) {
              throw new Exception("ERROR : lookup on " + row + " returned more than one result ");
            }

          } else {
            throw new Exception("ERROR : lookup on " + row + " failed ");
          }
        }
      }

      long t2 = System.currentTimeMillis();

      System.out.printf("time    : %9.2f secs%n", ((t2 - t1) / 1000.0));
      System.out.printf("lookups : %9d keys%n", numLookups);
      System.out.printf("rate    : %9.2f lookups/sec%n", numLookups / ((t2 - t1) / 1000.0));

    } else if (opts.mode.equals("split")) {
      TreeSet<Text> splits = new TreeSet<>();
      int shift = (int) opts.start;
      int count = (int) opts.num;

      for (long i = 0; i < count; i++) {
        long splitPoint = i << shift;

        splits.add(new Text(encodeLong(splitPoint)));
        System.out.printf("added split point 0x%016x  %,12d%n", splitPoint, splitPoint);
      }

      connector.tableOperations().create(opts.getTableName());
      connector.tableOperations().addSplits(opts.getTableName(), splits);

    } else {
      throw new Exception("ERROR : " + opts.mode + " is not a valid operation.");
    }
  }

  private static void checkKeyValue(long expected, Key k, Value v) throws Exception {
    if (expected != decodeLong(TextUtil.getBytes(k.getRow()))) {
      throw new Exception("ERROR : expected row " + expected + " saw " + decodeLong(TextUtil.getBytes(k.getRow())));
    }

    if (!v.toString().equals("" + expected)) {
      throw new Exception("ERROR : expected value " + expected + " saw " + v.toString());
    }
  }

  public static void main(String[] args) {
    Opts opts = new Opts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    ScannerOpts scanOpts = new ScannerOpts();
    opts.parseArgs(TestBinaryRows.class.getName(), args, scanOpts, bwOpts);

    try {
      runTest(opts.getConnector(), opts, bwOpts, scanOpts);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}

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
package org.apache.accumulo.examples.simple.isolation;

import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

/**
 * This example shows how a concurrent reader and writer can interfere with each other. It creates two threads that run forever reading and writing to the same
 * table.
 *
 * When the example is run with isolation enabled, no interference will be observed.
 *
 * When the example is run with out isolation, the reader will see partial mutations of a row.
 *
 */

public class InterferenceTest {

  private static final int NUM_ROWS = 500;
  private static final int NUM_COLUMNS = 113; // scanner batches 1000 by default, so make num columns not a multiple of 10
  private static final Logger log = Logger.getLogger(InterferenceTest.class);

  static class Writer implements Runnable {

    private final BatchWriter bw;
    private final long iterations;

    Writer(BatchWriter bw, long iterations) {
      this.bw = bw;
      this.iterations = iterations;
    }

    @Override
    public void run() {
      int row = 0;
      int value = 0;

      for (long i = 0; i < iterations; i++) {
        Mutation m = new Mutation(new Text(String.format("%03d", row)));
        row = (row + 1) % NUM_ROWS;

        for (int cq = 0; cq < NUM_COLUMNS; cq++)
          m.put(new Text("000"), new Text(String.format("%04d", cq)), new Value(("" + value).getBytes()));

        value++;

        try {
          bw.addMutation(m);
        } catch (MutationsRejectedException e) {
          e.printStackTrace();
          System.exit(-1);
        }
      }
      try {
        bw.close();
      } catch (MutationsRejectedException e) {
        log.error(e, e);
      }
    }
  }

  static class Reader implements Runnable {

    private Scanner scanner;
    volatile boolean stop = false;

    Reader(Scanner scanner) {
      this.scanner = scanner;
    }

    @Override
    public void run() {
      while (!stop) {
        ByteSequence row = null;
        int count = 0;

        // all columns in a row should have the same value,
        // use this hash set to track that
        HashSet<String> values = new HashSet<String>();

        for (Entry<Key,Value> entry : scanner) {
          if (row == null)
            row = entry.getKey().getRowData();

          if (!row.equals(entry.getKey().getRowData())) {
            if (count != NUM_COLUMNS)
              System.err.println("ERROR Did not see " + NUM_COLUMNS + " columns in row " + row);

            if (values.size() > 1)
              System.err.println("ERROR Columns in row " + row + " had multiple values " + values);

            row = entry.getKey().getRowData();
            count = 0;
            values.clear();
          }

          count++;

          values.add(entry.getValue().toString());
        }

        if (count > 0 && count != NUM_COLUMNS)
          System.err.println("ERROR Did not see " + NUM_COLUMNS + " columns in row " + row);

        if (values.size() > 1)
          System.err.println("ERROR Columns in row " + row + " had multiple values " + values);
      }
    }

    public void stopNow() {
      stop = true;
    }
  }

  static class Opts extends ClientOnRequiredTable {
    @Parameter(names = "--iterations", description = "number of times to run", required = true)
    long iterations = 0;
    @Parameter(names = "--isolated", description = "use isolated scans")
    boolean isolated = false;
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(InterferenceTest.class.getName(), args, bwOpts);

    if (opts.iterations < 1)
      opts.iterations = Long.MAX_VALUE;

    Connector conn = opts.getConnector();
    if (!conn.tableOperations().exists(opts.tableName))
      conn.tableOperations().create(opts.tableName);

    Thread writer = new Thread(new Writer(conn.createBatchWriter(opts.tableName, bwOpts.getBatchWriterConfig()), opts.iterations));
    writer.start();
    Reader r;
    if (opts.isolated)
      r = new Reader(new IsolatedScanner(conn.createScanner(opts.tableName, opts.auths)));
    else
      r = new Reader(conn.createScanner(opts.tableName, opts.auths));
    Thread reader;
    reader = new Thread(r);
    reader.start();
    writer.join();
    r.stopNow();
    reader.join();
    System.out.println("finished");
  }
}

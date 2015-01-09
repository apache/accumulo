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

package org.apache.accumulo.examples.simple.client;

import java.util.Map.Entry;

import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;

import com.beust.jcommander.Parameter;

/**
 * A simple example showing how to use the distributed tracing API in client code
 *
 */
public class TracingExample {

  private static final String DEFAULT_TABLE_NAME = "test";

  static class Opts extends ClientOnDefaultTable {
    @Parameter(names = {"-C", "--createtable"}, description = "create table before doing anything")
    boolean createtable = false;
    @Parameter(names = {"-D", "--deletetable"}, description = "delete table when finished")
    boolean deletetable = false;
    @Parameter(names = {"-c", "--create"}, description = "create entries before any deletes")
    boolean createEntries = false;
    @Parameter(names = {"-r", "--read"}, description = "read entries after any creates/deletes")
    boolean readEntries = false;

    public Opts() {
      super(DEFAULT_TABLE_NAME);
      auths = new Authorizations();
    }
  }

  public void enableTracing(Opts opts) throws Exception {
    DistributedTrace.enable(opts.getInstance(), new ZooReader(opts.getInstance().getZooKeepers(), 15000), "myHost", "myApp");
  }

  public void execute(Opts opts) throws TableNotFoundException, InterruptedException, AccumuloException, AccumuloSecurityException, TableExistsException {

    if (opts.createtable) {
      opts.getConnector().tableOperations().create(opts.getTableName());
    }

    if (opts.createEntries) {
      createEntries(opts);
    }

    if (opts.readEntries) {
      readEntries(opts);
    }

    if (opts.deletetable) {
      opts.getConnector().tableOperations().delete(opts.getTableName());
    }
  }

  private void createEntries(Opts opts) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    // Trace the write operation. Note, unless you flush the BatchWriter, you will not capture
    // the write operation as it is occurs asynchronously. You can optionally create additional Spans
    // within a given Trace as seen below around the flush
    Trace.on("Client Write");

    System.out.println("TraceID: " + Long.toHexString(Trace.currentTrace().traceId()));
    BatchWriter batchWriter = opts.getConnector().createBatchWriter(opts.getTableName(), new BatchWriterConfig());

    Mutation m = new Mutation("row");
    m.put("cf", "cq", "value");

    batchWriter.addMutation(m);
    Span flushSpan = Trace.start("Client Flush");
    batchWriter.flush();
    flushSpan.stop();

    // Use Trace.offNoFlush() if you don't want the operation to block.
    batchWriter.close();
    Trace.off();
  }

  private void readEntries(Opts opts) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    Scanner scanner = opts.getConnector().createScanner(opts.getTableName(), opts.auths);

    // Trace the read operation.
    Span readSpan = Trace.on("Client Read");
    System.out.println("TraceID: " + Long.toHexString(Trace.currentTrace().traceId()));

    int numberOfEntriesRead = 0;
    for (Entry<Key,Value> entry : scanner) {
      System.out.println(entry.getKey().toString() + " -> " + entry.getValue().toString());
      ++numberOfEntriesRead;
    }
    // You can add additional metadata (key, values) to Spans which will be able to be viewed in the Monitor
    readSpan.data("Number of Entries Read", String.valueOf(numberOfEntriesRead));

    Trace.off();
  }

  public static void main(String[] args) throws Exception {

    TracingExample tracingExample = new TracingExample();
    Opts opts = new Opts();
    ScannerOpts scannerOpts = new ScannerOpts();
    opts.parseArgs(TracingExample.class.getName(), args, scannerOpts);

    tracingExample.enableTracing(opts);
    tracingExample.execute(opts);
  }

}

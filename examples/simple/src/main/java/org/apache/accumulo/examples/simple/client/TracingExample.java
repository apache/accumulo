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

import static java.nio.charset.StandardCharsets.UTF_8;

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
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * A simple example showing how to use the distributed tracing API in client code
 *
 */
public class TracingExample {
  private static final Logger log = LoggerFactory.getLogger(TracingExample.class);
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
    DistributedTrace.enable("myHost", "myApp");
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
    TraceScope scope = Trace.startSpan("Client Write", Sampler.ALWAYS);

    System.out.println("TraceID: " + Long.toHexString(scope.getSpan().getTraceId()));
    BatchWriter batchWriter = opts.getConnector().createBatchWriter(opts.getTableName(), new BatchWriterConfig());

    Mutation m = new Mutation("row");
    m.put("cf", "cq", "value");

    batchWriter.addMutation(m);
    // You can add timeline annotations to Spans which will be able to be viewed in the Monitor
    scope.getSpan().addTimelineAnnotation("Initiating Flush");
    batchWriter.flush();

    batchWriter.close();
    scope.close();
  }

  private void readEntries(Opts opts) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    Scanner scanner = opts.getConnector().createScanner(opts.getTableName(), opts.auths);

    // Trace the read operation.
    TraceScope readScope = Trace.startSpan("Client Read", Sampler.ALWAYS);
    System.out.println("TraceID: " + Long.toHexString(readScope.getSpan().getTraceId()));

    int numberOfEntriesRead = 0;
    for (Entry<Key,Value> entry : scanner) {
      System.out.println(entry.getKey().toString() + " -> " + entry.getValue().toString());
      ++numberOfEntriesRead;
    }
    // You can add additional metadata (key, values) to Spans which will be able to be viewed in the Monitor
    readScope.getSpan().addKVAnnotation("Number of Entries Read".getBytes(UTF_8), String.valueOf(numberOfEntriesRead).getBytes(UTF_8));

    readScope.close();
  }

  public static void main(String[] args) throws Exception {
    try {
      TracingExample tracingExample = new TracingExample();
      Opts opts = new Opts();
      ScannerOpts scannerOpts = new ScannerOpts();
      opts.parseArgs(TracingExample.class.getName(), args, scannerOpts);

      tracingExample.enableTracing(opts);
      tracingExample.execute(opts);
    } catch (Exception e) {
      log.error("Caught exception running TraceExample", e);
      System.exit(1);
    }
  }

}

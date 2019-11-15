/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tracer;

import static java.lang.Math.min;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.tracer.thrift.RemoteSpan;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

public class TraceDump {
  static final long DEFAULT_TIME_IN_MILLIS = 10 * 60 * 1000L;

  static class Opts extends ClientOpts {
    @Parameter(names = "--table", description = "table to use")
    String tableName = "trace";
    @Parameter(names = {"-r", "--recent"}, description = "List recent traces")
    boolean list = false;
    @Parameter(names = {"-ms", "--ms"}, description = "Time period of recent traces to list in ms")
    long length = DEFAULT_TIME_IN_MILLIS;
    @Parameter(names = {"-d", "--dump"}, description = "Dump all traces")
    boolean dump = false;
    @Parameter(description = " <trace id> { <trace id> ... }")
    List<String> traceIds = new ArrayList<>();
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(TraceDump.class.getName(), args);
    int code = 0;
    if (opts.list) {
      code = listSpans(opts);
    }
    if (code == 0 && opts.dump) {
      code = dumpTrace(opts);
    }
    System.exit(code);
  }

  public static List<RemoteSpan> sortByStart(Collection<RemoteSpan> spans) {
    List<RemoteSpan> spanList = new ArrayList<>(spans);
    Collections.sort(spanList, (o1, o2) -> (int) (o1.start - o2.start));
    return spanList;
  }

  private static int listSpans(Opts opts) throws Exception {
    PrintStream out = System.out;
    long endTime = System.currentTimeMillis();
    long startTime = endTime - opts.length;
    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {
      Scanner scanner = client.createScanner(opts.tableName, opts.auths);
      Range range = new Range(new Text("start:" + Long.toHexString(startTime)),
          new Text("start:" + Long.toHexString(endTime)));
      scanner.setRange(range);
      out.println("Trace            Day/Time                 (ms)  Start");
      for (Entry<Key,Value> entry : scanner) {
        RemoteSpan span = TraceFormatter.getRemoteSpan(entry);
        out.println(String.format("%016x %s %5d %s", span.traceId,
            TraceFormatter.formatDate(new Date(span.getStart())), span.stop - span.start,
            span.description));
      }
    }
    return 0;
  }

  public interface Printer {
    void print(String line);
  }

  private static int dumpTrace(Opts opts) throws Exception {
    final PrintStream out = System.out;
    int count = 0;
    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {
      for (String traceId : opts.traceIds) {
        Scanner scanner = client.createScanner(opts.tableName, opts.auths);
        Range range = new Range(new Text(traceId));
        scanner.setRange(range);
        count = printTrace(scanner, out::println);
      }
    }
    return count > 0 ? 0 : 1;
  }

  public static int printTrace(Scanner scanner, final Printer out) {
    int count = 0;
    SpanTree tree = new SpanTree();
    long start = Long.MAX_VALUE;
    for (Entry<Key,Value> entry : scanner) {
      RemoteSpan span = TraceFormatter.getRemoteSpan(entry);
      tree.addNode(span);
      start = min(start, span.start);
      if (span.getParentIdsSize() == 0)
        count++;
    }
    if (start == Long.MAX_VALUE) {
      out.print("Did not find any traces!");
      return 0;
    }
    out.print(String.format("Trace started at %s", TraceFormatter.formatDate(new Date(start))));
    out.print("Time  Start  Service@Location       Name");

    final long finalStart = start;
    Set<Long> visited = tree.visit(new SpanTreeVisitor() {
      @Override
      public void visit(int level, RemoteSpan node) {
        String fmt = "%5d+%-5d %" + (level * 2 + 1) + "s%s@%s %s";
        out.print(String.format(fmt, node.stop - node.start, node.start - finalStart, "", node.svc,
            node.sender, node.description));
      }
    });
    tree.nodes.keySet().removeAll(visited);
    if (!tree.nodes.isEmpty()) {
      out.print(
          "The following spans are not rooted (probably due to a parent span of length 0ms):");
      for (RemoteSpan span : sortByStart(tree.nodes.values())) {
        String fmt = "%5d+%-5d %" + 1 + "s%s@%s %s";
        out.print(String.format(fmt, span.stop - span.start, span.start - finalStart, "", span.svc,
            span.sender, span.description));
      }
      return -1;
    }
    return count;
  }
}

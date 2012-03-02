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
package org.apache.accumulo.core.trace;

import static java.lang.Math.min;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.cloudtrace.thrift.RemoteSpan;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;


public class TraceDump {
  static final long DEFAULT_TIME_IN_MILLIS = 10 * 60 * 1000l;
  
  private static final Options OPTIONS = new Options();
  public static final Option LIST_SPANS = new Option("l", "list", false, "List recent traces.");
  public static final Option START_TIME = new Option("s", "start", true, "The start time of traces to display");
  public static final Option END_TIME = new Option("e", "end", true, "The end time of traces to display");
  public static final Option DUMP_TRACE = new Option("d", "dump", false, "Dump the traces");
  public static final Option INSTANCE_URL = new Option("i", "instance", true, "URL to point to accumulo.");
  public static final String TRACE_TABLE = "trace";
  
  static {
    for (Option opt : new Option[] {LIST_SPANS, START_TIME, END_TIME, DUMP_TRACE, INSTANCE_URL}) {
      OPTIONS.addOption(opt);
    }
  }
  
  public static void main(String[] args) throws Exception {
    CommandLine commandLine = new BasicParser().parse(OPTIONS, args);
    int code = 0;
    if (code == 0 && commandLine.hasOption(LIST_SPANS.getLongOpt())) {
      code = listSpans(commandLine);
    }
    if (code == 0 && commandLine.hasOption(DUMP_TRACE.getLongOpt())) {
      code = dumpTrace(commandLine);
    }
    System.exit(code);
  }
  
  public static InstanceUserPassword getInstance(CommandLine commandLine) {
    InstanceUserPassword result = null;
    String url = commandLine.getOptionValue(INSTANCE_URL.getOpt(), "zoo://root:secret@localhost/test");
    if (!url.startsWith("zoo://")) {
      throw new IllegalArgumentException("Instance url must start with zoo://");
    }
    String uri = url.substring(6); // root:secret@localhost/test
    String parts[] = uri.split("@", 2); // root:secret, localhost/test
    String userPass = parts[0];
    String zooInstance = parts[1];
    parts = userPass.split(":", 2); // root, secret
    String user = parts[0];
    String password = parts[1];
    parts = zooInstance.split("/", 2); // localhost, test
    String zoo = parts[0];
    String instance = parts[1];
    result = new InstanceUserPassword(new ZooKeeperInstance(instance, zoo), user, password);
    return result;
  }
  
  public static List<RemoteSpan> sortByStart(Collection<RemoteSpan> spans) {
    List<RemoteSpan> spanList = new ArrayList<RemoteSpan>(spans);
    Collections.sort(spanList, new Comparator<RemoteSpan>() {
      @Override
      public int compare(RemoteSpan o1, RemoteSpan o2) {
        return (int) (o1.start - o2.start);
      }
    });
    return spanList;
  }
  
  private static int listSpans(CommandLine commandLine) throws Exception {
    PrintStream out = System.out;
    long endTime = System.currentTimeMillis();
    long startTime = endTime - DEFAULT_TIME_IN_MILLIS;
    InstanceUserPassword info = getInstance(commandLine);
    Connector conn = info.instance.getConnector(info.username, info.password);
    Scanner scanner = conn.createScanner(TRACE_TABLE, conn.securityOperations().getUserAuthorizations(info.username));
    Range range = new Range(new Text("start:" + Long.toHexString(startTime)), new Text("start:" + Long.toHexString(endTime)));
    scanner.setRange(range);
    out.println("Trace            Day/Time                 (ms)  Start");
    for (Entry<Key,Value> entry : scanner) {
      RemoteSpan span = TraceFormatter.getRemoteSpan(entry);
      out.println(String.format("%016x %s %5d %s", span.traceId, TraceFormatter.formatDate(new Date(span.getStart())), span.stop - span.start, span.description));
    }
    return 0;
  }
  
  public interface Printer {
    void print(String line);
  }
  
  private static int dumpTrace(CommandLine commandLine) throws Exception {
    final PrintStream out = System.out;
    InstanceUserPassword info = getInstance(commandLine);
    Connector conn = info.instance.getConnector(info.username, info.password);
    
    int count = 0;
    for (Object arg : commandLine.getArgList()) {
      Scanner scanner = conn.createScanner(TRACE_TABLE, conn.securityOperations().getUserAuthorizations(info.username));
      Range range = new Range(new Text(arg.toString()));
      scanner.setRange(range);
      count = printTrace(scanner, new Printer() {
        @Override
        public void print(String line) {
          out.println(line);
        }
      });
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
      if (span.parentId <= 0)
        count++;
    }
    out.print(String.format("Trace started at %s", TraceFormatter.formatDate(new Date(start))));
    out.print("Time  Start  Service@Location       Name");
    
    final long finalStart = start;
    Set<Long> visited = tree.visit(new SpanTreeVisitor() {
      @Override
      public void visit(int level, RemoteSpan parent, RemoteSpan node, Collection<RemoteSpan> children) {
        String fmt = "%5d+%-5d %" + (level * 2 + 1) + "s%s@%s %s";
        out.print(String.format(fmt, node.stop - node.start, node.start - finalStart, "", node.svc, node.sender, node.description));
      }
    });
    tree.nodes.keySet().removeAll(visited);
    if (!tree.nodes.isEmpty()) {
      out.print("Warning: the following spans are not rooted!");
      for (RemoteSpan span : sortByStart(tree.nodes.values())) {
        out.print(String.format("%s %s %s", Long.toHexString(span.spanId), Long.toHexString(span.parentId), span.description));
      }
    }
    return count;
  }
}

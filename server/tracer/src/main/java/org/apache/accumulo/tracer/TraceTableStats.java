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
package org.apache.accumulo.tracer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.tracer.thrift.RemoteSpan;

/**
 * Reads the trace table and prints out some stats about the spans found.
 */
public class TraceTableStats {
  static class Opts extends ClientOnDefaultTable {
    public Opts() {
      super("trace");
    }
  }

  static class SpanTypeCount {
    String type;
    long nonzeroCount = 0l;
    long zeroCount = 0l;
    ArrayList<Long> log10SpanLength = new ArrayList<>();
    Set<Long> traceIds = new HashSet<>();

    public SpanTypeCount() {
      for (int i = 0; i < 7; i++)
        log10SpanLength.add(0l);
    }

    @Override
    public String toString() {
      return "{" + "type='" + type + '\'' + ", nonzeroCount=" + nonzeroCount + ", zeroCount=" + zeroCount + ", numTraces=" + traceIds.size()
          + ", log10SpanLength=" + log10SpanLength + '}';
    }
  }

  public static void main(String[] args) throws Exception {
    TraceTableStats stats = new TraceTableStats();
    Opts opts = new Opts();
    opts.parseArgs(TraceTableStats.class.getName(), args);
    stats.count(opts);
  }

  public void count(Opts opts) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    Connector conn = opts.getConnector();
    Scanner scanner = conn.createScanner(opts.getTableName(), Authorizations.EMPTY);
    scanner.setRange(new Range(null, true, "idx:", false));
    Map<String,SpanTypeCount> counts = new TreeMap<>();
    final SpanTypeCount hdfs = new SpanTypeCount();
    hdfs.type = "HDFS";
    final SpanTypeCount accumulo = new SpanTypeCount();
    accumulo.type = "Accumulo";
    long numSpans = 0;
    double maxSpanLength = 0;
    double maxSpanLengthMS = 0;
    for (Entry<Key,Value> entry : scanner) {
      numSpans++;
      RemoteSpan span = TraceFormatter.getRemoteSpan(entry);
      String id = span.getSvc() + ":" + span.getDescription().replaceAll("[0-9][0-9][0-9]+", "");
      SpanTypeCount stc = counts.get(id);
      if (stc == null) {
        stc = new SpanTypeCount();
        counts.put(id, stc);
        if (span.description.startsWith("org.apache.hadoop") || span.svc.equals("NameNode") || span.svc.equals("DataNode")
            || span.description.contains("DFSOutputStream") || span.description.contains("DFSInputStream") || span.description.contains("BlockReader")) {
          stc.type = hdfs.type;
        } else {
          stc.type = accumulo.type;
        }
      }
      increment(stc, span);
      if (stc.type.equals(hdfs.type)) {
        increment(hdfs, span);
      } else {
        increment(accumulo, span);
      }
      maxSpanLength = Math.max(maxSpanLength, Math.log10(span.stop - span.start));
      maxSpanLengthMS = Math.max(maxSpanLengthMS, span.stop - span.start);
    }
    System.out.println();
    System.out.println("log10 max span length " + maxSpanLength + " " + maxSpanLengthMS);
    System.out.println("Total spans " + numSpans);
    System.out.println("Percentage Accumulo nonzero of total " + accumulo.nonzeroCount + "/" + numSpans + " = " + (accumulo.nonzeroCount * 1.0 / numSpans));
    System.out.println(hdfs + ", total " + (hdfs.nonzeroCount + hdfs.zeroCount));
    System.out.println(accumulo + ", total " + (accumulo.nonzeroCount + accumulo.zeroCount));
    System.out.println();
    System.out.println("source:desc={stats}");
    for (Entry<String,SpanTypeCount> c : counts.entrySet()) {
      System.out.println(c);
    }
  }

  private static void increment(SpanTypeCount stc, RemoteSpan span) {
    stc.traceIds.add(span.getTraceId());
    if (span.stop == span.start) {
      stc.zeroCount++;
      incrementIndex(stc.log10SpanLength, 0);
    } else {
      stc.nonzeroCount++;
      long ms = span.stop - span.start;
      if (ms <= 10)
        incrementIndex(stc.log10SpanLength, 1);
      else if (ms <= 100)
        incrementIndex(stc.log10SpanLength, 2);
      else if (ms <= 1000)
        incrementIndex(stc.log10SpanLength, 3);
      else if (ms <= 10000)
        incrementIndex(stc.log10SpanLength, 4);
      else if (ms <= 100000)
        incrementIndex(stc.log10SpanLength, 5);
      else if (ms <= 1000000)
        incrementIndex(stc.log10SpanLength, 6);
      else
        throw new IllegalArgumentException("unexpected span length " + ms);
    }
  }

  private static void incrementIndex(ArrayList<Long> hist, int i) {
    hist.set(i, hist.get(i) + 1);
  }
}

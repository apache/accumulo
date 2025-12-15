/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.tracing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Range;

import com.google.gson.FormattingStyle;
import com.google.gson.GsonBuilder;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;

public class ScanTraceClient {

  public static class Options {
    String clientPropsPath;
    String table;
    String startRow;
    String endRow;
    String family;
    String qualifier;

    Options() {}

    Options(String table) {
      this.table = table;
    }

    void conigureScanner(Scanner scanner) {
      if (startRow != null || endRow != null) {
        scanner.setRange(new Range(startRow, true, endRow, false));
      }
      setColumn(scanner);
    }

    void conigureScanner(BatchScanner scanner) {
      if (startRow != null || endRow != null) {
        scanner.setRanges(List.of(new Range(startRow, true, endRow, false)));
      } else {
        scanner.setRanges(List.of(new Range()));
      }
      setColumn(scanner);
    }

    void setColumn(ScannerBase scanner) {
      System.out.println(scanner.getClass().getName() + " fam " + family);
      if (family != null) {
        scanner.fetchColumn(family, qualifier);
      }
    }

  }

  public static class Results {
    // trace id for the batch scan
    String traceId1;
    // trace id for the normal scan
    String traceId2;
    // The number of entries returned by both scans
    long scanCount;
    // The number of bytes returned by both scans
    long scanSize;

    @Override
    public String toString() {
      return "Results{" + "scanCount=" + scanCount + ", traceId1='" + traceId1 + '\''
          + ", traceId2='" + traceId2 + '\'' + ", scanSize=" + scanSize + '}';
    }

  }

  public static void main(String[] args) throws Exception {

    Options opts = new GsonBuilder().create().fromJson(args[0], Options.class);

    String clientPropsPath = opts.clientPropsPath;
    String table = opts.table;

    Tracer tracer = GlobalOpenTelemetry.get().getTracer(ScanTraceClient.class.getName());
    try (var client = Accumulo.newClient().from(clientPropsPath).build()) {
      long scanCount = 0;
      long scanSize = 0;
      long batchScancount = 0;
      long batchScanSize = 0;

      Span span = tracer.spanBuilder("batch-scan").startSpan();
      try (var scanner = client.createBatchScanner(table); var scope = span.makeCurrent()) {
        opts.conigureScanner(scanner);
        for (var entry : scanner) {
          batchScancount++;
          batchScanSize += entry.getKey().getSize() + entry.getValue().getSize();
        }
      } finally {
        span.end();
      }
      var traceId1 = span.getSpanContext().getTraceId();

      // start a second trace
      span = tracer.spanBuilder("seq-scan").startSpan();
      try (var scanner = client.createScanner(table); var scope = span.makeCurrent()) {
        opts.conigureScanner(scanner);
        scanner.setBatchSize(10_000);
        for (var entry : scanner) {
          scanCount++;
          scanSize += entry.getKey().getSize() + entry.getValue().getSize();
        }
      } finally {
        span.end();
      }
      var traceId2 = span.getSpanContext().getTraceId();

      assertEquals(scanCount, batchScancount);
      assertEquals(scanSize, batchScanSize);
      assertNotEquals(traceId1, traceId2);

      Results results = new Results();
      results.traceId1 = traceId1;
      results.traceId2 = traceId2;
      results.scanCount = scanCount;
      results.scanSize = scanSize;

      var gson = new GsonBuilder().setFormattingStyle(FormattingStyle.COMPACT).create();
      System.out.println("RESULT:" + gson.toJson(results));
    }
  }
}

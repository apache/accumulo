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
package org.apache.accumulo.monitor.rest.trace;

import org.apache.accumulo.tracer.thrift.RemoteSpan;

/**
 *
 * Generates a recent trace
 *
 * @since 2.0.0
 *
 */
public class RecentTracesInformation {

  // Variable names become JSON keys
  public String type;
  public Long avg;

  public int total = 0;

  public long min = Long.MAX_VALUE;
  public long max = Long.MIN_VALUE;

  private long totalMS = 0l;
  public long histogram[] = new long[] {0l, 0l, 0l, 0l, 0l, 0l};

  public RecentTracesInformation() {}

  /**
   * Adds the type of the trace
   *
   * @param type
   *          Trace type
   */
  public RecentTracesInformation(String type) {
    this.type = type;
  }

  /**
   * Adds a span for the trace
   *
   * @param span
   *          Remote span to obtain information
   */
  public void addSpan(RemoteSpan span) {
    total++;
    long ms = span.stop - span.start;
    totalMS += ms;
    min = Math.min(min, ms);
    max = Math.max(max, ms);
    int index = 0;
    while (ms >= 10 && index < histogram.length) {
      ms /= 10;
      index++;
    }
    histogram[index]++;

    avg = total != 0 ? totalMS / total : null;
  }
}

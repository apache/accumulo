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
 * Generates a list of traces per type
 *
 * @since 2.0.0
 *
 */
public class TracesForTypeInformation {

  // Variable names become JSON keys
  public String id;
  public String source;

  public Long start;
  public Long ms;

  public TracesForTypeInformation() {}

  /**
   * Generates the trace information based on a span
   *
   * @param span
   *          Remote span with trace information
   */
  public TracesForTypeInformation(RemoteSpan span) {
    this.id = getIDFromSpan(span);
    this.start = getDateFromSpan(span);
    this.ms = getSpanTime(span);
    this.source = getLocation(span);
  }

  protected String getIDFromSpan(RemoteSpan span) {
    if (span == null)
      return null;

    return String.format("%s", Long.toHexString(span.traceId));
  }

  protected Long getDateFromSpan(RemoteSpan span) {
    if (span == null)
      return null;

    return span.start;
  }

  protected Long getSpanTime(RemoteSpan span) {
    if (span == null)
      return null;

    return span.stop - span.start;
  }

  protected String getLocation(RemoteSpan span) {
    if (span == null)
      return null;

    return span.svc + ":" + span.sender;
  }

}

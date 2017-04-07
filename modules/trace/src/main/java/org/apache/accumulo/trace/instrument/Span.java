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
package org.apache.accumulo.trace.instrument;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @deprecated since 1.7, use {@link org.apache.accumulo.core.trace.Span} instead
 */
@Deprecated
public class Span extends org.apache.accumulo.core.trace.Span implements CloudtraceSpan {
  public static final long ROOT_SPAN_ID = org.apache.htrace.Span.ROOT_SPAN_ID;

  public Span(org.apache.accumulo.core.trace.Span span) {
    super(span.getScope());
  }

  public Span(org.apache.htrace.TraceScope scope) {
    super(scope);
  }

  public Span(org.apache.htrace.Span span) {
    super(span);
  }

  @Override
  public Span child(String s) {
    return new Span(span.child(s));
  }

  @Override
  public void start() {
    throw new UnsupportedOperationException("can't start span");
  }

  @Override
  public long getStartTimeMillis() {
    return span.getStartTimeMillis();
  }

  @Override
  public long getStopTimeMillis() {
    return span.getStopTimeMillis();
  }

  @Override
  public long accumulatedMillis() {
    return span.getAccumulatedMillis();
  }

  @Override
  public boolean running() {
    return span.isRunning();
  }

  @Override
  public String description() {
    return span.getDescription();
  }

  @Override
  public long spanId() {
    return span.getSpanId();
  }

  @Override
  public Span parent() {
    throw new UnsupportedOperationException("can't get parent");
  }

  @Override
  public long parentId() {
    return span.getParentId();
  }

  @Override
  public Map<String,String> getData() {
    Map<byte[],byte[]> data = span.getKVAnnotations();
    HashMap<String,String> stringData = new HashMap<>();
    for (Entry<byte[],byte[]> d : data.entrySet()) {
      stringData.put(new String(d.getKey(), UTF_8), new String(d.getValue(), UTF_8));
    }
    return stringData;
  }
}

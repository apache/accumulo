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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.htrace.NullScope;
import org.apache.htrace.TraceScope;

/**
 * This is a wrapper for a TraceScope object, which is a wrapper for a Span and its parent. Not recommended for client use.
 */
public class Span {
  public static final Span NULL_SPAN = new Span(NullScope.INSTANCE);
  private TraceScope scope = null;
  protected org.apache.htrace.Span span = null;

  public Span(TraceScope scope) {
    this.scope = scope;
    this.span = scope.getSpan();
  }

  public Span(org.apache.htrace.Span span) {
    this.span = span;
  }

  public TraceScope getScope() {
    return scope;
  }

  public org.apache.htrace.Span getSpan() {
    return span;
  }

  public long traceId() {
    return span.getTraceId();
  }

  public void data(String k, String v) {
    if (span != null)
      span.addKVAnnotation(k.getBytes(UTF_8), v.getBytes(UTF_8));
  }

  public void stop() {
    if (scope == null) {
      if (span != null) {
        span.stop();
      }
    } else {
      scope.close();
    }
  }
}

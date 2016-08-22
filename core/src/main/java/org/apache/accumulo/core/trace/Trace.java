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

import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.htrace.Sampler;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.wrappers.TraceProxy;
import org.apache.htrace.wrappers.TraceRunnable;

/**
 * Utility class for tracing within Accumulo. Not intended for client use!
 *
 */
public class Trace {
  /**
   * Start a trace span with a given description.
   */
  public static Span on(String description) {
    return on(description, Sampler.ALWAYS);
  }

  /**
   * Start a trace span with a given description with the given sampler.
   */
  public static <T> Span on(String description, Sampler<T> sampler) {
    return new Span(org.apache.htrace.Trace.startSpan(description, sampler));
  }

  /**
   * Finish the current trace.
   */
  public static void off() {
    org.apache.htrace.Span span = org.apache.htrace.Trace.currentSpan();
    if (span != null) {
      span.stop();
      // close() will no-op, but ensure safety if the implementation changes
      org.apache.htrace.Tracer.getInstance().continueSpan(null).close();
    }
  }

  /**
   * @deprecated since 1.7, use {@link #off()} instead
   */
  @Deprecated
  public static void offNoFlush() {
    off();
  }

  /**
   * Returns whether tracing is currently on.
   */
  public static boolean isTracing() {
    return org.apache.htrace.Trace.isTracing();
  }

  /**
   * Return the current span.
   *
   * @deprecated since 1.7 -- it is better to save the span you create in a local variable and call its methods, rather than retrieving the current span
   */
  @Deprecated
  public static Span currentTrace() {
    return new Span(org.apache.htrace.Trace.currentSpan());
  }

  /**
   * Get the trace id of the current span.
   */
  public static long currentTraceId() {
    return org.apache.htrace.Trace.currentSpan().getTraceId();
  }

  /**
   * Start a new span with a given name, if already tracing.
   */
  public static Span start(String description) {
    return new Span(org.apache.htrace.Trace.startSpan(description));
  }

  /**
   * Continue a trace by starting a new span with a given parent and description.
   */
  public static Span trace(TInfo info, String description) {
    if (info.traceId == 0) {
      return Span.NULL_SPAN;
    }
    TraceInfo ti = new TraceInfo(info.traceId, info.parentId);
    return new Span(org.apache.htrace.Trace.startSpan(description, ti));
  }

  /**
   * Add data to the current span.
   */
  public static void data(String k, String v) {
    org.apache.htrace.Span span = org.apache.htrace.Trace.currentSpan();
    if (span != null)
      span.addKVAnnotation(k.getBytes(UTF_8), v.getBytes(UTF_8));
  }

  /**
   * Wrap a runnable in a TraceRunnable, if tracing.
   */
  public static Runnable wrap(Runnable runnable) {
    if (isTracing()) {
      return new TraceRunnable(org.apache.htrace.Trace.currentSpan(), runnable);
    } else {
      return runnable;
    }
  }

  // Wrap all calls to the given object with spans
  public static <T> T wrapAll(T instance) {
    return TraceProxy.trace(instance);
  }

  // Sample trace all calls to the given object
  public static <T,V> T wrapAll(T instance, Sampler<V> dist) {
    return TraceProxy.trace(instance, dist);
  }
}

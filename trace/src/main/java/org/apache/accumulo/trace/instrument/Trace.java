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

/**
 * @deprecated since 1.7, use {@link org.apache.accumulo.core.trace.Trace} instead
 */
@Deprecated
public class Trace extends org.apache.accumulo.core.trace.Trace {
  // Initiate tracing if it isn't already started
  public static Span on(String description) {
    return new Span(org.apache.accumulo.core.trace.Trace.on(description));
  }

  // Turn tracing off:
  public static void off() {
    org.apache.accumulo.core.trace.Trace.off();
  }

  public static void offNoFlush() {
    org.apache.accumulo.core.trace.Trace.offNoFlush();
  }

  // Are we presently tracing?
  public static boolean isTracing() {
    return org.apache.accumulo.core.trace.Trace.isTracing();
  }

  // If we are tracing, return the current span, else null
  public static Span currentTrace() {
    return new Span(org.apache.htrace.Trace.currentSpan());
  }

  // Create a new time span, if tracing is on
  public static Span start(String description) {
    return new Span(org.apache.accumulo.core.trace.Trace.start(description));
  }

  // Start a trace in the current thread from information passed via RPC
  public static Span trace(org.apache.accumulo.trace.thrift.TInfo info, String description) {
    return new Span(org.apache.accumulo.core.trace.Trace.trace(info, description));
  }

  // Initiate a trace in this thread, starting now
  public static Span startThread(Span parent, String description) {
    return new Span(org.apache.htrace.Trace.startSpan(description, parent.getSpan()));
  }

  // Stop a trace in this thread, starting now
  public static void endThread(Span span) {
    if (span != null) {
      span.stop();
      // close() will no-op, but ensure safety if the implementation changes
      org.apache.htrace.Tracer.getInstance().continueSpan(null).close();
    }
  }

  // Wrap the runnable in a new span, if tracing
  public static Runnable wrap(Runnable runnable) {
    return org.apache.accumulo.core.trace.Trace.wrap(runnable);
  }

  // Wrap all calls to the given object with spans
  public static <T> T wrapAll(T instance) {
    return org.apache.accumulo.core.trace.Trace.wrapAll(instance);
  }

  // Sample trace all calls to the given object
  public static <T> T wrapAll(T instance, Sampler dist) {
    return org.apache.accumulo.core.trace.Trace.wrapAll(instance, dist);
  }
}

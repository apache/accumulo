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

import org.apache.accumulo.trace.thrift.TInfo;

/**
 * A Trace allows a user to gather global, distributed, detailed performance information while requesting a service. The general usage for a user is to do
 * something like this:
 *
 * Trace.on("doSomething"); try { doSomething(); } finally { Trace.off(); }
 *
 * This updates the environment for this thread, and data collection will occur whenever the thread encounters any Span notations in the code. The information
 * about the trace will also be carried over RPC calls as well. If the thread should hand off work to another thread, the environment can be carried with it, so
 * that the trace continues on the new thread.
 */
public class Trace {

  // Initiate tracing if it isn't already started
  public static Span on(String description) {
    return Tracer.getInstance().on(description);
  }

  // Turn tracing off:
  public static void off() {
    Tracer.getInstance().stopTracing();
    Tracer.getInstance().flush();
  }

  public static void offNoFlush() {
    Tracer.getInstance().stopTracing();
  }

  // Are we presently tracing?
  public static boolean isTracing() {
    return Tracer.getInstance().isTracing();
  }

  // If we are tracing, return the current span, else null
  public static Span currentTrace() {
    return Tracer.getInstance().currentTrace();
  }

  // Create a new time span, if tracing is on
  public static Span start(String description) {
    return Tracer.getInstance().start(description);
  }

  // Start a trace in the current thread from information passed via RPC
  public static Span trace(TInfo info, String description) {
    if (info.traceId == 0) {
      return Tracer.NULL_SPAN;
    }
    return Tracer.getInstance().continueTrace(description, info.traceId, info.parentId);
  }

  // Initiate a trace in this thread, starting now
  public static Span startThread(Span parent, String description) {
    return Tracer.getInstance().startThread(parent, description);
  }

  // Stop a trace in this thread, starting now
  public static void endThread(Span span) {
    Tracer.getInstance().endThread(span);
  }

  // Wrap the runnable in a new span, if tracing
  public static Runnable wrap(Runnable runnable) {
    if (isTracing())
      return new TraceRunnable(Trace.currentTrace(), runnable);
    return runnable;
  }

  // Wrap all calls to the given object with spans
  public static <T> T wrapAll(T instance) {
    return TraceProxy.trace(instance);
  }

  // Sample trace all calls to the given object
  public static <T> T wrapAll(T instance, Sampler dist) {
    return TraceProxy.trace(instance, dist);
  }
}

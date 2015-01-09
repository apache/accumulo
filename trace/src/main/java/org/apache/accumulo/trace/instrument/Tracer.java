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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.accumulo.trace.instrument.impl.NullSpan;
import org.apache.accumulo.trace.instrument.impl.RootMilliSpan;
import org.apache.accumulo.trace.instrument.receivers.SpanReceiver;
import org.apache.accumulo.trace.thrift.TInfo;

/**
 * A Tracer provides the implementation for collecting and distributing Spans within a process.
 */
public class Tracer {
  private final static Random random = new SecureRandom();
  private final List<SpanReceiver> receivers = new ArrayList<SpanReceiver>();

  private static final ThreadLocal<Span> currentTrace = new ThreadLocal<Span>();
  public static final NullSpan NULL_SPAN = new NullSpan();
  private static final TInfo dontTrace = new TInfo(0, 0);

  private static Tracer instance = null;

  synchronized public static void setInstance(Tracer tracer) {
    instance = tracer;
  }

  synchronized public static Tracer getInstance() {
    if (instance == null) {
      instance = new Tracer();
    }
    return instance;
  }

  public static TInfo traceInfo() {
    Span span = currentTrace.get();
    if (span != null) {
      return new TInfo(span.traceId(), span.spanId());
    }
    return dontTrace;
  }

  public Span start(String description) {
    Span parent = currentTrace.get();
    if (parent == null)
      return NULL_SPAN;
    return push(parent.child(description));
  }

  public Span on(String description) {
    Span parent = currentTrace.get();
    Span root;
    if (parent == null) {
      root = new RootMilliSpan(description, random.nextLong(), random.nextLong(), Span.ROOT_SPAN_ID);
    } else {
      root = parent.child(description);
    }
    return push(root);
  }

  public Span startThread(Span parent, String activity) {
    return push(parent.child(activity));
  }

  public void endThread(Span span) {
    if (span != null) {
      span.stop();
      currentTrace.set(null);
    }
  }

  public boolean isTracing() {
    return currentTrace.get() != null;
  }

  public Span currentTrace() {
    return currentTrace.get();
  }

  public void stopTracing() {
    endThread(currentTrace());
  }

  protected void deliver(Span span) {
    for (SpanReceiver receiver : receivers) {
      receiver.span(span.traceId(), span.spanId(), span.parentId(), span.getStartTimeMillis(), span.getStopTimeMillis(), span.description(), span.getData());

    }
  }

  public synchronized void addReceiver(SpanReceiver receiver) {
    receivers.add(receiver);
  }

  public synchronized void removeReceiver(SpanReceiver receiver) {
    receivers.remove(receiver);
  }

  public Span push(Span span) {
    if (span != null) {
      currentTrace.set(span);
      span.start();
    }
    return span;
  }

  public void pop(Span span) {
    if (span != null) {
      deliver(span);
      currentTrace.set(span.parent());
    } else
      currentTrace.set(null);
  }

  public Span continueTrace(String description, long traceId, long parentId) {
    return push(new RootMilliSpan(description, traceId, random.nextLong(), parentId));
  }

  public void flush() {
    for (SpanReceiver receiver : receivers) {
      receiver.flush();
    }
  }

}

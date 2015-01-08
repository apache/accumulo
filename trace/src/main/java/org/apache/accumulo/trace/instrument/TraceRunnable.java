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
 * Wrap a Runnable with a Span that survives a change in threads.
 *
 */
public class TraceRunnable implements Runnable, Comparable<TraceRunnable> {

  private final Span parent;
  private final Runnable runnable;

  public TraceRunnable(Runnable runnable) {
    this(Trace.currentTrace(), runnable);
  }

  public TraceRunnable(Span parent, Runnable runnable) {
    this.parent = parent;
    this.runnable = runnable;
  }

  @Override
  public void run() {
    if (parent != null) {
      Span chunk = Trace.startThread(parent, Thread.currentThread().getName());
      try {
        runnable.run();
      } finally {
        Trace.endThread(chunk);
      }
    } else {
      runnable.run();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TraceRunnable) {
      return 0 == this.compareTo((TraceRunnable) o);
    }

    return false;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public int compareTo(TraceRunnable o) {
    return ((Comparable) this.runnable).compareTo(o.runnable);
  }
}

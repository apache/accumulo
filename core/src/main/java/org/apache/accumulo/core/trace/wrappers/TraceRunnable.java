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
package org.apache.accumulo.core.trace.wrappers;

import org.htrace.Span;
import org.htrace.Trace;
import org.htrace.TraceScope;

/**
 * Wrap a Runnable with a Span that survives a change in threads.
 * 
 */
public class TraceRunnable implements Runnable, Comparable<TraceRunnable> {
  
  private final Span parent;
  private final Runnable runnable;
  private final String description;
  
  public TraceRunnable(Runnable runnable) {
    this(Trace.currentSpan(), runnable);
  }
  
  public TraceRunnable(Span parent, Runnable runnable) {
    this(parent, runnable, null);
  }

  public TraceRunnable(Span parent, Runnable runnable, String description) {
    this.parent = parent;
    this.runnable = runnable;
    this.description = description;
  }
  
  @Override
  public void run() {
    if (parent != null) {
      TraceScope chunk = Trace.startSpan(getDescription(), parent);
      try {
        runnable.run();
      } finally {
        TraceExecutorService.endThread(chunk.getSpan());
      }
    } else {
      runnable.run();
    }
  }

  private String getDescription() {
    return this.description == null ? Thread.currentThread().getName() : description;
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

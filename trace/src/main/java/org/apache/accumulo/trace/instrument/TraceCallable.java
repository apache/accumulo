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

import org.htrace.Span;
import org.htrace.Trace;
import org.htrace.TraceScope;

import java.util.concurrent.Callable;

/**
 * Wrap a Callable with a Span that survives a change in threads.
 * 
 */
public class TraceCallable<V> implements Callable<V> {
  private final Callable<V> impl;
  private final Span parent;
  private final String description;
  
  TraceCallable(Callable<V> impl) {
    this(Trace.currentSpan(), impl);
  }
  
  TraceCallable(Span parent, Callable<V> impl) {
    this(parent, impl, null);
  }

  TraceCallable(Span parent, Callable<V> impl, String description) {
    this.impl = impl;
    this.parent = parent;
    this.description = description;
  }
  
  @Override
  public V call() throws Exception {
    if (parent != null) {
      TraceScope chunk = Trace.startSpan(getDescription(), parent);
      try {
        return impl.call();
      } finally {
        TraceExecutorService.endThread(chunk.getSpan());
      }
    } else {
      return impl.call();
    }
  }

  public Callable<V> getImpl() {
    return impl;
  }

  private String getDescription() {
    return this.description == null ? Thread.currentThread().getName() : description;
  }
}

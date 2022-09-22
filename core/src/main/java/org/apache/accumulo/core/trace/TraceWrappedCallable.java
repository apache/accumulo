/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.trace;

import java.util.Objects;
import java.util.concurrent.Callable;

import org.apache.accumulo.core.util.threads.ThreadPools;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

/**
 * A class to wrap {@link Callable}s for {@link ThreadPools} in a way that still provides access to
 * the wrapped {@link Callable} instance. This supersedes the use of {@link Context#wrap(Callable)}.
 */
class TraceWrappedCallable<V> implements Callable<V> {

  private final Context context;
  private final Callable<V> unwrapped;

  static <C> Callable<C> unwrapFully(Callable<C> c) {
    while (c instanceof TraceWrappedCallable) {
      c = ((TraceWrappedCallable<C>) c).unwrapped;
    }
    return c;
  }

  TraceWrappedCallable(Callable<V> other) {
    this.context = Context.current();
    this.unwrapped = unwrapFully(other);
  }

  @Override
  public V call() throws Exception {
    try (Scope unused = context.makeCurrent()) {
      return unwrapped.call();
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof TraceWrappedCallable) {
      return Objects.equals(unwrapped, ((TraceWrappedCallable<?>) obj).unwrapped);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return unwrapped.hashCode();
  }

}

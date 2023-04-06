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

import org.apache.accumulo.core.util.threads.ThreadPools;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

/**
 * A class to wrap {@link Runnable}s for {@link ThreadPools} in a way that still provides access to
 * the wrapped {@link Runnable} instance. This supersedes the use of {@link Context#wrap(Runnable)}.
 */
class TraceWrappedRunnable implements Runnable {

  private final Context context;
  private final Runnable unwrapped;

  static Runnable unwrapFully(Runnable r) {
    while (r instanceof TraceWrappedRunnable) {
      r = ((TraceWrappedRunnable) r).unwrapped;
    }
    return r;
  }

  TraceWrappedRunnable(Runnable other) {
    this.context = Context.current();
    this.unwrapped = unwrapFully(other);
  }

  @Override
  public void run() {
    try (Scope unused = context.makeCurrent()) {
      unwrapped.run();
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof TraceWrappedRunnable) {
      return Objects.equals(unwrapped, ((TraceWrappedRunnable) obj).unwrapped);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return unwrapped.hashCode();
  }

}

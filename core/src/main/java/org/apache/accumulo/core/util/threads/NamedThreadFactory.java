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
package org.apache.accumulo.core.util.threads;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.OptionalInt;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ThreadFactory that sets the name and optionally the priority on a newly created Thread.
 */
class NamedThreadFactory implements ThreadFactory {

  private static final String FORMAT = "%s-%s-%d";

  private final AtomicInteger threadNum = new AtomicInteger(1);
  private final String name;
  private final OptionalInt priority;
  private final UncaughtExceptionHandler handler;

  NamedThreadFactory(String name, UncaughtExceptionHandler ueh) {
    this(name, OptionalInt.empty(), ueh);
  }

  NamedThreadFactory(String name, OptionalInt priority, UncaughtExceptionHandler ueh) {
    this.name = name;
    this.priority = priority;
    this.handler = ueh;
  }

  @Override
  public Thread newThread(Runnable r) {
    String threadName = null;
    if (r instanceof NamedRunnable) {
      NamedRunnable nr = (NamedRunnable) r;
      threadName = String.format(FORMAT, name, nr.getName(), threadNum.getAndIncrement());
    } else {
      threadName =
          String.format(FORMAT, name, r.getClass().getSimpleName(), threadNum.getAndIncrement());
    }
    return Threads.createThread(threadName, priority, r, handler);
  }
}

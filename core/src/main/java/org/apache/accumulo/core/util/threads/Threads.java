/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

public class Threads {

  public static Runnable createNamedRunnable(String name, Runnable r) {
    return new NamedRunnable(name, r);
  }

  public static Runnable createNamedRunnable(String name, OptionalInt priority, Runnable r) {
    return new NamedRunnable(name, priority, r);
  }

  private static final UncaughtExceptionHandler UEH = new AccumuloUncaughtExceptionHandler();

  public static Thread createThread(String name, Runnable r) {
    return createThread(name, OptionalInt.empty(), r);
  }

  public static Thread createThread(String name, OptionalInt priority, Runnable r) {
    Thread thread = null;
    if (r instanceof NamedRunnable) {
      NamedRunnable nr = (NamedRunnable) r;
      thread = new Thread(r, name);
      if (nr.getPriority().isPresent()) {
        thread.setPriority(nr.getPriority().getAsInt());
      } else if (priority.isPresent()) {
        thread.setPriority(priority.getAsInt());
      }
    } else {
      thread = new Thread(r, name);
      if (priority.isPresent()) {
        thread.setPriority(priority.getAsInt());
      }
    }
    thread.setDaemon(true);
    thread.setUncaughtExceptionHandler(UEH);
    return thread;
  }
}

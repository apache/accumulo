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
package org.apache.accumulo.core.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.OptionalInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Threads {

  /**
   * Runnable implementation that has a name and priority. Used by the NamedThreadFactory when
   * creating new Threads
   */
  public static class NamedRunnable implements Runnable {

    private final String name;
    private final OptionalInt priority;
    private final Runnable r;

    private NamedRunnable(String name, Runnable r) {
      this(name, OptionalInt.empty(), r);
    }

    private NamedRunnable(String name, OptionalInt priority, Runnable r) {
      this.name = name;
      this.priority = priority;
      this.r = r;
    }

    public String getName() {
      return name;
    }

    public OptionalInt getPriority() {
      return priority;
    }

    public void run() {
      r.run();
    }

  }

  public static Runnable createNamedRunnable(String name, Runnable r) {
    return new NamedRunnable(name, r);
  }

  public static Runnable createNamedRunnable(String name, OptionalInt priority, Runnable r) {
    return new NamedRunnable(name, priority, r);
  }

  /**
   * UncaughtExceptionHandler that logs all Exceptions and Errors thrown from a Thread. If an Error
   * is thrown, halt the JVM.
   *
   */
  private static class AccumuloUncaughtExceptionHandler implements UncaughtExceptionHandler {

    private static final Logger LOG =
        LoggerFactory.getLogger(AccumuloUncaughtExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      if (e instanceof Exception) {
        LOG.error("Caught an Exception in {}. Thread is dead.", t, e);
      } else if (e instanceof Error) {
        try {
          e.printStackTrace();
          System.err.println("Error thrown in thread: " + t + ", halting VM.");
        } catch (Throwable e1) {
          // If e == OutOfMemoryError, then it's probably that another Error might be
          // thrown when trying to print to System.err.
        } finally {
          Runtime.getRuntime().halt(-1);
        }
      }
    }
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

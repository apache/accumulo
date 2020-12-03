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

import java.io.IOError;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Date;
import java.util.OptionalInt;
import java.util.ServiceConfigurationError;

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
   * UncaughtExceptionHandler that logs all Exceptions and Errors thrown from a Thread. If the
   * system property HaltVMOnThreadError is set to true, then this object will also stop the JVM
   * when specific types of Error are thrown.
   *
   */
  private static class AccumuloUncaughtExceptionHandler implements UncaughtExceptionHandler {

    private static final Logger LOG =
        LoggerFactory.getLogger(AccumuloUncaughtExceptionHandler.class);
    private static final String HALT_PROPERTY = "HaltVMOnThreadError";

    private void logError(String msg, Thread t, Throwable e) {
      try {
        LOG.error(msg, t, e);
      } catch (Exception e1) {
        // maybe the logging system is screwed up OR there is a bug in the exception, like
        // t.getMessage() throws a NPE
        System.err.println(
            "ERROR " + new Date() + " Failed to log message about thread death " + e1.getMessage());
        e1.printStackTrace();

        // try to print original exception
        System.err
            .println("ERROR " + new Date() + " Exception that failed to log : " + e.getMessage());
        e.printStackTrace();

      }
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      if (e instanceof Exception) {
        logError("Caught an Exception in {}. Thread is dead.", t, e);
      } else if (e instanceof VirtualMachineError || e instanceof LinkageError
          || e instanceof ThreadDeath || e instanceof IOError
          || e instanceof ServiceConfigurationError) {
        if (System.getProperty(HALT_PROPERTY, "false").equals("true")) {
          logError("Caught an Error in {}. Halting VM.", t, e);
          Halt.halt(String.format("Caught an exception in %s. Halting VM, check the logs.", t));
        } else {
          logError("Caught an Error in {}. Thread is dead.", t, e);
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

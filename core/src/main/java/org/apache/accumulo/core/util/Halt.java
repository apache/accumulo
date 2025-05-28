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
package org.apache.accumulo.core.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import org.apache.accumulo.core.util.threads.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Halt {
  private static final Logger log = LoggerFactory.getLogger(Halt.class);

  public static void halt(final int status, final String msg) {
    halt(status, msg, null, null);
  }

  public static void halt(final int status, final String msg, final Throwable exception) {
    halt(status, msg, exception, null);
  }

  public static void halt(final int status, final String msg, final Runnable runnable) {
    halt(status, msg, null, runnable);
  }

  public static void halt(final int status, final String msg, final Throwable exception,
      final Runnable runnable) {
    try {
      final String errorMessage = "FATAL " + msg;

      // Printing to stderr and to the log in case the message does not make
      // it to the log. This could happen if an asynchronous logging impl is used
      System.err.println(errorMessage);
      if (exception != null) {
        exception.printStackTrace();
      }
      System.err.flush();

      if (exception != null) {
        log.error(errorMessage, exception);
      } else {
        log.error(errorMessage);
      }
      // give ourselves a little time to try and do something
      Threads.createThread("Halt Thread", () -> {
        sleepUninterruptibly(100, MILLISECONDS);
        Runtime.getRuntime().halt(status);
      }).start();

      if (runnable != null) {
        runnable.run();
      }
      Runtime.getRuntime().halt(status);
    } finally {
      // In case something else decides to throw a Runtime exception
      Runtime.getRuntime().halt(-1);
    }
  }

}

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

  public static void halt(final String msg) {
    // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
    halt(0, new Runnable() {
      @Override
      public void run() {
        log.error("FATAL {}", msg);
      }
    });
  }

  public static void halt(final String msg, int status) {
    halt(status, new Runnable() {
      @Override
      public void run() {
        log.error("FATAL {}", msg);
      }
    });
  }

  public static void halt(final int status, Runnable runnable) {

    try {
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

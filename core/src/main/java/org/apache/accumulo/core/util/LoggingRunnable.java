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
package org.apache.accumulo.core.util;

import java.util.Date;

import org.apache.log4j.Logger;

public class LoggingRunnable implements Runnable {
  private Runnable runnable;
  private Logger log;

  public LoggingRunnable(Logger log, Runnable r) {
    this.runnable = r;
    this.log = log;
  }

  public void run() {
    try {
      runnable.run();
    } catch (Throwable t) {
      try {
        log.error("Thread \"" + Thread.currentThread().getName() + "\" died " + t.getMessage(), t);
      } catch (Throwable t2) {
        // maybe the logging system is screwed up OR there is a bug in the exception, like t.getMessage() throws a NPE
        System.err.println("ERROR " + new Date() + " Failed to log message about thread death " + t2.getMessage());
        t2.printStackTrace();

        // try to print original exception
        System.err.println("ERROR " + new Date() + " Exception that failed to log : " + t.getMessage());
        t.printStackTrace();
      }
    }
  }
}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UncaughtExceptionHandler that logs all Exceptions and Errors thrown from a Thread. If an Error is
 * thrown, halt the JVM.
 *
 */
class AccumuloUncaughtExceptionHandler implements UncaughtExceptionHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AccumuloUncaughtExceptionHandler.class);

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

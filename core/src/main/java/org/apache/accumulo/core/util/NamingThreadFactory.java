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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.fate.util.LoggingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamingThreadFactory implements ThreadFactory {
  private static final Logger log = LoggerFactory.getLogger(NamingThreadFactory.class);

  private static final UncaughtExceptionHandler UEH = new AccumuloUncaughtExceptionHandler();

  private AtomicInteger threadNum = new AtomicInteger(1);
  private String name;
  private OptionalInt priority;

  public NamingThreadFactory(String name) {
    this.name = name;
    this.priority = OptionalInt.empty();
  }

  public NamingThreadFactory(String name, OptionalInt priority) {
    this.name = name;
    this.priority = priority;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread thread =
        new Daemon(new LoggingRunnable(log, r), name + " " + threadNum.getAndIncrement());
    thread.setUncaughtExceptionHandler(UEH);
    if (priority.isPresent()) {
      thread.setPriority(priority.getAsInt());
    }
    return thread;
  }

}

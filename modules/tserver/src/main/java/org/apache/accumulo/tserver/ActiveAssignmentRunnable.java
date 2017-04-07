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
package org.apache.accumulo.tserver;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.data.impl.KeyExtent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ActiveAssignmentRunnable implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(ActiveAssignmentRunnable.class);

  private final ConcurrentHashMap<KeyExtent,RunnableStartedAt> activeAssignments;
  private final KeyExtent extent;
  private final Runnable delegate;

  // Make sure that the other thread calling getException will see the assignment by the thread calling run()
  private volatile Thread executingThread;

  public ActiveAssignmentRunnable(ConcurrentHashMap<KeyExtent,RunnableStartedAt> activeAssignments, KeyExtent extent, Runnable delegate) {
    requireNonNull(activeAssignments);
    requireNonNull(extent);
    requireNonNull(delegate);
    this.activeAssignments = activeAssignments;
    this.extent = extent;
    this.delegate = delegate;
  }

  @Override
  public void run() {
    if (activeAssignments.containsKey(extent)) {
      throw new IllegalStateException("Active assignment already exists for " + extent);
    }

    executingThread = Thread.currentThread();

    try {
      RunnableStartedAt runnableWithStartTime = new RunnableStartedAt(this, System.currentTimeMillis());
      log.trace("Started assignment for {} at {}", extent, runnableWithStartTime.getStartTime());
      activeAssignments.put(extent, runnableWithStartTime);
      delegate.run();
    } finally {
      if (log.isTraceEnabled()) {
        // Avoid the call to currentTimeMillis if we'd just throw it away anyways
        log.trace("Finished assignment for {} at {}", extent, System.currentTimeMillis());
      }
      activeAssignments.remove(extent);
    }
  }

  public Exception getException() {
    final Exception e = new Exception("Assignment of " + extent);
    if (null != executingThread) {
      e.setStackTrace(executingThread.getStackTrace());
    }
    return e;
  }
}

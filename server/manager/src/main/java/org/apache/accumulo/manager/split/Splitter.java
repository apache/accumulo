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
package org.apache.accumulo.manager.split;

import java.util.concurrent.*;

import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.manager.TabletOperations;
import org.apache.accumulo.server.ServerContext;

import com.google.common.base.Preconditions;

public class Splitter {

  private final ServerContext context;
  private final Ample.DataLevel level;

  private final ExecutorService splitExecutor;

  private final ScheduledExecutorService scanExecutor;
  private final TabletOperations tabletOps;
  private ScheduledFuture<?> scanFuture;

  public Splitter(ServerContext context, Ample.DataLevel level, TabletOperations tabletOps) {
    this.context = context;
    this.level = level;
    this.tabletOps = tabletOps;
    this.splitExecutor = Executors.newFixedThreadPool(5);
    this.scanExecutor = Executors.newScheduledThreadPool(1);
  }

  public synchronized void start() {
    Preconditions.checkState(scanFuture == null);
    Preconditions.checkState(!scanExecutor.isShutdown());
    scanFuture = scanExecutor.scheduleWithFixedDelay(
        new SplitScanner(context, splitExecutor, level, tabletOps), 1, 10, TimeUnit.SECONDS);
  }

  public synchronized void stop() {
    scanFuture.cancel(true);
    scanExecutor.shutdownNow();
    splitExecutor.shutdownNow();
  }
}

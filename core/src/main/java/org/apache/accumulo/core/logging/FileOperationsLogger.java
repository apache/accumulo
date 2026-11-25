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
package org.apache.accumulo.core.logging;

import java.time.Duration;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.StorageStatistics.LongStatistic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileOperationsLogger {

  private static final Logger LOG = LoggerFactory.getLogger(Logging.PREFIX + "file_ops");

  private static final Thread FS_MONITOR_THREAD = new Thread(() -> {
    while (true) {
      logFilesystemMetrics();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.info("FS Ops Logging thread interrupted", e);
        return;
      }
    }
  }, "fs-ops-logger");

  public static boolean isEnabled() {
    return LOG.isTraceEnabled();
  }

  public static void seekCalled(long position, Duration duration) {
    if (!isEnabled()) {
      return;
    }
    LOG.debug("BCFile: seek to position {}, took {} ns", position, duration.toNanos());
  }

  public static void readCalled(int numBytes, Duration duration) {
    if (!isEnabled()) {
      return;
    }
    LOG.debug("BCFile: read {} bytes, took {} ns", numBytes, duration.toNanos());
  }

  public static synchronized void startLoggingFilesystemMetrics() {
    if (FS_MONITOR_THREAD.isAlive()) {
      return;
    }
    FS_MONITOR_THREAD.setDaemon(true);
    FS_MONITOR_THREAD.start();
  }

  private static void logFilesystemMetrics() {
    if (!isEnabled()) {
      return;
    }
    StorageStatistics stats = FileSystem.getGlobalStorageStatistics().get("DFSOpsCountStatistics");
    if (stats != null) {
      Iterator<LongStatistic> ops = stats.getLongStatistics();
      while (ops.hasNext()) {
        LongStatistic op = ops.next();
        LOG.debug("Global DFS Metric {} has value {}", op.getName(), op.getValue());
      }
    }
  }
}

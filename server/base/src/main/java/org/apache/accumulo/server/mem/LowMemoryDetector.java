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
package org.apache.accumulo.server.mem;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LowMemoryDetector {

  private static final Logger LOG = LoggerFactory.getLogger(LowMemoryDetector.class);

  @FunctionalInterface
  public static interface Action {
    void execute();
  }

  public enum DetectionScope {
    MINC, MAJC, SCAN
  };

  private static final Logger log = LoggerFactory.getLogger(LowMemoryDetector.class);

  private long lastMemorySize = 0;
  private int lowMemCount = 0;
  private long lastMemoryCheckTime = 0;
  private final Lock memCheckTimeLock = new ReentrantLock();
  private volatile boolean runningLowOnMemory = false;

  public long getIntervalMillis(AccumuloConfiguration conf) {
    return conf.getTimeInMillis(Property.GENERAL_LOW_MEM_DETECTOR_INTERVAL);
  }

  public boolean isRunningLowOnMemory() {
    return runningLowOnMemory;
  }

  /**
   * @param context server context
   * @param scope whether this is being checked in the context of scan or compact code
   * @param isUserTable boolean as to whether the table being scanned / compacted is a user table.
   *        No action is taken for system tables.
   * @param action Action to perform when this method returns true
   * @return true if server running low on memory
   */
  public boolean isRunningLowOnMemory(ServerContext context, DetectionScope scope,
      Supplier<Boolean> isUserTable, Action action) {
    if (isUserTable.get()) {
      Property p = null;
      switch (scope) {
        case SCAN:
          p = Property.GENERAL_LOW_MEM_SCAN_PROTECTION;
          break;
        case MINC:
          p = Property.GENERAL_LOW_MEM_MINC_PROTECTION;
          break;
        case MAJC:
          p = Property.GENERAL_LOW_MEM_MAJC_PROTECTION;
          break;
        default:
          throw new IllegalArgumentException("Unknown scope: " + scope);
      }
      boolean isEnabled = context.getConfiguration().getBoolean(p);
      // Only incur the penalty of accessing the volatile variable when enabled for this scope
      if (isEnabled && runningLowOnMemory) {
        action.execute();
        return true;
      }
    }
    return false;
  }

  public void logGCInfo(AccumuloConfiguration conf) {

    Double freeMemoryPercentage = conf.getFraction(Property.GENERAL_LOW_MEM_DETECTOR_THRESHOLD);

    memCheckTimeLock.lock();
    try {
      final long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

      Runtime rt = Runtime.getRuntime();

      StringBuilder sb = new StringBuilder("gc");

      boolean sawChange = false;

      final long maxConfiguredMemory = rt.maxMemory();
      final long allocatedMemory = rt.totalMemory();
      final long allocatedFreeMemory = rt.freeMemory();
      final long freeMemory = maxConfiguredMemory - (allocatedMemory - allocatedFreeMemory);
      final double lowMemoryThreshold = maxConfiguredMemory * freeMemoryPercentage;
      log.trace("Memory info: max={}, allocated={}, free={}, free threshold={}",
          maxConfiguredMemory, allocatedMemory, freeMemory, (long) lowMemoryThreshold);

      if (freeMemory < (long) lowMemoryThreshold) {
        lowMemCount++;
        if (lowMemCount > 3 && !runningLowOnMemory) {
          runningLowOnMemory = true;
          log.info("Running low on memory: max={}, allocated={}, free={}, free threshold={}",
              maxConfiguredMemory, allocatedMemory, freeMemory, (long) lowMemoryThreshold);
        }
      } else {
        // If we were running low on memory, but are not any longer, than log at warn
        // so that it shows up in the logs
        if (runningLowOnMemory) {
          log.warn("Recovered from low memory condition");
        } else {
          log.trace("Not running low on memory");
        }
        runningLowOnMemory = false;
        lowMemCount = 0;
      }

      if (freeMemory != lastMemorySize) {
        sawChange = true;
      }

      String sign = "+";
      if (freeMemory - lastMemorySize <= 0) {
        sign = "";
      }

      sb.append(String.format(" freemem=%,d(%s%,d) totalmem=%,d", freeMemory, sign,
          (freeMemory - lastMemorySize), rt.totalMemory()));

      if (sawChange) {
        log.debug(sb.toString());
      }

      final long keepAliveTimeout = conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
      if (lastMemoryCheckTime > 0 && lastMemoryCheckTime < now) {
        final long diff = now - lastMemoryCheckTime;
        if (diff > keepAliveTimeout + 1000) {
          log.warn(String.format(
              "GC pause checker not called in a timely"
                  + " fashion. Expected every %.1f seconds but was %.1f seconds since last check",
              keepAliveTimeout / 1000., diff / 1000.));
        }
        lastMemoryCheckTime = now;
        return;
      }

      // if (maxIncreaseInCollectionTime > keepAliveTimeout) {
      // Halt.halt("Garbage collection may be interfering with lock keep-alive. Halting.", -1);
      // }

      lastMemorySize = freeMemory;
      lastMemoryCheckTime = now;
    } finally {
      memCheckTimeLock.unlock();
    }
  }

}

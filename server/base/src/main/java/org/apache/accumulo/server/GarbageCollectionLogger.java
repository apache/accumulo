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
package org.apache.accumulo.server;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.util.Halt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GarbageCollectionLogger {
  private static final Logger log = LoggerFactory.getLogger(GarbageCollectionLogger.class);

  private final HashMap<String,Long> prevGcTime = new HashMap<>();
  private long lastMemorySize = 0;
  private long gcTimeIncreasedCount = 0;
  private static long lastMemoryCheckTime = 0;

  public GarbageCollectionLogger() {}

  public synchronized void logGCInfo(AccumuloConfiguration conf) {
    final long now = System.currentTimeMillis();

    List<GarbageCollectorMXBean> gcmBeans = ManagementFactory.getGarbageCollectorMXBeans();
    Runtime rt = Runtime.getRuntime();

    StringBuilder sb = new StringBuilder("gc");

    boolean sawChange = false;

    long maxIncreaseInCollectionTime = 0;

    for (GarbageCollectorMXBean gcBean : gcmBeans) {
      Long prevTime = prevGcTime.get(gcBean.getName());
      long pt = 0;
      if (prevTime != null) {
        pt = prevTime;
      }

      long time = gcBean.getCollectionTime();

      if (time - pt != 0) {
        sawChange = true;
      }

      long increaseInCollectionTime = time - pt;
      sb.append(String.format(" %s=%,.2f(+%,.2f) secs", gcBean.getName(), time / 1000.0, increaseInCollectionTime / 1000.0));
      maxIncreaseInCollectionTime = Math.max(increaseInCollectionTime, maxIncreaseInCollectionTime);
      prevGcTime.put(gcBean.getName(), time);
    }

    long mem = rt.freeMemory();
    if (maxIncreaseInCollectionTime == 0) {
      gcTimeIncreasedCount = 0;
    } else {
      gcTimeIncreasedCount++;
      if (gcTimeIncreasedCount > 3 && mem < rt.maxMemory() * 0.05) {
        log.warn("Running low on memory");
        gcTimeIncreasedCount = 0;
      }
    }

    if (mem != lastMemorySize) {
      sawChange = true;
    }

    String sign = "+";
    if (mem - lastMemorySize <= 0) {
      sign = "";
    }

    sb.append(String.format(" freemem=%,d(%s%,d) totalmem=%,d", mem, sign, (mem - lastMemorySize), rt.totalMemory()));

    if (sawChange) {
      log.debug(sb.toString());
    }

    final long keepAliveTimeout = conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
    if (lastMemoryCheckTime > 0 && lastMemoryCheckTime < now) {
      final long diff = now - lastMemoryCheckTime;
      if (diff > keepAliveTimeout + 1000) {
        log.warn(String.format("GC pause checker not called in a timely fashion. Expected every %.1f seconds but was %.1f seconds since last check",
            keepAliveTimeout / 1000., diff / 1000.));
      }
      lastMemoryCheckTime = now;
      return;
    }

    if (maxIncreaseInCollectionTime > keepAliveTimeout) {
      Halt.halt("Garbage collection may be interfering with lock keep-alive.  Halting.", -1);
    }

    lastMemorySize = mem;
    lastMemoryCheckTime = now;
  }

}

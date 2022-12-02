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

import java.util.EnumMap;

public class StopWatch<K extends Enum<K>> {
  EnumMap<K,Long> startTime;
  EnumMap<K,Long> totalTime;

  public StopWatch(Class<K> k) {
    startTime = new EnumMap<>(k);
    totalTime = new EnumMap<>(k);
  }

  public synchronized void start(K timer) {
    if (startTime.containsKey(timer)) {
      throw new IllegalStateException(timer + " already started");
    }
    startTime.put(timer, System.currentTimeMillis());
  }

  public synchronized void stop(K timer) {

    Long st = startTime.get(timer);

    if (st == null) {
      throw new IllegalStateException(timer + " not started");
    }

    Long existingTime = totalTime.get(timer);
    if (existingTime == null) {
      existingTime = 0L;
    }

    totalTime.put(timer, existingTime + (System.currentTimeMillis() - st));
    startTime.remove(timer);
  }

  public synchronized long get(K timer) {
    Long existingTime = totalTime.get(timer);
    if (existingTime == null) {
      existingTime = 0L;
    }
    return existingTime;
  }

  public synchronized double getSecs(K timer) {
    Long existingTime = totalTime.get(timer);
    if (existingTime == null) {
      existingTime = 0L;
    }
    return existingTime / 1000.0;
  }

}

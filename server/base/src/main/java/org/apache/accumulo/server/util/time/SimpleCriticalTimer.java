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
package org.apache.accumulo.server.util.time;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.AccumuloUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic singleton timer for critical tasks. Overrides {@link #getUncaughtExceptionHandler()} to
 * use {@link AccumuloUncaughtExceptionHandler}
 */
public class SimpleCriticalTimer extends SimpleTimer {

  private static final Logger log = LoggerFactory.getLogger(SimpleCriticalTimer.class);

  private static int instanceThreadPoolSize = -1;
  private static SimpleCriticalTimer instance;

  /**
   * Gets the timer instance. If an instance has already been created, it will have the number of
   * threads supplied when it was constructed, and the size provided here is ignored.
   *
   * @param threadPoolSize
   *          number of threads
   */
  public static synchronized SimpleTimer getInstance(int threadPoolSize) {
    if (instance == null) {
      instance = new SimpleCriticalTimer(threadPoolSize);
      SimpleCriticalTimer.instanceThreadPoolSize = threadPoolSize;
    } else {
      if (SimpleCriticalTimer.instanceThreadPoolSize != threadPoolSize) {
        log.warn("Asked to create SimpleTimer with thread pool size {}, existing instance has {}",
            threadPoolSize, instanceThreadPoolSize);
      }
    }
    return instance;
  }

  /**
   * Gets the timer instance. If an instance has already been created, it will have the number of
   * threads supplied when it was constructed, and the size provided by the configuration here is
   * ignored. If a null configuration is supplied, the number of threads defaults to 1.
   *
   * @param conf
   *          configuration from which to get the number of threads
   * @see Property#GENERAL_SIMPLETIMER_THREADPOOL_SIZE
   */
  public static synchronized SimpleTimer getInstance(AccumuloConfiguration conf) {
    int threadPoolSize;
    if (conf != null) {
      threadPoolSize = conf.getCount(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);
    } else {
      threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    }
    return getInstance(threadPoolSize);
  }

  protected SimpleCriticalTimer(int threadPoolSize) {
    super(threadPoolSize);
  }

  @Override
  protected UncaughtExceptionHandler getUncaughtExceptionHandler() {
    return new AccumuloUncaughtExceptionHandler();
  }

}

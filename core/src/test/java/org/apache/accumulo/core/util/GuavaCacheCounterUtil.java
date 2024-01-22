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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;

/**
 * Unit test helper for counting guava cache entries. Guava cache size() is approximate, and can
 * include garbage-collected entries, so we iterate to get the actual cache size
 */
public class GuavaCacheCounterUtil {
  private static final Logger LOG = LoggerFactory.getLogger(GuavaCacheCounterUtil.class);
  private static final int retries = 5;
  private static final long delayMills = 1_000;

  /**
   * guava cache size() is approximate, and can include garbage-collected entries, so we iterate to
   * get the actual cache size
   */
  public static long cacheCount(final Cache<?,?> cache) {
    return cache.asMap().entrySet().stream().count();
  }

  public static void assertCacheCountEquals(final long expected, final Cache<?,?> cache) {
    Preconditions.checkArgument(expected >= 0, "Expected cache size must be >= 0");
    long received = Long.MIN_VALUE;
    int retryCount = 0;
    while (retryCount++ < retries) {
      received = cacheCount(cache);
      if (received == expected) {
        return;
      }
      LOG.info("Cache count {} did not match expected: {}, try count: {}", received, expected,
          received);
      UtilWaitThread.sleep(delayMills);
    }
    assertEquals(expected, received, "expected cache entry count did not match expected");
  }

}

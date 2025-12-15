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
package org.apache.accumulo.core.trace;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.spi.cache.CacheType;

class ScanInstrumentationImpl extends ScanInstrumentation {
  private final AtomicLong fileBytesRead = new AtomicLong();
  private final AtomicLong uncompressedBytesRead = new AtomicLong();
  private final AtomicInteger[] cacheHits = new AtomicInteger[CacheType.values().length];
  private final AtomicInteger[] cacheMisses = new AtomicInteger[CacheType.values().length];
  private final AtomicInteger[] cacheBypasses = new AtomicInteger[CacheType.values().length];

  ScanInstrumentationImpl() {
    for (int i = 0; i < CacheType.values().length; i++) {
      cacheHits[i] = new AtomicInteger();
      cacheMisses[i] = new AtomicInteger();
      cacheBypasses[i] = new AtomicInteger();
    }
  }

  @Override
  public boolean enabled() {
    return true;
  }

  @Override
  public void incrementFileBytesRead(long amount) {
    fileBytesRead.addAndGet(amount);
  }

  // TODO should it be an option to cache compressed data?
  @Override
  public void incrementUncompressedBytesRead(long amount) {
    uncompressedBytesRead.addAndGet(amount);
  }

  @Override
  public void incrementCacheMiss(CacheType cacheType) {
    cacheMisses[cacheType.ordinal()].incrementAndGet();
  }

  @Override
  public void incrementCacheHit(CacheType cacheType) {
    cacheHits[cacheType.ordinal()].incrementAndGet();
  }

  @Override
  public void incrementCacheBypass(CacheType cacheType) {
    cacheBypasses[cacheType.ordinal()].incrementAndGet();
  }

  @Override
  public long getFileBytesRead() {
    return fileBytesRead.get();
  }

  @Override
  public long getUncompressedBytesRead() {
    return uncompressedBytesRead.get();
  }

  @Override
  public int getCacheHits(CacheType cacheType) {
    return cacheHits[cacheType.ordinal()].get();
  }

  @Override
  public int getCacheMisses(CacheType cacheType) {
    return cacheMisses[cacheType.ordinal()].get();
  }

  @Override
  public int getCacheBypasses(CacheType cacheType) {
    return cacheBypasses[cacheType.ordinal()].get();
  }

}

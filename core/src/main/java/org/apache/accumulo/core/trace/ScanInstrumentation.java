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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.spi.cache.CacheType;

import io.opentelemetry.api.trace.Span;

/**
 * This class helps collect per scan information for the purposes of tracing.
 */
public class ScanInstrumentation {
  private final AtomicLong fileBytesRead = new AtomicLong();
  private final AtomicLong uncompressedBytesRead = new AtomicLong();
  private final AtomicInteger[] cacheHits = new AtomicInteger[CacheType.values().length];
  private final AtomicInteger[] cacheMisses = new AtomicInteger[CacheType.values().length];
  private final AtomicInteger[] cacheBypasses = new AtomicInteger[CacheType.values().length];

  private static final Map<String,ScanInstrumentation> INSTRUMENTED_SCANS =
      new ConcurrentHashMap<>();

  private ScanInstrumentation() {
    for (int i = 0; i < CacheType.values().length; i++) {
      cacheHits[i] = new AtomicInteger();
      cacheMisses[i] = new AtomicInteger();
      cacheBypasses[i] = new AtomicInteger();
    }
  }

  /**
   * Increments the raw bytes read directly from DFS by a scan.
   *
   * @param amount the amount of bytes read
   */
  public void incrementFileBytesRead(long amount) {
    fileBytesRead.addAndGet(amount);
  }

  // TODO should it be an option to cache compressed data?
  /**
   * Increments the uncompressed and decrypted bytes read by a scan. This will include all
   * uncompressed data read by a scan regardless of if the underlying data came from cache or DFS.
   */
  public void incrementUncompressedBytesRead(long amount) {
    uncompressedBytesRead.addAndGet(amount);
  }

  /**
   * Increments the count of rfile blocks that were not already in the cache.
   */
  public void incrementCacheMiss(CacheType cacheType) {
    cacheMisses[cacheType.ordinal()].incrementAndGet();
  }

  /**
   * Increments the count of rfile blocks that were already in the cache.
   */
  public void incrementCacheHit(CacheType cacheType) {
    cacheHits[cacheType.ordinal()].incrementAndGet();
  }

  /**
   * Increments the count of rfile blocks that were directly read from DFS bypassing the cache.
   */
  public void incrementCacheBypass(CacheType cacheType) {
    cacheBypasses[cacheType.ordinal()].incrementAndGet();
  }

  public long getFileBytesRead() {
    return fileBytesRead.get();
  }

  public long getUncompressedBytesRead() {
    return uncompressedBytesRead.get();
  }

  public int getCacheHits(CacheType cacheType) {
    return cacheHits[cacheType.ordinal()].get();
  }

  public int getCacheMisses(CacheType cacheType) {
    return cacheMisses[cacheType.ordinal()].get();
  }

  public int getCacheBypasses(CacheType cacheType) {
    return cacheBypasses[cacheType.ordinal()].get();
  }

  public static void enable(Span span) {
    if (span.isRecording()) {
      INSTRUMENTED_SCANS.put(span.getSpanContext().getTraceId(), new ScanInstrumentation());
    }
  }

  public static ScanInstrumentation get() {
    var span = Span.current();
    if (span.isRecording()) {
      return INSTRUMENTED_SCANS.get(span.getSpanContext().getTraceId());
    }
    return null;
  }

  public static void disable(Span span) {
    if (span.isRecording()) {
      INSTRUMENTED_SCANS.remove(span.getSpanContext().getTraceId());
    }
  }
}

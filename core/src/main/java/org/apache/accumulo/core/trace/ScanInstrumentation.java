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

import org.apache.accumulo.core.spi.cache.CacheType;

import com.google.common.base.Preconditions;

import io.opentelemetry.api.trace.Span;

/**
 * This class helps collect per scan information for the purposes of tracing.
 */
public abstract class ScanInstrumentation {
  private static final ThreadLocal<ScanInstrumentation> INSTRUMENTED_SCANS = new ThreadLocal<>();

  private static final ScanInstrumentation NOOP_SI = new ScanInstrumentation() {
    @Override
    public boolean enabled() {
      return false;
    }

    @Override
    public void incrementFileBytesRead(long amount) {}

    @Override
    public void incrementUncompressedBytesRead(long amount) {}

    @Override
    public void incrementCacheMiss(CacheType cacheType) {}

    @Override
    public void incrementCacheHit(CacheType cacheType) {}

    @Override
    public void incrementCacheBypass(CacheType cacheType) {}

    @Override
    public long getFileBytesRead() {
      return 0;
    }

    @Override
    public long getUncompressedBytesRead() {
      return 0;
    }

    @Override
    public int getCacheHits(CacheType cacheType) {
      return 0;
    }

    @Override
    public int getCacheMisses(CacheType cacheType) {
      return 0;
    }

    @Override
    public int getCacheBypasses(CacheType cacheType) {
      return 0;
    }
  };

  public interface ScanScope extends AutoCloseable {
    @Override
    void close();
  }

  public static ScanScope enable(Span span) {
    if (span.isRecording()) {
      INSTRUMENTED_SCANS.set(new ScanInstrumentationImpl());
      var id = Thread.currentThread().getId();
      return () -> {
        Preconditions.checkState(id == Thread.currentThread().getId());
        INSTRUMENTED_SCANS.remove();
      };
    } else {
      return () -> {};
    }
  }

  public static ScanInstrumentation get() {
    var si = INSTRUMENTED_SCANS.get();
    if (si == null) {
      return NOOP_SI;
    }
    return si;
  }

  public abstract boolean enabled();

  /**
   * Increments the raw bytes read directly from DFS by a scan.
   *
   * @param amount the amount of bytes read
   */
  public abstract void incrementFileBytesRead(long amount);

  /**
   * Increments the uncompressed and decrypted bytes read by a scan. This will include all
   * uncompressed data read by a scan regardless of if the underlying data came from cache or DFS.
   */
  public abstract void incrementUncompressedBytesRead(long amount);

  /**
   * Increments the count of rfile blocks that were not already in the cache.
   */
  public abstract void incrementCacheMiss(CacheType cacheType);

  /**
   * Increments the count of rfile blocks that were already in the cache.
   */
  public abstract void incrementCacheHit(CacheType cacheType);

  /**
   * Increments the count of rfile blocks that were directly read from DFS bypassing the cache.
   */
  public abstract void incrementCacheBypass(CacheType cacheType);

  public abstract long getFileBytesRead();

  public abstract long getUncompressedBytesRead();

  public abstract int getCacheHits(CacheType cacheType);

  public abstract int getCacheMisses(CacheType cacheType);

  public abstract int getCacheBypasses(CacheType cacheType);
}

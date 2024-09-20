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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.CacheEntry;
import org.apache.accumulo.core.spi.cache.CacheType;
import org.apache.accumulo.core.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * Provides trace level logging of block cache activity.
 */
public class LoggingBlockCache implements BlockCache {

  private final BlockCache blockCache;
  private static final Logger log = LoggerFactory.getLogger(Logging.PREFIX + "cache");
  private final CacheType type;

  private LoggingBlockCache(CacheType type, BlockCache blockCache) {
    this.type = type;
    this.blockCache = blockCache;
  }

  private static String toString(CacheEntry ce) {
    return ce == null ? null : ce.getBuffer().length + " bytes";
  }

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf) {
    var timer = Timer.startNew();
    var ce = blockCache.cacheBlock(blockName, buf);
    var elapsed = timer.elapsed(TimeUnit.MICROSECONDS);
    log.trace("{} cacheBlock({},{} bytes) returned {} in {}μs", type, blockName, buf.length,
        toString(ce), elapsed);
    return ce;
  }

  @Override
  public CacheEntry getBlock(String blockName) {
    var timer = Timer.startNew();
    var ce = blockCache.getBlock(blockName);
    var elapsed = timer.elapsed(TimeUnit.MICROSECONDS);
    log.trace("{} getBlock({}) returned {} in {}μs", type, blockName, toString(ce), elapsed);
    return ce;

  }

  private final class LoggingLoader implements Loader {
    private final Loader loader;

    private LoggingLoader(Loader loader) {
      this.loader = loader;
    }

    @Override
    public Map<String,Loader> getDependencies() {
      var deps = loader.getDependencies();
      log.trace("{} loader:{} getDependencies() returned {}", type, loader.hashCode(),
          deps.keySet());
      return Maps.transformValues(deps, secondLoader -> new LoggingLoader(secondLoader));
    }

    @Override
    public byte[] load(int maxSize, Map<String,byte[]> dependencies) {
      var timer = Timer.startNew();
      byte[] data = loader.load(maxSize, dependencies);
      var elapsed = timer.elapsed(TimeUnit.MICROSECONDS);
      Map<String,Integer> logDeps =
          Maps.transformValues(dependencies, bytes -> bytes == null ? null : bytes.length);
      log.trace("{} loader:{} load({},{}) returned {} in {}μs", type, loader.hashCode(), maxSize,
          logDeps, data == null ? null : data.length + " bytes", elapsed);
      return data;
    }
  }

  @Override
  public CacheEntry getBlock(String blockName, Loader loader) {
    var timer = Timer.startNew();
    var ce = blockCache.getBlock(blockName, new LoggingLoader(loader));
    var elapsed = timer.elapsed(TimeUnit.MICROSECONDS);
    log.trace("{} getBlock({}, loader:{}) returned {} in {}μs", type, blockName, loader.hashCode(),
        toString(ce), elapsed);
    return ce;
  }

  @Override
  public long getMaxHeapSize() {
    return blockCache.getMaxHeapSize();
  }

  @Override
  public long getMaxSize() {
    return blockCache.getMaxSize();
  }

  @Override
  public Stats getStats() {
    return blockCache.getStats();
  }

  public static BlockCache wrap(CacheType type, BlockCache blockCache) {
    if (blockCache != null && log.isTraceEnabled() && !(blockCache instanceof LoggingBlockCache)) {
      return new LoggingBlockCache(type, blockCache);
    } else {
      return blockCache;
    }
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.file.blockfile.cache.lru;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.core.file.blockfile.cache.BlockCacheManager;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheManager.Configuration;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public final class LruBlockCacheConfiguration {

  public static final String PROPERTY_PREFIX = "lru";

  /** Default Configuration Parameters */

  /** Backing Concurrent Map Configuration */
  public static final Float DEFAULT_LOAD_FACTOR = 0.75f;
  public static final Integer DEFAULT_CONCURRENCY_LEVEL = 16;

  /** Eviction thresholds */
  public static final Float DEFAULT_MIN_FACTOR = 0.75f;
  public static final Float DEFAULT_ACCEPTABLE_FACTOR = 0.85f;

  /** Priority buckets */
  public static final Float DEFAULT_SINGLE_FACTOR = 0.25f;
  public static final Float DEFAULT_MULTI_FACTOR = 0.50f;
  public static final Float DEFAULT_MEMORY_FACTOR = 0.25f;

  // property names
  public static final String ACCEPTABLE_FACTOR_PROPERTY = "acceptable.factor";
  public static final String MIN_FACTOR_PROPERTY = "min.factor";
  public static final String SINGLE_FACTOR_PROPERTY = "single.factor";
  public static final String MULTI_FACTOR_PROPERTY = "multi.factor";
  public static final String MEMORY_FACTOR_PROPERTY = "memory.factor";
  public static final String MAP_LOAD_PROPERTY = "map.load";
  public static final String MAP_CONCURRENCY_PROPERTY = "map.concurrency";
  public static final String EVICTION_THREAD_PROPERTY = "eviction.thread";

  /** Acceptable size of cache (no evictions if size < acceptable) */
  private final float acceptableFactor;

  /** Minimum threshold of cache (when evicting, evict until size < min) */
  private final float minFactor;

  /** Single access bucket size */
  private final float singleFactor;

  /** Multiple access bucket size */
  private final float multiFactor;

  /** In-memory bucket size */
  private final float memoryFactor;

  /** LruBlockCache cache = new LruBlockCache **/
  private final float mapLoadFactor;

  /** LruBlockCache cache = new LruBlockCache **/
  private final int mapConcurrencyLevel;

  private final boolean useEvictionThread;

  private final Configuration conf;

  private final Map<String,String> props;

  private final CacheType type;

  private Optional<String> get(String k) {
    return Optional.ofNullable(props.get(k));
  }

  public LruBlockCacheConfiguration(Configuration conf, CacheType type) {

    this.type = type;
    this.conf = conf;
    this.props = conf.getProperties(PROPERTY_PREFIX, type);

    this.acceptableFactor = get(ACCEPTABLE_FACTOR_PROPERTY).map(Float::valueOf).filter(f -> f > 0).orElse(DEFAULT_ACCEPTABLE_FACTOR);
    this.minFactor = get(MIN_FACTOR_PROPERTY).map(Float::valueOf).filter(f -> f > 0).orElse(DEFAULT_MIN_FACTOR);
    this.singleFactor = get(SINGLE_FACTOR_PROPERTY).map(Float::valueOf).filter(f -> f > 0).orElse(DEFAULT_SINGLE_FACTOR);
    this.multiFactor = get(MULTI_FACTOR_PROPERTY).map(Float::valueOf).filter(f -> f > 0).orElse(DEFAULT_MULTI_FACTOR);
    this.memoryFactor = get(MEMORY_FACTOR_PROPERTY).map(Float::valueOf).filter(f -> f > 0).orElse(DEFAULT_MEMORY_FACTOR);
    this.mapLoadFactor = get(MAP_LOAD_PROPERTY).map(Float::valueOf).filter(f -> f > 0).orElse(DEFAULT_LOAD_FACTOR);
    this.mapConcurrencyLevel = get(MAP_CONCURRENCY_PROPERTY).map(Integer::valueOf).filter(i -> i > 0).orElse(DEFAULT_CONCURRENCY_LEVEL);
    this.useEvictionThread = get(EVICTION_THREAD_PROPERTY).map(Boolean::valueOf).orElse(true);

    if (this.getSingleFactor() + this.getMultiFactor() + this.getMemoryFactor() != 1) {
      throw new IllegalArgumentException("Single, multi, and memory factors " + " should total 1.0");
    }
    if (this.getMinFactor() >= this.getAcceptableFactor()) {
      throw new IllegalArgumentException("minFactor must be smaller than acceptableFactor");
    }
    if (this.getMinFactor() >= 1.0f || this.getAcceptableFactor() >= 1.0f) {
      throw new IllegalArgumentException("all factors must be < 1");
    }
  }

  public float getAcceptableFactor() {
    return acceptableFactor;
  }

  public float getMinFactor() {
    return minFactor;
  }

  public float getSingleFactor() {
    return singleFactor;
  }

  public float getMultiFactor() {
    return multiFactor;
  }

  public float getMemoryFactor() {
    return memoryFactor;
  }

  public float getMapLoadFactor() {
    return mapLoadFactor;
  }

  public int getMapConcurrencyLevel() {
    return mapConcurrencyLevel;
  }

  public boolean isUseEvictionThread() {
    return useEvictionThread;
  }

  public static class Builder {
    private Map<String,String> props = new HashMap<>();
    private String prefix;

    private Builder(String prefix) {
      this.prefix = prefix;
    }

    private void set(String prop, float val) {
      props.put(prefix + prop, Float.toString(val));
    }

    public Builder acceptableFactor(float af) {
      Preconditions.checkArgument(af > 0);
      set(ACCEPTABLE_FACTOR_PROPERTY, af);
      return this;
    }

    public Builder minFactor(float mf) {
      Preconditions.checkArgument(mf > 0);
      set(MIN_FACTOR_PROPERTY, mf);
      return this;
    }

    public Builder singleFactor(float sf) {
      Preconditions.checkArgument(sf > 0);
      set(SINGLE_FACTOR_PROPERTY, sf);
      return this;
    }

    public Builder multiFactor(float mf) {
      Preconditions.checkArgument(mf > 0);
      set(MULTI_FACTOR_PROPERTY, mf);
      return this;
    }

    public Builder memoryFactor(float mf) {
      Preconditions.checkArgument(mf > 0);
      set(MEMORY_FACTOR_PROPERTY, mf);
      return this;
    }

    public Builder mapLoadFactor(float mlf) {
      Preconditions.checkArgument(mlf > 0);
      set(MAP_LOAD_PROPERTY, mlf);
      return this;
    }

    public Builder mapConcurrencyLevel(int mcl) {
      Preconditions.checkArgument(mcl > 0);
      props.put(prefix + MAP_CONCURRENCY_PROPERTY, mcl + "");
      return this;
    }

    public Builder useEvictionThread(boolean uet) {
      props.put(prefix + EVICTION_THREAD_PROPERTY, uet + "");
      return this;
    }

    public Map<String,String> buildMap() {
      return ImmutableMap.copyOf(props);
    }
  }

  public static Builder builder(CacheType ct) {
    return new Builder(BlockCacheManager.getFullyQualifiedPropertyPrefix(PROPERTY_PREFIX, ct));
  }

  @Override
  public String toString() {
    return super.toString() + ", acceptableFactor: " + this.getAcceptableFactor() + ", minFactor: " + this.getMinFactor() + ", singleFactor: "
        + this.getSingleFactor() + ", multiFactor: " + this.getMultiFactor() + ", memoryFactor: " + this.getMemoryFactor() + ", mapLoadFactor: "
        + this.getMapLoadFactor() + ", mapConcurrencyLevel: " + this.getMapConcurrencyLevel() + ", useEvictionThread: " + this.isUseEvictionThread();
  }

  public long getMaxSize() {
    return conf.getMaxSize(type);
  }

  public long getBlockSize() {
    return conf.getBlockSize();
  }

}

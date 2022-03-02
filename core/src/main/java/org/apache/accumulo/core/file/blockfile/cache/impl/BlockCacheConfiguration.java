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
package org.apache.accumulo.core.file.blockfile.cache.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.cache.BlockCacheManager;
import org.apache.accumulo.core.spi.cache.BlockCacheManager.Configuration;
import org.apache.accumulo.core.spi.cache.CacheType;

public class BlockCacheConfiguration implements Configuration {

  /** Approximate block size */
  private final long blockSize;

  private final Property serverPrefix;

  private final Map<String,String> genProps;

  private final long indexMaxSize;

  private final long dataMaxSize;

  private final long summaryMaxSize;

  public BlockCacheConfiguration(AccumuloConfiguration conf, Property serverPrefix,
      Property indexCacheSizeProperty, Property dataCacheSizeProperty,
      Property summaryCacheSizeProperty, Property defaultBlockSizeProperty) {

    this.serverPrefix = serverPrefix;
    this.genProps = conf.getAllPropertiesWithPrefix(serverPrefix);
    this.indexMaxSize = conf.getAsBytes(indexCacheSizeProperty);
    this.dataMaxSize = conf.getAsBytes(dataCacheSizeProperty);
    this.summaryMaxSize = conf.getAsBytes(summaryCacheSizeProperty);
    this.blockSize = conf.getAsBytes(defaultBlockSizeProperty);
  }

  @Override
  public long getMaxSize(CacheType type) {
    switch (type) {
      case INDEX:
        return indexMaxSize;
      case DATA:
        return dataMaxSize;
      case SUMMARY:
        return summaryMaxSize;
      default:
        throw new IllegalArgumentException("Unknown block cache type");
    }
  }

  @Override
  public long getBlockSize() {
    return this.blockSize;
  }

  @Override
  public String toString() {
    return "indexMaxSize: " + indexMaxSize + "dataMaxSize: " + dataMaxSize + "summaryMaxSize: "
        + summaryMaxSize + ", blockSize: " + getBlockSize();
  }

  @Override
  public Map<String,String> getProperties(String prefix, CacheType type) {
    HashMap<String,String> props = new HashMap<>();

    // get default props first
    String defaultPrefix =
        BlockCacheManager.getFullyQualifiedPropertyPrefix(serverPrefix.getKey(), prefix);
    genProps.forEach((k, v) -> {
      if (k.startsWith(defaultPrefix)) {
        props.put(k.substring(defaultPrefix.length()), v);
      }
    });

    String typePrefix =
        BlockCacheManager.getFullyQualifiedPropertyPrefix(serverPrefix.getKey(), prefix, type);
    genProps.forEach((k, v) -> {
      if (k.startsWith(typePrefix)) {
        props.put(k.substring(typePrefix.length()), v);
      }
    });

    return Collections.unmodifiableMap(props);
  }
}

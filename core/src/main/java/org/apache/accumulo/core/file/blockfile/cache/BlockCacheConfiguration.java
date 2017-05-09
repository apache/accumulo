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
package org.apache.accumulo.core.file.blockfile.cache;

import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;

public class BlockCacheConfiguration {

  public static final String MAX_SIZE_PROPERTY = "max.size";
  public static final String BLOCK_SIZE_PROPERTY = "block.size";

  private static final Long DEFAULT = Long.valueOf(-1);

  /** Maximum allowable size of cache (block put if size > max, evict) */
  private final long maxSize;

  /** Approximate block size */
  private final long blockSize;

  protected final BlockCacheConfigurationHelper helper;

  public BlockCacheConfiguration(AccumuloConfiguration conf, CacheType type, BlockCacheFactory<?,?> factory) {

    helper = new BlockCacheConfigurationHelper(conf, type, factory);

    Map<String,String> props = conf.getAllPropertiesWithPrefix(Property.GENERAL_ARBITRARY_PROP_PREFIX);
    this.maxSize = getOrDefault(props, helper.getFullPropertyName(MAX_SIZE_PROPERTY), DEFAULT);
    this.blockSize = getOrDefault(props, helper.getFullPropertyName(BLOCK_SIZE_PROPERTY), DEFAULT);

    if (DEFAULT.equals(this.maxSize)) {
      throw new IllegalArgumentException("Block cache max size must be specified.");
    }
    if (DEFAULT.equals(this.blockSize)) {
      throw new IllegalArgumentException("Block cache block size must be specified.");
    }
  }

  public long getMaxSize() {
    return this.maxSize;
  }

  public long getBlockSize() {
    return this.blockSize;
  }

  @Override
  public String toString() {
    return "maxSize: " + getMaxSize() + ", blockSize: " + getBlockSize();
  }

  @SuppressWarnings("unchecked")
  protected <T> T getOrDefault(Map<String,String> props, String propertyName, T defaultValue) {
    String o = props.get(propertyName);
    if (null == o && defaultValue == null) {
      throw new RuntimeException("Property " + propertyName + " not specified and no default supplied.");
    } else if (null == o) {
      return defaultValue;
    } else {
      if (defaultValue.getClass().equals(Integer.class)) {
        return (T) Integer.valueOf(o);
      } else if (defaultValue.getClass().equals(Long.class)) {
        return (T) Long.valueOf(o);
      } else if (defaultValue.getClass().equals(Float.class)) {
        return (T) Float.valueOf(o);
      } else if (defaultValue.getClass().equals(Boolean.class)) {
        return (T) Boolean.valueOf(o);
      } else {
        throw new RuntimeException("Unknown parameter type");
      }
    }

  }

}

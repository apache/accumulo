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
import java.util.Optional;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;

public class BlockCacheConfiguration {

  public static final String CACHE_PROPERTY_BASE = Property.GENERAL_ARBITRARY_PROP_PREFIX + "cache.block.";

  /** Maximum allowable size of cache (block put if size > max, evict) */
  private final long maxSize;

  /** Approximate block size */
  private final long blockSize;

  private final Map<String,String> genProps;

  private final String prefix;

  private final String defaultPrefix;

  public BlockCacheConfiguration(AccumuloConfiguration conf, CacheType type, String implName) {
    defaultPrefix = getDefaultPrefix(implName);
    prefix = getPrefix(type, implName);
    genProps = conf.getAllPropertiesWithPrefix(Property.GENERAL_ARBITRARY_PROP_PREFIX);

    switch (type) {
      case INDEX:
        this.maxSize = conf.getAsBytes(Property.TSERV_INDEXCACHE_SIZE);
        break;
      case DATA:
        this.maxSize = conf.getAsBytes(Property.TSERV_DATACACHE_SIZE);
        break;
      case SUMMARY:
        this.maxSize = conf.getAsBytes(Property.TSERV_SUMMARYCACHE_SIZE);
        break;
      default:
        throw new IllegalArgumentException("Unknown block cache type");
    }
    this.blockSize = conf.getAsBytes(Property.TSERV_DEFAULT_BLOCKSIZE);
  }

  public long getMaxSize() {
    return this.maxSize;
  }

  public long getBlockSize() {
    return this.blockSize;
  }

  protected Optional<String> get(String suffix) {
    String val = genProps.get(prefix + suffix);
    if (val == null) {
      val = genProps.get(defaultPrefix + suffix);
    }
    return Optional.ofNullable(val);
  }

  public static String getDefaultPrefix(String implName) {
    return CACHE_PROPERTY_BASE + implName + ".default.";
  }

  public static String getPrefix(CacheType type, String implName) {
    return CACHE_PROPERTY_BASE + implName + "." + type.name().toLowerCase() + ".";
  }

  @Override
  public String toString() {
    return "maxSize: " + getMaxSize() + ", blockSize: " + getBlockSize();
  }
}

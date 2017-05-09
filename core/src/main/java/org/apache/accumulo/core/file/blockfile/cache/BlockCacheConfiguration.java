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

  /** Maximum allowable size of cache (block put if size > max, evict) */
  private final long maxSize;

  /** Approximate block size */
  private final long blockSize;

  public BlockCacheConfiguration(AccumuloConfiguration conf, CacheType type, String implName) {

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
        this.maxSize = conf.getAsBytes(Property.TSERV_DEFAULT_BLOCKSIZE);
        break;
    }
    this.blockSize = conf.getAsBytes(Property.TSERV_DEFAULT_BLOCKSIZE);
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

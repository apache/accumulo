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
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;

public class BlockCacheConfiguration {

  public static class BlockCacheConfigurationHelper {

    private final ConfigurationCopy conf;
    private final String basePropertyName;

    protected BlockCacheConfigurationHelper(ConfigurationCopy conf, CacheType type, String implName) {
      this.conf = conf;
      this.basePropertyName = BlockCacheManager.CACHE_PROPERTY_BASE + implName + "." + type.name().toLowerCase() + ".";
    }

    public String getPropertyPrefix() {
      return basePropertyName;
    }

    public String getFullPropertyName(String propertySuffix) {
      return this.basePropertyName + propertySuffix;
    }

    public Optional<String> get(String property) {
      return Optional.ofNullable(this.conf.get(this.getFullPropertyName(property)));
    }

    public void set(String propertySuffix, String value) {
      conf.set(getFullPropertyName(propertySuffix), value);
    }

    public ConfigurationCopy getConfiguration() {
      return this.conf;
    }
  }

  /** Maximum allowable size of cache (block put if size > max, evict) */
  private final long maxSize;

  /** Approximate block size */
  private final long blockSize;

  /** Helper object for working with block cache configuration **/
  private final BlockCacheConfigurationHelper helper;

  public BlockCacheConfiguration(AccumuloConfiguration conf, CacheType type, String implName) {

    Map<String,String> props = conf.getAllPropertiesWithPrefix(Property.GENERAL_ARBITRARY_PROP_PREFIX);
    ConfigurationCopy blockCacheConfiguration = new ConfigurationCopy(props);
    this.helper = new BlockCacheConfigurationHelper(blockCacheConfiguration, type, implName);

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

  public BlockCacheConfigurationHelper getHelper() {
    return this.helper;
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

}

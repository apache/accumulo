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
package org.apache.accumulo.core.file.blockfile.impl;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.scan.ScanDispatch;

public class ScanCacheProvider implements CacheProvider {

  private final BlockCache indexCache;
  private final BlockCache dataCache;

  public ScanCacheProvider(AccumuloConfiguration tableConfig, ScanDispatch dispatch,
      BlockCache indexCache, BlockCache dataCache) {
    switch (dispatch.getIndexCacheUsage()) {
      case ENABLED:
        this.indexCache = indexCache;
        break;
      case DISABLED:
        this.indexCache = null;
        break;
      case OPPORTUNISTIC:
        this.indexCache = new OpportunisticBlockCache(indexCache);
        break;
      case TABLE:
        this.indexCache =
            tableConfig.getBoolean(Property.TABLE_INDEXCACHE_ENABLED) ? indexCache : null;
        break;
      default:
        throw new IllegalStateException();
    }

    switch (dispatch.getDataCacheUsage()) {
      case ENABLED:
        this.dataCache = dataCache;
        break;
      case DISABLED:
        this.dataCache = null;
        break;
      case OPPORTUNISTIC:
        this.dataCache = new OpportunisticBlockCache(dataCache);
        break;
      case TABLE:
        this.dataCache =
            tableConfig.getBoolean(Property.TABLE_BLOCKCACHE_ENABLED) ? dataCache : null;
        break;
      default:
        throw new IllegalStateException();
    }

  }

  @Override
  public BlockCache getDataCache() {
    return dataCache;
  }

  @Override
  public BlockCache getIndexCache() {
    return indexCache;
  }
}

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
package org.apache.accumulo.core.spi.compaction;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.util.cache.Caches;

import com.github.benmanes.caffeine.cache.Cache;

/**
 * A unique identifier for a compaction service
 *
 * @since 2.1.0
 */
public class CompactionServiceId extends AbstractId<CompactionServiceId> {
  private static final long serialVersionUID = 1L;

  static final Cache<String,CompactionServiceId> cache =
      Caches.getInstance().createNewBuilder(Caches.CacheName.COMPACTION_SERVICE_ID, false)
          .weakValues().expireAfterAccess(1, TimeUnit.DAYS).build();

  private CompactionServiceId(String canonical) {
    super(canonical);
  }

  /**
   * Get a CompactionServiceID object for the provided canonical string. This is guaranteed to be
   * non-null.
   *
   * @param canonical compaction service ID string
   * @return CompactionServiceId object
   */
  public static CompactionServiceId of(String canonical) {
    return cache.get(canonical, CompactionServiceId::new);
  }
}

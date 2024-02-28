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

import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.util.cache.Caches;

import com.github.benmanes.caffeine.cache.Cache;

/**
 * A unique identifier for a compactor group that a {@link CompactionPlanner} can schedule
 * compactions on using a {@link CompactionJob}.
 *
 * @since 3.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */
public class CompactorGroupId extends AbstractId<CompactorGroupId> {
  private static final long serialVersionUID = 1L;

  static final Cache<String,CompactorGroupId> cache = Caches.getInstance()
      .createNewBuilder(Caches.CacheName.COMPACTOR_GROUP_ID, false).weakValues().build();

  private CompactorGroupId(String canonical) {
    super(canonical);
  }

  /**
   * Get a CompactorGroupId object for the provided canonical string. This is guaranteed to be
   * non-null.
   *
   * @param canonical compactor group ID string
   * @return CompactorGroupId object
   */
  public static CompactorGroupId of(String canonical) {
    return cache.get(canonical, CompactorGroupId::new);
  }
}

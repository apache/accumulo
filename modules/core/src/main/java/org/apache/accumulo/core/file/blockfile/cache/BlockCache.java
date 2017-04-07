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

/**
 * Block cache interface.
 */
public interface BlockCache {
  /**
   * Add block to cache.
   *
   * @param blockName
   *          Zero-based file block number.
   * @param buf
   *          The block contents wrapped in a ByteBuffer.
   * @param inMemory
   *          Whether block should be treated as in-memory
   */
  CacheEntry cacheBlock(String blockName, byte buf[], boolean inMemory);

  /**
   * Add block to cache (defaults to not in-memory).
   *
   * @param blockName
   *          Zero-based file block number.
   * @param buf
   *          The block contents wrapped in a ByteBuffer.
   */
  CacheEntry cacheBlock(String blockName, byte buf[]);

  /**
   * Fetch block from cache.
   *
   * @param blockName
   *          Block number to fetch.
   * @return Block or null if block is not in the cache.
   */
  CacheEntry getBlock(String blockName);

  /**
   * Get the maximum size of this cache.
   *
   * @return max size in bytes
   */
  long getMaxSize();
}

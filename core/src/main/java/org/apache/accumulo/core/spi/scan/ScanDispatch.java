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
package org.apache.accumulo.core.spi.scan;

import org.apache.accumulo.core.spi.scan.ScanDispatcher.DispatchParameters;

/**
 * Encapsulates information about how a scan should be executed. This is the return type for
 * {@link ScanDispatcher#dispatch(DispatchParameters)}. To create an instance of this use
 * {@link ScanDispatch#builder()}
 *
 * @since 2.1.0
 */
public interface ScanDispatch {

  /**
   * Communicates how a scan should use cache.
   *
   * @since 2.1.0
   */
  enum CacheUsage {
    /**
     * Use cache for this can, possibly overriding table settings.
     */
    ENABLED,
    /**
     * Do not use cache for this scan, possibly overriding table settings.
     */
    DISABLED,
    /**
     * Use data if it exists in cache, but never load data into cache.
     */
    OPPORTUNISTIC,
    /**
     * Use the tables cache settings for this scan.
     */
    TABLE
  }

  public String getExecutorName();

  public CacheUsage getDataCacheUsage();

  public CacheUsage getIndexCacheUsage();

  /**
   * @since 2.1.0
   */
  public static interface Builder {

    /**
     * If this is not called, then {@value SimpleScanDispatcher#DEFAULT_SCAN_EXECUTOR_NAME} should
     * be used.
     *
     * @param name a non null name of an existing scan executor to use for this scan from the key
     *        set of {@link ScanDispatcher.DispatchParameters#getScanExecutors()}
     * @return may return self or a new object
     */
    public Builder setExecutorName(String name);

    /**
     * If this is not called, then {@link CacheUsage#TABLE} should be used.
     *
     * @param usage a non null usage indicating how the scan should use cache for file metadata
     *        (like the index tree within a file)
     * @return may return self or a new object
     */
    public Builder setIndexCacheUsage(CacheUsage usage);

    /**
     * If this is not called, then {@link CacheUsage#TABLE} should be used.
     *
     * @param usage a non null usage indicating how the scan should use cache for file data
     * @return may return self or a new object
     */
    public Builder setDataCacheUsage(CacheUsage usage);

    /**
     * @return an immutable {@link ScanDispatch} object.
     */
    public ScanDispatch build();
  }

  /**
   * @return a {@link ScanDispatch} builder
   */
  public static Builder builder() {
    return DefaultScanDispatch.DEFAULT_SCAN_DISPATCH;
  }
}

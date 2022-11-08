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

import java.util.Objects;

import org.apache.accumulo.core.spi.scan.ScanDispatch.Builder;

import com.google.common.base.Preconditions;

/**
 * This class is intentionally package private. Do not make public!
 */
class ScanDispatchImpl implements ScanDispatch, Builder {

  // The purpose of this is to allow building an immutable ScanDispatch object without creating
  // separate Builder and ScanDispatch objects. This is done to reduce object creation and
  // copying. This could easily be changed to two objects without changing the interfaces.
  private boolean built = false;

  private String executorName;
  private CacheUsage indexCacheUsage;
  private CacheUsage dataCacheUsage;

  ScanDispatchImpl() {
    executorName = SimpleScanDispatcher.DEFAULT_SCAN_EXECUTOR_NAME;
    indexCacheUsage = CacheUsage.TABLE;
    dataCacheUsage = CacheUsage.TABLE;
  }

  @Override
  public String getExecutorName() {
    Preconditions.checkState(built);
    return executorName;
  }

  @Override
  public Builder setExecutorName(String name) {
    Preconditions.checkState(!built);
    this.executorName = Objects.requireNonNull(name);
    return this;
  }

  @Override
  public ScanDispatch build() {
    Preconditions.checkState(!built);
    built = true;
    return this;
  }

  @Override
  public Builder setIndexCacheUsage(CacheUsage usage) {
    Preconditions.checkState(!built);
    this.indexCacheUsage = Objects.requireNonNull(usage);
    return this;
  }

  @Override
  public Builder setDataCacheUsage(CacheUsage usage) {
    Preconditions.checkState(!built);
    this.dataCacheUsage = Objects.requireNonNull(usage);
    return this;
  }

  @Override
  public CacheUsage getDataCacheUsage() {
    Preconditions.checkState(built);
    return dataCacheUsage;
  }

  @Override
  public CacheUsage getIndexCacheUsage() {
    Preconditions.checkState(built);
    return indexCacheUsage;
  }

  @Override
  public String toString() {
    return "{executorName=" + executorName + ", indexCacheUsage=" + indexCacheUsage
        + ", dataCacheUsage=" + dataCacheUsage + ", built=" + built + "}";
  }
}

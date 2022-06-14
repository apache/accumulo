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

/**
 * This class is intentionally package private. Do not make public!
 *
 * <p>
 * The purpose of this class is to avoid any object creation in the case where
 * {@code ScanDispatch.builder().build()} is called.
 */
class DefaultScanDispatch extends ScanDispatchImpl {

  static DefaultScanDispatch DEFAULT_SCAN_DISPATCH = new DefaultScanDispatch();

  private DefaultScanDispatch() {
    super.build();
  }

  @Override
  public Builder setExecutorName(String name) {
    return new ScanDispatchImpl().setExecutorName(name);
  }

  @Override
  public Builder setIndexCacheUsage(CacheUsage usage) {
    return new ScanDispatchImpl().setIndexCacheUsage(usage);
  }

  @Override
  public Builder setDataCacheUsage(CacheUsage usage) {
    return new ScanDispatchImpl().setDataCacheUsage(usage);
  }

  @Override
  public ScanDispatch build() {
    return this;
  }
}

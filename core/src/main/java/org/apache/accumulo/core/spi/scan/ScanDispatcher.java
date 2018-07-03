/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.spi.scan;

import java.util.Map;

import com.google.common.base.Preconditions;

/**
 * A per table scan dispatcher that decides which executor should be used to processes a scan. For
 * information about configuring, find the documentation for the {@code table.scan.dispatcher} and
 * {@code table.scan.dispatcher.opts.} properties.
 *
 * @since 2.0.0
 */
public interface ScanDispatcher {
  /**
   * This method is called once after a ScanDispatcher is instantiated.
   *
   * @param options
   *          The configured options. For example if the table properties
   *          {@code table.scan.dispatcher.opts.p1=abc} and
   *          {@code table.scan.dispatcher.opts.p9=123} were set, then this map would contain
   *          {@code p1=abc} and {@code p9=123}.
   */
  public default void init(Map<String,String> options) {
    Preconditions.checkArgument(options.isEmpty(), "No options expected");
  }

  /**
   * @param scanInfo
   *          Information about the scan.
   * @param scanExecutors
   *          Information about the currently configured executors.
   * @return Should return one of the executors named in scanExecutors.keySet()
   */
  String dispatch(ScanInfo scanInfo, Map<String,ScanExecutor> scanExecutors);
}

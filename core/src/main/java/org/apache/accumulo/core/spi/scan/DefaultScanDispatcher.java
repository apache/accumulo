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
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class DefaultScanDispatcher implements ScanDispatcher {

  private final Set<String> VALID_OPTS = ImmutableSet.of("executor");
  private String scanExecutor;

  @Override
  public void init(Map<String,String> options) {
    Set<String> invalidOpts = Sets.difference(options.keySet(), VALID_OPTS);
    Preconditions.checkArgument(invalidOpts.size() == 0, "Invalid options : %s", invalidOpts);
    this.scanExecutor = options.getOrDefault("executor", "default");
  }

  @Override
  public String dispatch(ScanInfo scanInfo, Map<String,ScanExecutor> scanExecutors) {
    return scanExecutor;
  }
}

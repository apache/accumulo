/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.spi.compaction;

/**
 * This class intentionally package private. It supports one object allocation for
 * {@code CompactionDirectives}.
 */
class CompactionsDirectiveImpl implements CompactionDirectives {

  static final CompactionsDirectiveImpl DEFAULT =
      new CompactionsDirectiveImpl(CompactionServiceId.of("default"));

  private CompactionServiceId service;

  private CompactionsDirectiveImpl(CompactionServiceId service) {
    this.service = service;
  }

  @Override
  public CompactionServiceId getService() {
    return service;
  }

  public void setService(String serviceStr) {
    this.service = CompactionServiceId.of(serviceStr);
  }

  @Override
  public String toString() {
    return "service=" + service;
  }
}

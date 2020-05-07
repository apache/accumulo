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

import java.util.Objects;

import org.apache.accumulo.core.spi.compaction.CompactionDirectives.Builder;

import com.google.common.base.Preconditions;

/**
 * This class intentionally package private. This implementation is odd because it supports zero
 * object allocations for {@code CompactionDirectives.builder().build()}.
 */
class CompactionsDirectiveImpl implements Builder, CompactionDirectives {

  private static final CompactionDirectives DEFAULT =
      new CompactionsDirectiveImpl().setService(CompactionServiceId.of("default")).build();

  static final Builder DEFAULT_BUILDER = new Builder() {
    @Override
    public Builder setService(CompactionServiceId service) {
      return new CompactionsDirectiveImpl().setService(service);
    }

    @Override
    public Builder setService(String compactionServiceId) {
      return new CompactionsDirectiveImpl().setService(compactionServiceId);
    }

    @Override
    public CompactionDirectives build() {
      return DEFAULT;
    }
  };

  boolean built = false;
  private CompactionServiceId service;

  @Override
  public Builder setService(CompactionServiceId service) {
    Objects.requireNonNull(service);
    Preconditions.checkState(!built);
    this.service = service;
    return this;
  }

  @Override
  public Builder setService(String compactionServiceId) {
    return setService(CompactionServiceId.of(compactionServiceId));
  }

  @Override
  public CompactionServiceId getService() {
    Preconditions.checkState(built);
    return service;
  }

  @Override
  public CompactionDirectives build() {
    built = true;
    return this;
  }

  @Override
  public String toString() {
    return "service=" + service;
  }
}

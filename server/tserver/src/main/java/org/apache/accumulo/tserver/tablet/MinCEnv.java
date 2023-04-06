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
package org.apache.accumulo.tserver.tablet;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionReason;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.FileCompactor;
import org.apache.accumulo.server.iterators.SystemIteratorEnvironment;
import org.apache.accumulo.server.iterators.TabletIteratorEnvironment;
import org.apache.accumulo.tserver.MinorCompactionReason;

public class MinCEnv implements FileCompactor.CompactionEnv {
  private final MinorCompactionReason reason;
  private final SortedKeyValueIterator<Key,Value> iter;

  public MinCEnv(MinorCompactionReason reason, SortedKeyValueIterator<Key,Value> iter) {
    this.reason = reason;
    this.iter = iter;
  }

  @Override
  public boolean isCompactionEnabled() {
    return true;
  }

  @Override
  public IteratorUtil.IteratorScope getIteratorScope() {
    return IteratorUtil.IteratorScope.minc;
  }

  @Override
  public RateLimiter getReadLimiter() {
    return null;
  }

  @Override
  public RateLimiter getWriteLimiter() {
    return null;
  }

  @Override
  public SystemIteratorEnvironment createIteratorEnv(ServerContext context,
      AccumuloConfiguration acuTableConf, TableId tableId) {
    return new TabletIteratorEnvironment(context, IteratorUtil.IteratorScope.minc, acuTableConf,
        tableId);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> getMinCIterator() {
    return iter;
  }

  @Override
  public TCompactionReason getReason() {
    switch (reason) {
      case USER:
        return TCompactionReason.USER;
      case CLOSE:
        return TCompactionReason.CLOSE;
      case SYSTEM:
      default:
        return TCompactionReason.SYSTEM;
    }
  }
}

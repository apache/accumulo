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
package org.apache.accumulo.compactor;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionReason;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.util.ratelimit.NullRateLimiter;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.FileCompactor.CompactionEnv;
import org.apache.accumulo.server.iterators.SystemIteratorEnvironment;
import org.apache.accumulo.server.iterators.TabletIteratorEnvironment;

import com.google.common.annotations.VisibleForTesting;

public class ExtCEnv implements CompactionEnv {

  private final CompactionJobHolder jobHolder;
  private TExternalCompactionJob job;
  private String queueName;

  public static class CompactorIterEnv extends TabletIteratorEnvironment {

    private String queueName;

    public CompactorIterEnv(ServerContext context, IteratorScope scope, boolean fullMajC,
        AccumuloConfiguration tableConfig, TableId tableId, CompactionKind kind, String queueName) {
      super(context, scope, fullMajC, tableConfig, tableId, kind);
      this.queueName = queueName;
    }

    @VisibleForTesting
    public String getQueueName() {
      return queueName;
    }
  }

  ExtCEnv(CompactionJobHolder jobHolder, String queueName) {
    this.jobHolder = jobHolder;
    this.job = jobHolder.getJob();
    this.queueName = queueName;
  }

  @Override
  public boolean isCompactionEnabled() {
    return !jobHolder.isCancelled();
  }

  @Override
  public IteratorScope getIteratorScope() {
    return IteratorScope.majc;
  }

  @Override
  public RateLimiter getReadLimiter() {
    return NullRateLimiter.INSTANCE;
  }

  @Override
  public RateLimiter getWriteLimiter() {
    return NullRateLimiter.INSTANCE;
  }

  @Override
  public SystemIteratorEnvironment createIteratorEnv(ServerContext context,
      AccumuloConfiguration acuTableConf, TableId tableId) {
    return new CompactorIterEnv(context, IteratorScope.majc,
        !jobHolder.getJob().isPropagateDeletes(), acuTableConf, tableId,
        CompactionKind.valueOf(job.getKind().name()), queueName);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> getMinCIterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TCompactionReason getReason() {
    switch (job.getKind()) {
      case USER:
        return TCompactionReason.USER;
      case CHOP:
        return TCompactionReason.CHOP;
      case SELECTOR:
      case SYSTEM:
      default:
        return TCompactionReason.SYSTEM;
    }
  }

}

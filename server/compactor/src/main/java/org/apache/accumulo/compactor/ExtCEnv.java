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
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionReason;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.FileCompactor.CompactionEnv;
import org.apache.accumulo.server.iterators.SystemIteratorEnvironment;
import org.apache.accumulo.server.iterators.SystemIteratorEnvironmentImpl;

import com.google.common.annotations.VisibleForTesting;

public class ExtCEnv implements CompactionEnv {

  private final CompactionJobHolder jobHolder;
  private final TExternalCompactionJob job;
  private final ResourceGroupId groupName;

  public static class CompactorIterEnv extends SystemIteratorEnvironmentImpl {

    private static class Builder extends SystemIteratorEnvironmentImpl.Builder {

      private final ResourceGroupId groupName;

      public Builder(ServerContext context, ResourceGroupId groupName) {
        super(context);
        this.groupName = groupName;
      }

      @Override
      public SystemIteratorEnvironmentImpl build() {
        return new CompactorIterEnv(this);
      }

    }

    private final ResourceGroupId groupName;

    public CompactorIterEnv(Builder builder) {
      super(builder);
      this.groupName = builder.groupName;
    }

    @VisibleForTesting
    public ResourceGroupId getQueueName() {
      return groupName;
    }

  }

  ExtCEnv(CompactionJobHolder jobHolder, ResourceGroupId groupName) {
    this.jobHolder = jobHolder;
    this.job = jobHolder.getJob();
    this.groupName = groupName;
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
  public SystemIteratorEnvironment createIteratorEnv(ServerContext context,
      AccumuloConfiguration acuTableConf, TableId tableId) {

    CompactorIterEnv.Builder builder = new CompactorIterEnv.Builder(context, groupName);
    builder.withScope(IteratorScope.majc).withTableId(tableId);

    if (CompactionKind.valueOf(job.getKind().name()) == CompactionKind.USER) {
      builder.isUserCompaction();
    }

    if (!jobHolder.getJob().isPropagateDeletes()) {
      builder.isFullMajorCompaction();
    }

    return builder.build();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> getMinCIterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TCompactionReason getReason() {
    return switch (job.getKind()) {
      case USER -> TCompactionReason.USER;
      case SYSTEM -> TCompactionReason.SYSTEM;
    };
  }

}

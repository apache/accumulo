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
package org.apache.accumulo.server.manager.state;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SUSPEND;

import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.server.ServerContext;

import com.google.common.base.Preconditions;

class MetaDataStateStore extends AbstractTabletStateStore implements TabletStateStore {

  protected final ClientContext context;
  private final String targetTableName;
  private final Ample ample;
  private final DataLevel level;

  protected MetaDataStateStore(DataLevel level, ServerContext context, String targetTableName) {
    super(context);
    this.level = level;
    this.context = context;
    this.ample = context.getAmple();
    this.targetTableName = targetTableName;
  }

  MetaDataStateStore(DataLevel level, ServerContext context) {
    this(level, context, AccumuloTable.METADATA.tableName());
  }

  @Override
  public DataLevel getLevel() {
    return level;
  }

  @Override
  public ClosableIterator<TabletManagement> iterator(List<Range> ranges,
      TabletManagementParameters parameters) {
    Preconditions.checkArgument(parameters.getLevel() == getLevel());
    return new TabletManagementScanner(context, ranges, parameters, targetTableName);
  }

  @Override
  public void unsuspend(Collection<TabletMetadata> tablets) throws DistributedStoreException {
    try (var tabletsMutator = ample.conditionallyMutateTablets()) {
      for (TabletMetadata tm : tablets) {
        if (tm.getSuspend() != null) {
          tabletsMutator.mutateTablet(tm.getExtent()).requireAbsentOperation()
              .requireSame(tm, SUSPEND).deleteSuspension()
              .submit(tabletMetadata -> tabletMetadata.getSuspend() == null);
        }
      }

      boolean unacceptedConditions = tabletsMutator.process().values().stream()
          .anyMatch(conditionalResult -> conditionalResult.getStatus() != Status.ACCEPTED);
      if (unacceptedConditions) {
        throw new DistributedStoreException("Some mutations failed to satisfy conditions");
      }
    } catch (RuntimeException ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public String name() {
    return "Normal Tablets";
  }

  @Override
  protected void processSuspension(Ample.ConditionalTabletMutator tabletMutator, TabletMetadata tm,
      long suspensionTimestamp) {
    if (tm.hasCurrent()) {
      if (suspensionTimestamp >= 0) {
        tabletMutator.putSuspension(tm.getLocation().getServerInstance(), suspensionTimestamp);
      }
    }

    if (tm.getSuspend() != null && suspensionTimestamp < 0) {
      tabletMutator.deleteSuspension();
    }
  }
}

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

import java.util.Collection;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;

class MetaDataStateStore extends AbstractTabletStateStore implements TabletStateStore {

  protected final ClientContext context;
  protected final CurrentState state;
  private final String targetTableName;
  private final Ample ample;

  protected MetaDataStateStore(ClientContext context, CurrentState state, String targetTableName) {
    super(context);
    this.context = context;
    this.state = state;
    this.ample = context.getAmple();
    this.targetTableName = targetTableName;
  }

  MetaDataStateStore(ClientContext context, CurrentState state) {
    this(context, state, MetadataTable.NAME);
  }

  @Override
  public ClosableIterator<TabletLocationState> iterator() {
    return new MetaDataTableScanner(context, TabletsSection.getRange(), state, targetTableName);
  }

  @Override
  public void unsuspend(Collection<TabletLocationState> tablets) throws DistributedStoreException {
    try (var tabletsMutator = ample.conditionallyMutateTablets()) {
      for (TabletLocationState tls : tablets) {
        if (tls.suspend != null) {
          continue;
        }

        // ELASTICITY_TODO pending #3314, add conditional mutation check that tls.suspend exists
        tabletsMutator.mutateTablet(tls.extent).requireAbsentOperation().deleteSuspension()
            .submit(tabletMetadata -> tabletMetadata.getSuspend() == null);
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
  protected void processSuspension(Ample.ConditionalTabletMutator tabletMutator,
      TabletLocationState tls, long suspensionTimestamp) {
    if (tls.current != null) {
      if (suspensionTimestamp >= 0) {
        tabletMutator.putSuspension(tls.current.getServerInstance(), suspensionTimestamp);
      }
    }

    if (tls.suspend != null && suspensionTimestamp < 0) {
      tabletMutator.deleteSuspension();
    }
  }
}

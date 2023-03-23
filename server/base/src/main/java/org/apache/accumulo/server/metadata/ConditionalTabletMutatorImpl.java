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

package org.apache.accumulo.server.metadata;

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.OPID_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.encodePrevEndRow;

import java.util.function.Consumer;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.*;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperation;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metadata.iterators.LocationExistsIterator;
import org.apache.accumulo.server.metadata.iterators.PresentIterator;
import org.apache.accumulo.server.metadata.iterators.TabletExistsIterator;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class ConditionalTabletMutatorImpl extends TabletMutatorBase<Ample.ConditionalTabletMutator>
    implements Ample.ConditionalTabletMutator, Ample.OperationRequirements {

  private static final int INITIAL_ITERATOR_PRIO = 1000000;

  private final ConditionalMutation mutation;
  private final Consumer<ConditionalMutation> mutationConsumer;
  private final Ample.ConditionalTabletsMutator parent;

  private boolean sawOperationRequirement = false;

  protected ConditionalTabletMutatorImpl(Ample.ConditionalTabletsMutator parent,
      ServerContext context, KeyExtent extent, Consumer<ConditionalMutation> mutationConsumer) {
    super(context, extent, new ConditionalMutation(extent.toMetaRow()));
    this.mutation = (ConditionalMutation) super.mutation;
    this.mutationConsumer = mutationConsumer;
    this.parent = parent;
  }

  @Override
  public Ample.ConditionalTabletMutator requireAbsentLocation() {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    IteratorSetting is = new IteratorSetting(INITIAL_ITERATOR_PRIO, LocationExistsIterator.class);
    Condition c = new Condition("", "").setIterators(is);
    mutation.addCondition(c);
    return this;
  }

  @Override
  public Ample.ConditionalTabletMutator requireLocation(TServerInstance tsi,
      TabletMetadata.LocationType type) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    Condition c =
        new Condition(getLocationFamily(type), tsi.getSession()).setValue(tsi.getHostPort());
    mutation.addCondition(c);
    return this;
  }

  @Override
  public Ample.ConditionalTabletMutator requireFile(StoredTabletFile path) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    IteratorSetting is = new IteratorSetting(INITIAL_ITERATOR_PRIO, PresentIterator.class);
    Condition c = new Condition(DataFileColumnFamily.NAME, path.getMetaUpdateDeleteText())
        .setValue(PresentIterator.VALUE).setIterators(is);
    mutation.addCondition(c);
    return this;
  }

  @Override
  public Ample.ConditionalTabletMutator requireAbsentBulkFile(TabletFile bulkref) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    Condition c = new Condition(BulkFileColumnFamily.NAME, bulkref.getMetaInsertText());
    mutation.addCondition(c);
    return this;
  }

  @Override
  public Ample.ConditionalTabletMutator requirePrevEndRow(Text per) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    Condition c =
        new Condition(PREV_ROW_COLUMN.getColumnFamily(), PREV_ROW_COLUMN.getColumnQualifier())
            .setValue(encodePrevEndRow(per).get());
    mutation.addCondition(c);
    return this;
  }

  @Override
  public Ample.ConditionalTabletMutator requireAbsentTablet() {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    IteratorSetting is = new IteratorSetting(INITIAL_ITERATOR_PRIO, TabletExistsIterator.class);
    Condition c = new Condition("", "").setIterators(is);
    mutation.addCondition(c);
    sawOperationRequirement = true;
    return this;
  }

  @Override
  public Ample.ConditionalTabletMutator requireAbsentOperation() {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    Condition c = new Condition(OPID_COLUMN.getColumnFamily(), OPID_COLUMN.getColumnQualifier());
    mutation.addCondition(c);
    sawOperationRequirement = true;
    return this;
  }

  @Override
  public Ample.ConditionalTabletMutator requireOperation(TabletOperation operation,
      TabletOperationId opid) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    Condition c = new Condition(OPID_COLUMN.getColumnFamily(), OPID_COLUMN.getColumnQualifier())
        .setValue(operation.name() + ":" + opid.canonical());
    mutation.addCondition(c);
    sawOperationRequirement = true;
    return this;
  }

  @Override
  public Ample.ConditionalTabletsMutator submit() {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    Preconditions.checkState(sawOperationRequirement, "No operation requirements were seen");
    getMutation();
    mutationConsumer.accept(mutation);
    return parent;
  }
}

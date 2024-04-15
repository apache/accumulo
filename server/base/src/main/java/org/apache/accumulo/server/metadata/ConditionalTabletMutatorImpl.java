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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.OPID_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.SELECTED_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn.SUSPEND_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.AVAILABILITY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.encodePrevEndRow;

import java.util.HashSet;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.TabletAvailabilityUtil;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.SortedFilesIterator;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CompactedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.UserCompactionRequestedColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMutatorBase;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metadata.iterators.LocationExistsIterator;
import org.apache.accumulo.server.metadata.iterators.PresentIterator;
import org.apache.accumulo.server.metadata.iterators.SetEqualityIterator;
import org.apache.accumulo.server.metadata.iterators.TabletExistsIterator;

import com.google.common.base.Preconditions;

public class ConditionalTabletMutatorImpl extends TabletMutatorBase<Ample.ConditionalTabletMutator>
    implements Ample.ConditionalTabletMutator, Ample.OperationRequirements {

  public static final int INITIAL_ITERATOR_PRIO = 1000000;

  private final ConditionalMutation mutation;
  private final Consumer<ConditionalMutation> mutationConsumer;
  private final Ample.ConditionalTabletsMutator parent;

  private final BiConsumer<KeyExtent,Ample.RejectionHandler> rejectionHandlerConsumer;

  private final ServerContext context;
  private final ServiceLock lock;
  private final KeyExtent extent;

  private boolean sawOperationRequirement = false;
  private boolean checkPrevEndRow = true;

  protected ConditionalTabletMutatorImpl(Ample.ConditionalTabletsMutator parent,
      ServerContext context, KeyExtent extent, Consumer<ConditionalMutation> mutationConsumer,
      BiConsumer<KeyExtent,Ample.RejectionHandler> rejectionHandlerConsumer) {
    super(new ConditionalMutation(extent.toMetaRow()));
    this.mutation = (ConditionalMutation) super.mutation;
    this.mutationConsumer = mutationConsumer;
    this.parent = parent;
    this.rejectionHandlerConsumer = rejectionHandlerConsumer;
    this.extent = extent;
    this.context = context;
    this.lock = this.context.getServiceLock();
    Objects.requireNonNull(this.lock, "ServiceLock not set on ServerContext");
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
  public Ample.ConditionalTabletMutator requireLocation(Location location) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    Preconditions.checkArgument(location.getType() == TabletMetadata.LocationType.FUTURE
        || location.getType() == TabletMetadata.LocationType.CURRENT);
    sawOperationRequirement = true;
    Condition c = new Condition(getLocationFamily(location.getType()), location.getSession())
        .setValue(location.getHostPort());
    mutation.addCondition(c);
    return this;
  }

  @Override
  public Ample.ConditionalTabletMutator
      requireTabletAvailability(TabletAvailability tabletAvailability) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    Condition c = new Condition(AVAILABILITY_COLUMN.getColumnFamily(),
        AVAILABILITY_COLUMN.getColumnQualifier())
        .setValue(TabletAvailabilityUtil.toValue(tabletAvailability).get());
    mutation.addCondition(c);
    return this;
  }

  @Override
  public Ample.ConditionalTabletMutator requireCompaction(ExternalCompactionId ecid) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    IteratorSetting is = new IteratorSetting(INITIAL_ITERATOR_PRIO, PresentIterator.class);
    Condition c = new Condition(ExternalCompactionColumnFamily.STR_NAME, ecid.canonical())
        .setValue(PresentIterator.VALUE).setIterators(is);
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
    checkPrevEndRow = false;
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
  public Ample.ConditionalTabletMutator requireOperation(TabletOperationId opid) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    Condition c = new Condition(OPID_COLUMN.getColumnFamily(), OPID_COLUMN.getColumnQualifier())
        .setValue(opid.canonical());
    mutation.addCondition(c);
    sawOperationRequirement = true;
    return this;
  }

  private void requireSameSingle(TabletMetadata tabletMetadata, ColumnType type) {
    switch (type) {
      case PREV_ROW:
        throw new IllegalStateException("PREV_ROW already set from Extent");
      case LOGS: {
        Condition c = SetEqualityIterator.createCondition(new HashSet<>(tabletMetadata.getLogs()),
            logEntry -> logEntry.getColumnQualifier().toString().getBytes(UTF_8),
            LogColumnFamily.NAME);
        mutation.addCondition(c);
      }
        break;
      case FILES: {
        // ELASTICITY_TODO compare values?
        Condition c = SetEqualityIterator.createCondition(tabletMetadata.getFiles(),
            stf -> stf.getMetadata().getBytes(UTF_8), DataFileColumnFamily.NAME);
        mutation.addCondition(c);
      }
        break;
      case SELECTED: {
        Condition c =
            new Condition(SELECTED_COLUMN.getColumnFamily(), SELECTED_COLUMN.getColumnQualifier());
        if (tabletMetadata.getSelectedFiles() != null) {
          // ensure the SelectedFiles metadata value is re-encoded in case it was manually edited
          c.setIterators(new IteratorSetting(INITIAL_ITERATOR_PRIO, SortedFilesIterator.class));
          c = c.setValue(
              Objects.requireNonNull(tabletMetadata.getSelectedFiles().getMetadataValue()));
        }
        mutation.addCondition(c);
      }
        break;
      case ECOMP: {
        Condition c =
            SetEqualityIterator.createCondition(tabletMetadata.getExternalCompactions().keySet(),
                ecid -> ecid.canonical().getBytes(UTF_8), ExternalCompactionColumnFamily.NAME);
        mutation.addCondition(c);
      }
        break;
      case LOCATION:
        if (tabletMetadata.getLocation() == null) {
          requireAbsentLocation();
        } else {
          requireLocation(tabletMetadata.getLocation());
        }
        break;
      case LOADED: {
        Condition c = SetEqualityIterator.createCondition(tabletMetadata.getLoaded().keySet(),
            stf -> stf.getMetadata().getBytes(UTF_8), BulkFileColumnFamily.NAME);
        mutation.addCondition(c);
      }
        break;
      case COMPACTED: {
        Condition c = SetEqualityIterator.createCondition(tabletMetadata.getCompacted(),
            fTid -> fTid.canonical().getBytes(UTF_8), CompactedColumnFamily.NAME);
        mutation.addCondition(c);
      }
        break;
      case TIME: {
        Condition c =
            new Condition(TIME_COLUMN.getColumnFamily(), TIME_COLUMN.getColumnQualifier());
        c = c.setValue(tabletMetadata.getTime().encode());
        mutation.addCondition(c);
      }
        break;
      case FLUSH_ID: {
        Condition c =
            new Condition(FLUSH_COLUMN.getColumnFamily(), FLUSH_COLUMN.getColumnQualifier());
        if (tabletMetadata.getFlushId().isPresent()) {
          c = c.setValue(Long.toString(tabletMetadata.getFlushId().getAsLong()));
        }
        mutation.addCondition(c);
      }
        break;
      case USER_COMPACTION_REQUESTED: {
        Condition c =
            SetEqualityIterator.createCondition(tabletMetadata.getUserCompactionsRequested(),
                fTid -> fTid.canonical().getBytes(UTF_8), UserCompactionRequestedColumnFamily.NAME);
        mutation.addCondition(c);
      }
        break;
      case SUSPEND: {
        Condition c =
            new Condition(SUSPEND_COLUMN.getColumnFamily(), SUSPEND_COLUMN.getColumnQualifier());
        if (tabletMetadata.getSuspend() != null) {
          c.setValue(tabletMetadata.getSuspend().server + "|"
              + tabletMetadata.getSuspend().suspensionTime);
        }
        mutation.addCondition(c);
      }
        break;
      default:
        throw new UnsupportedOperationException("Column type " + type + " is not supported.");
    }
  }

  @Override
  public Ample.ConditionalTabletMutator requireSame(TabletMetadata tabletMetadata, ColumnType type,
      ColumnType... otherTypes) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    requireSameSingle(tabletMetadata, type);
    for (var ct : otherTypes) {
      requireSameSingle(tabletMetadata, ct);
    }
    return this;
  }

  @Override
  public Ample.ConditionalTabletMutator requireAbsentLogs() {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    // create a tablet metadata with an empty set of logs and require the same as that
    requireSameSingle(TabletMetadata.builder(extent).build(ColumnType.LOGS), ColumnType.LOGS);
    return this;
  }

  @Override
  public void submit(Ample.RejectionHandler rejectionCheck) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    Preconditions.checkState(sawOperationRequirement, "No operation requirements were seen");
    if (checkPrevEndRow) {
      Condition c =
          new Condition(PREV_ROW_COLUMN.getColumnFamily(), PREV_ROW_COLUMN.getColumnQualifier())
              .setValue(encodePrevEndRow(extent.prevEndRow()).get());
      mutation.addCondition(c);
    }
    this.putZooLock(context.getZooKeeperRoot(), lock);
    getMutation();
    mutationConsumer.accept(mutation);
    rejectionHandlerConsumer.accept(extent, rejectionCheck);
  }
}

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
package org.apache.accumulo.manager.tableOps.split;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;

import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.UnSplittableMetadata;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.split.SplitUtils;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FindSplits extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(PreSplit.class);
  private final SplitInfo splitInfo;

  public FindSplits(KeyExtent extent) {
    this.splitInfo = new SplitInfo(extent, new TreeSet<>());
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    var extent = splitInfo.getOriginal();
    var ample = manager.getContext().getAmple();
    var tabletMetadata = ample.readTablet(extent);
    Optional<UnSplittableMetadata> computedUnsplittable = Optional.empty();

    if (tabletMetadata == null) {
      log.trace("Table {} no longer exist, so not gonna try to find a split point for it", extent);
      return null;
    }

    if (tabletMetadata.getOperationId() != null) {
      log.debug("Not splitting {} because it has operation id {}", tabletMetadata.getExtent(),
          tabletMetadata.getOperationId());
      return null;
    }

    // The TabletManagementIterator should not be trying to split if the tablet was marked
    // as unsplittable and the metadata hasn't changed so check that the metadata is different
    if (tabletMetadata.getUnSplittable() != null) {
      computedUnsplittable =
          Optional.of(SplitUtils.toUnSplittable(manager.getContext(), tabletMetadata));
      Preconditions.checkState(
          !tabletMetadata.getUnSplittable().equals(computedUnsplittable.orElseThrow()),
          "Unexpected split attempted on tablet %s that was marked as unsplittable", extent);
    }

    if (!tabletMetadata.getLogs().isEmpty()) {
      // This code is only called by system initiated splits, so if walogs are present it probably
      // makes sense to wait for the data in them to be written to a file before finding splits
      // points.
      log.debug("Not splitting {} because it has walogs {}", tabletMetadata.getExtent(),
          tabletMetadata.getLogs().size());
      return null;
    }

    var estimatedSize =
        tabletMetadata.getFilesMap().values().stream().mapToLong(DataFileValue::getSize).sum();
    SortedSet<Text> splits =
        SplitUtils.findSplits(manager.getContext(), tabletMetadata, estimatedSize);

    if (extent.endRow() != null) {
      splits.remove(extent.endRow());
    }

    if (splits.isEmpty()) {
      Consumer<ConditionalResult> resultConsumer = result -> {
        if (result.getStatus() == Status.REJECTED) {
          log.debug("{} unsplittable metadata update for {} was rejected ", fateId,
              result.getExtent());
        }
      };

      try (var tabletsMutator = ample.conditionallyMutateTablets(resultConsumer)) {
        // Check if we still need to split. It's possible we don't if the unsplittable marker
        // has already been previously set. This could happen in some scenarios such as
        // a compaction that shrinks a previously unsplittable tablet below the threshold
        // or if the threshold has been raised higher because the tablet management iterator
        // will try and split any time the computed metadata changes.
        if (stillNeedsSplit(manager.getContext(), tabletMetadata, estimatedSize)) {
          log.info("Tablet {} needs to split, but no split points could be found.",
              tabletMetadata.getExtent());
          var unSplittableMeta = computedUnsplittable
              .orElseGet(() -> SplitUtils.toUnSplittable(manager.getContext(), tabletMetadata));

          // With the current design we don't need to require the files to be the same
          // for correctness as the TabletManagementIterator will detect the difference
          // when computing the hash and retry a new split operation if there is not a match.
          // But if we already know there's a change now, it would be more efficient to fail and
          // retry the current fate op vs completing and having the iterator submit a new one.
          log.debug("Setting unsplittable metadata on tablet {}. hashCode: {}",
              tabletMetadata.getExtent(), unSplittableMeta);
          var mutator = tabletsMutator.mutateTablet(extent).requireAbsentOperation()
              .requireSame(tabletMetadata, FILES).setUnSplittable(unSplittableMeta);
          mutator.submit(tm -> unSplittableMeta.equals(tm.getUnSplittable()));
        } else {
          // We no longer need to split so we can clear the marker.
          log.info("Tablet {} no longer needs to split, deleting unsplittable marker.",
              tabletMetadata.getExtent());
          var mutator = tabletsMutator.mutateTablet(extent).requireAbsentOperation()
              .requireSame(tabletMetadata, FILES).deleteUnSplittable();
          mutator.submit(tm -> tm.getUnSplittable() == null);
        }
      }

      return null;
    }

    return new PreSplit(extent, splits);
  }

  private boolean stillNeedsSplit(ServerContext context, TabletMetadata tabletMetadata,
      long estimatedSize) {
    if (tabletMetadata.getUnSplittable() != null) {
      // Recheck threshold if existing marker exists
      var tableConf = context.getTableConfiguration(tabletMetadata.getTableId());
      var splitThreshold = tableConf.getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
      return estimatedSize > splitThreshold;
    }
    return true;
  }

}

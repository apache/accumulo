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
package org.apache.accumulo.manager.tableOps.bulkVer2;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.LockRange;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.logging.BulkLogger;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.util.bulkCommand.ListBulk.BulkState;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanUpBulkImport extends AbstractBulkFateOperation {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(CleanUpBulkImport.class);

  public CleanUpBulkImport(BulkInfo info) {
    super(info);
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv env) throws Exception {
    log.debug("{} removing the bulkDir processing flag file in {}", fateId, bulkInfo.bulkDir);
    Ample ample = env.getContext().getAmple();
    Path bulkDir = new Path(bulkInfo.bulkDir);
    ample.removeBulkLoadInProgressFlag(
        "/" + bulkDir.getParent().getName() + "/" + bulkDir.getName());
    ample.putGcFileAndDirCandidates(bulkInfo.tableId,
        Collections.singleton(ReferenceFile.forFile(bulkInfo.tableId, bulkDir)));

    Text firstSplit = bulkInfo.firstSplit == null ? null : new Text(bulkInfo.firstSplit);
    Text lastSplit = bulkInfo.lastSplit == null ? null : new Text(bulkInfo.lastSplit);

    log.debug("{} removing the metadata table markers for loaded files in range {} {}", fateId,
        firstSplit, lastSplit);
    removeBulkLoadEntries(ample, bulkInfo.tableId, fateId, firstSplit, lastSplit);

    Utils.unreserveHdfsDirectory(env.getContext(), bulkInfo.sourceDir, fateId);
    Utils.getReadLock(env.getContext(), bulkInfo.tableId, fateId, LockRange.infinite()).unlock();
    // delete json renames and mapping files
    Path renamingFile = new Path(bulkDir, Constants.BULK_RENAME_FILE);
    Path mappingFile = new Path(bulkDir, Constants.BULK_LOAD_MAPPING);
    try {
      env.getVolumeManager().delete(renamingFile);
      env.getVolumeManager().delete(mappingFile);
    } catch (IOException ioe) {
      log.debug("{} Failed to delete renames and/or loadmap", fateId, ioe);
    }

    log.debug("completing bulkDir import transaction " + fateId);
    return null;
  }

  private static void removeBulkLoadEntries(Ample ample, TableId tableId, FateId fateId,
      Text firstSplit, Text lastSplit) {

    Retry retry = Retry.builder().infiniteRetries().retryAfter(Duration.ofMillis(100))
        .incrementBy(Duration.ofMillis(100)).maxWait(Duration.ofSeconds(1)).backOffFactor(1.5)
        .logInterval(Duration.ofMinutes(3)).createRetry();

    while (true) {
      try (
          var tablets = ample.readTablets().forTable(tableId).overlapping(firstSplit, lastSplit)
              .checkConsistency().fetch(ColumnType.PREV_ROW, ColumnType.LOADED).build();
          var tabletsMutator = ample.conditionallyMutateTablets()) {

        for (var tablet : tablets) {
          if (tablet.getLoaded().values().stream()
              .anyMatch(loadedFateId -> loadedFateId.equals(fateId))) {
            var tabletMutator =
                tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation();
            tablet.getLoaded().entrySet().stream().filter(entry -> entry.getValue().equals(fateId))
                .peek(entry -> BulkLogger.deletingLoadEntry(tablet.getExtent(), entry))
                .map(Map.Entry::getKey).forEach(tabletMutator::deleteBulkFile);
            tabletMutator.submit(tm -> false, () -> "remove bulk load entries " + fateId);
          }
        }

        var results = tabletsMutator.process();

        if (results.values().stream()
            .anyMatch(condResult -> condResult.getStatus() != Status.ACCEPTED)) {

          results.forEach((extent, condResult) -> {
            if (condResult.getStatus() != Status.ACCEPTED) {
              var metadata = Optional.ofNullable(condResult.readMetadata());
              log.debug("Tablet update failed {} {} {} {} ", fateId, extent, condResult.getStatus(),
                  metadata.map(TabletMetadata::getOperationId).map(AbstractId::toString)
                      .orElse("tablet is gone"));
            }
          });

          try {
            retry.waitForNextAttempt(log,
                String.format("%s tableId:%s conditional mutations to delete load markers failed.",
                    fateId, tableId));
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        } else {
          break;
        }
      }
    }
  }

  @Override
  public BulkState getState() {
    return BulkState.CLEANING;
  }
}

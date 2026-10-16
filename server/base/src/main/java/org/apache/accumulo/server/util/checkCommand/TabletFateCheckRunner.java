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
package org.apache.accumulo.server.util.checkCommand;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.adminCommand.SystemCheck.Check;

public class TabletFateCheckRunner implements CheckRunner {
  private static final Check check = Check.BULK_FATE;

  @Override
  public boolean runCheck(ServerContext context, ServerOpts opts, boolean fixFiles)
      throws Exception {
    boolean status = true;
    printRunning();

    log.trace("********** Checking for orphaned bulk-import loaded columns **********");

    final Set<FateId> initialMetadataIds = new HashSet<>();
    for (Ample.DataLevel level : Ample.DataLevel.values()) {
      try (
          TabletsMetadata tabletsMetadata =
              context
                  .getAmple().readTablets().forLevel(level).fetch(TabletMetadata.ColumnType.LOADED,
                      TabletMetadata.ColumnType.SELECTED, TabletMetadata.ColumnType.PREV_ROW)
                  .build()) {
        for (TabletMetadata tablet : tabletsMetadata) {
          tablet.getLoaded().values().forEach(initialMetadataIds::add);
          SelectedFiles selectedFiles = tablet.getSelectedFiles();
          if (selectedFiles != null) {
            initialMetadataIds.add(selectedFiles.getFateId());
          }
        }
      }
    }
    log.trace("Found {} fate ids in initial metadata scan", initialMetadataIds.size());

    final EnumSet<ReadOnlyFateStore.TStatus> tStatuses =
        EnumSet.of(ReadOnlyFateStore.TStatus.IN_PROGRESS,
            ReadOnlyFateStore.TStatus.FAILED_IN_PROGRESS, ReadOnlyFateStore.TStatus.SUBMITTED);
    final Set<Fate.FateOperation> fateOperations =
        Set.of(Fate.FateOperation.TABLE_COMPACT, Fate.FateOperation.TABLE_BULK_IMPORT2);
    final Set<FateId> liveFateIds = new HashSet<>();
    try (
        MetaFateStore<TabletFateCheckRunner> mfs =
            new MetaFateStore<>(context.getZooSession(), null, null);
        UserFateStore<TabletFateCheckRunner> ufs =
            new UserFateStore<>(context, SystemTables.FATE.tableName(), null, null)) {

      mfs.list(tStatuses)
          .filter(fis -> fis.getFateOperation().map(fateOperations::contains).orElse(false))
          .map(ReadOnlyFateStore.FateIdStatus::getFateId).forEach(liveFateIds::add);
      ufs.list(tStatuses)
          .filter(fis -> fis.getFateOperation().map(fateOperations::contains).orElse(false))
          .map(ReadOnlyFateStore.FateIdStatus::getFateId).forEach(liveFateIds::add);
    }

    log.trace("Found {} live FATE operations", liveFateIds.size());

    for (Ample.DataLevel level : Ample.DataLevel.values()) {
      try (
          TabletsMetadata tabletsMetadata =
              context
                  .getAmple().readTablets().forLevel(level).fetch(TabletMetadata.ColumnType.LOADED,
                      TabletMetadata.ColumnType.SELECTED, TabletMetadata.ColumnType.PREV_ROW)
                  .build()) {

        for (TabletMetadata tablet : tabletsMetadata) {

          // Check loaded columns
          Map<StoredTabletFile,FateId> loaded = tablet.getLoaded();
          for (Map.Entry<StoredTabletFile,FateId> entry : loaded.entrySet()) {
            FateId fateId = entry.getValue();
            if (!liveFateIds.contains(fateId) && initialMetadataIds.contains(fateId)) {
              log.warn(
                  "Tablet {} has loaded column for file {} referencing dead FATE op {} - "
                      + "investigate and clean up manually",
                  tablet.getExtent(), entry.getKey().getMetadataPath(), fateId);
              status = false;
            }
          }

          // Check selected columns
          SelectedFiles selectedFiles = tablet.getSelectedFiles();
          if (selectedFiles != null) {
            FateId fateId = selectedFiles.getFateId();
            if (!liveFateIds.contains(fateId) && initialMetadataIds.contains(fateId)) {
              log.warn("Tablet {} has selected column referencing dead FATE op {} - "
                  + "investigate and clean up manually", tablet.getExtent(), fateId);
              status = false;
            }
          }
        }
      }
    }

    printCompleted(status);
    return status;
  }

  @Override
  public Check getCheck() {
    return check;
  }
}

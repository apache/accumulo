/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.tableOps.bulkVer2;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.bulk.BulkImport;
import org.apache.accumulo.core.clientImpl.bulk.BulkSerialize;
import org.apache.accumulo.core.clientImpl.bulk.LoadMappingIterator;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;

/**
 * Prepare bulk import directory. This REPO creates a bulk directory in Accumulo, list all the files
 * in the original directory and creates a renaming file for moving the files (which happens next in
 * BulkImportMove). The renaming file has a mapping of originalPath to newPath. The newPath will be
 * the bulk directory in Accumulo. The renaming file is called {@value Constants#BULK_RENAME_FILE}
 * and is written to the {@value Constants#BULK_PREFIX} bulk directory generated here.
 *
 * @since 2.0.0
 */
public class PrepBulkImport extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(PrepBulkImport.class);

  private final BulkInfo bulkInfo;

  public PrepBulkImport(TableId tableId, String sourceDir, boolean setTime) {
    BulkInfo info = new BulkInfo();
    info.tableId = tableId;
    info.sourceDir = sourceDir;
    info.setTime = setTime;
    this.bulkInfo = info;
  }

  @Override
  public long isReady(long tid, Master master) throws Exception {
    if (!Utils.getReadLock(master, bulkInfo.tableId, tid).tryLock())
      return 100;

    if (master.onlineTabletServers().isEmpty())
      return 500;
    Tables.clearCache(master.getContext());

    return Utils.reserveHdfsDirectory(master, bulkInfo.sourceDir, tid);
  }

  @VisibleForTesting
  interface TabletIterFactory {
    Iterator<KeyExtent> newTabletIter(Text startRow);
  }

  private static boolean equals(Function<KeyExtent,Text> extractor, KeyExtent ke1, KeyExtent ke2) {
    return Objects.equals(extractor.apply(ke1), extractor.apply(ke2));
  }

  @VisibleForTesting
  static void checkForMerge(String tableId, Iterator<KeyExtent> lmi,
      TabletIterFactory tabletIterFactory) throws Exception {
    KeyExtent currRange = lmi.next();

    Text startRow = currRange.getPrevEndRow();

    Iterator<KeyExtent> tabletIter = tabletIterFactory.newTabletIter(startRow);

    KeyExtent currTablet = tabletIter.next();

    if (!tabletIter.hasNext() && equals(KeyExtent::getPrevEndRow, currTablet, currRange)
        && equals(KeyExtent::getEndRow, currTablet, currRange))
      currRange = null;

    while (tabletIter.hasNext()) {

      if (currRange == null) {
        if (!lmi.hasNext()) {
          break;
        }
        currRange = lmi.next();
      }

      while (!equals(KeyExtent::getPrevEndRow, currTablet, currRange) && tabletIter.hasNext()) {
        currTablet = tabletIter.next();
      }

      boolean matchedPrevRow = equals(KeyExtent::getPrevEndRow, currTablet, currRange);

      while (!equals(KeyExtent::getEndRow, currTablet, currRange) && tabletIter.hasNext()) {
        currTablet = tabletIter.next();
      }

      if (!matchedPrevRow || !equals(KeyExtent::getEndRow, currTablet, currRange)) {
        break;
      }

      currRange = null;
    }

    if (currRange != null || lmi.hasNext()) {
      // merge happened after the mapping was generated and before the table lock was acquired
      throw new AcceptableThriftTableOperationException(tableId, null, TableOperation.BULK_IMPORT,
          TableOperationExceptionType.BULK_CONCURRENT_MERGE, "Concurrent merge happened");
    }
  }

  private void checkForMerge(final Master master) throws Exception {

    VolumeManager fs = master.getVolumeManager();
    final Path bulkDir = new Path(bulkInfo.sourceDir);
    try (LoadMappingIterator lmi =
        BulkSerialize.readLoadMapping(bulkDir.toString(), bulkInfo.tableId, p -> fs.open(p))) {

      TabletIterFactory tabletIterFactory = startRow -> TabletsMetadata.builder()
          .forTable(bulkInfo.tableId).overlapping(startRow, null).checkConsistency().fetch(PREV_ROW)
          .build(master.getContext()).stream().map(TabletMetadata::getExtent).iterator();

      checkForMerge(bulkInfo.tableId.canonical(), Iterators.transform(lmi, entry -> entry.getKey()),
          tabletIterFactory);
    }
  }

  @Override
  public Repo<Master> call(final long tid, final Master master) throws Exception {
    // now that table lock is acquired check that all splits in load mapping exists in table
    checkForMerge(master);

    bulkInfo.tableState = Tables.getTableState(master.getContext(), bulkInfo.tableId);

    VolumeManager fs = master.getVolumeManager();
    final UniqueNameAllocator namer = master.getContext().getUniqueNameAllocator();
    Path sourceDir = new Path(bulkInfo.sourceDir);
    List<FileStatus> files = BulkImport.filterInvalid(fs.listStatus(sourceDir));

    Path bulkDir = createNewBulkDir(master.getContext(), fs, bulkInfo.tableId);
    Path mappingFile = new Path(sourceDir, Constants.BULK_LOAD_MAPPING);

    Map<String,String> oldToNewNameMap = new HashMap<>();

    for (FileStatus file : files) {
      // since these are only valid files we know it has an extension
      String newName =
          "I" + namer.getNextName() + "." + FilenameUtils.getExtension(file.getPath().getName());
      oldToNewNameMap.put(file.getPath().getName(), new Path(bulkDir, newName).getName());
    }

    // also have to move mapping file
    oldToNewNameMap.put(mappingFile.getName(), new Path(bulkDir, mappingFile.getName()).getName());

    BulkSerialize.writeRenameMap(oldToNewNameMap, bulkDir.toString(), p -> fs.create(p));

    bulkInfo.bulkDir = bulkDir.toString();
    // return the next step, which will move files
    return new BulkImportMove(bulkInfo);
  }

  private Path createNewBulkDir(ServerContext context, VolumeManager fs, TableId tableId)
      throws IOException {
    Path tempPath =
        fs.matchingFileSystem(new Path(bulkInfo.sourceDir), ServerConstants.getTablesDirs(context));
    if (tempPath == null)
      throw new IOException(bulkInfo.sourceDir + " is not in a volume configured for Accumulo");

    String tableDir = tempPath.toString();
    if (tableDir == null)
      throw new IOException(bulkInfo.sourceDir + " is not in a volume configured for Accumulo");
    Path directory = new Path(tableDir + "/" + tableId);
    fs.mkdirs(directory);

    UniqueNameAllocator namer = context.getUniqueNameAllocator();
    while (true) {
      Path newBulkDir = new Path(directory, Constants.BULK_PREFIX + namer.getNextName());
      if (fs.mkdirs(newBulkDir))
        return newBulkDir;
      log.warn("Failed to create {} for unknown reason", newBulkDir);

      sleepUninterruptibly(3, TimeUnit.SECONDS);
    }
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    // unreserve sourceDir/error directories
    Utils.unreserveHdfsDirectory(environment, bulkInfo.sourceDir, tid);
    Utils.getReadLock(environment, bulkInfo.tableId, tid).unlock();
    TransactionWatcher.ZooArbitrator.cleanup(environment.getContext(),
        Constants.BULK_ARBITRATOR_TYPE, tid);
  }
}

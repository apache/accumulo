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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.bulk.Bulk;
import org.apache.accumulo.core.clientImpl.bulk.BulkImport;
import org.apache.accumulo.core.clientImpl.bulk.BulkSerialize;
import org.apache.accumulo.core.clientImpl.bulk.LoadMappingIterator;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock;
import org.apache.accumulo.core.fate.zookeeper.LockRange;
import org.apache.accumulo.core.file.FilePrefix;
import org.apache.accumulo.core.logging.BulkLogger;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.accumulo.server.util.bulkCommand.ListBulk.BulkState;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Prepare bulk import directory. This REPO creates a bulk directory in Accumulo, list all the files
 * in the original directory and creates a renaming file for moving the files (which happens next in
 * BulkImportMove). The renaming file has a mapping of originalPath to newPath. The newPath will be
 * the bulk directory in Accumulo. The renaming file is called {@value Constants#BULK_RENAME_FILE}
 * and is written to the {@value Constants#BULK_PREFIX} bulk directory generated here.
 *
 * @since 2.0.0
 */
public class PrepBulkImport extends AbstractBulkFateOperation {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(PrepBulkImport.class);

  public PrepBulkImport(BulkInfo info) {
    super(info);
  }

  @Override
  public long isReady(FateId fateId, FateEnv env) throws Exception {
    long wait = Utils.reserveTable(env.getContext(), bulkInfo.tableId, fateId,
        DistributedReadWriteLock.LockType.READ, true, TableOperation.BULK_IMPORT,
        LockRange.of(bulkInfo.firstSplit, bulkInfo.lastSplit));
    if (wait > 0) {
      return wait;
    }

    if (env.onlineTabletServers().isEmpty()) {
      return 500;
    }

    return Utils.reserveHdfsDirectory(env.getContext(), bulkInfo.sourceDir, fateId);
  }

  @Override
  public BulkState getState() {
    return BulkState.PREPARING;
  }

  @VisibleForTesting
  interface TabletIterFactory extends AutoCloseable {
    Iterator<KeyExtent> newTabletIter(Text startRow);

    @Override
    void close();
  }

  private static boolean equals(Function<KeyExtent,Text> extractor, KeyExtent ke1, KeyExtent ke2) {
    return Objects.equals(extractor.apply(ke1), extractor.apply(ke2));
  }

  /**
   * Checks a load mapping to ensure all of the rows in the mapping exists in the table and that no
   * file goes to too many tablets.
   */
  @VisibleForTesting
  static void validateLoadMapping(String tableId, LoadMappingIterator lmi,
      TabletIterFactory tabletIterFactory, int maxNumTablets, int maxFilesPerTablet, FateId fateId,
      int skip) throws Exception {

    var currRange = lmi.next();
    checkFilesPerTablet(tableId, maxFilesPerTablet, currRange);

    Text startRow = currRange.getKey().prevEndRow();

    PeekingIterator<KeyExtent> pi =
        new PeekingIterator<>(tabletIterFactory.newTabletIter(startRow));

    try {
      KeyExtent currTablet = pi.next();

      var fileCounts = new HashMap<String,Integer>();
      int count;

      if (!pi.hasNext() && equals(KeyExtent::prevEndRow, currTablet, currRange.getKey())
          && equals(KeyExtent::endRow, currTablet, currRange.getKey())) {
        currRange = null;
      }

      while (pi.hasNext()) {

        if (currRange == null) {
          if (!lmi.hasNext()) {
            break;
          }
          currRange = lmi.next();
          checkFilesPerTablet(tableId, maxFilesPerTablet, currRange);
        }
        // If the user set the TABLE_BULK_SKIP_THRESHOLD property, then only look
        // at the next skipDistance tablets before recreating the iterator
        if (!equals(KeyExtent::prevEndRow, currTablet, currRange.getKey()) && skip > 0
            && currRange.getKey().prevEndRow() != null) {
          final KeyExtent search = currRange.getKey();
          if (!pi.findWithin((ke) -> Objects.equals(ke.prevEndRow(), search.prevEndRow()), skip)) {
            log.debug(
                "Tablet metadata for prevEndRow {} not found in {} tablets from current tablet {}, recreating TabletMetadata to jump ahead",
                search.prevEndRow(), skip, currTablet);
            tabletIterFactory.close();
            pi = new PeekingIterator<>(tabletIterFactory.newTabletIter(search.prevEndRow()));
            currTablet = pi.next();
          }
        }
        while (!equals(KeyExtent::prevEndRow, currTablet, currRange.getKey()) && pi.hasNext()) {
          currTablet = pi.next();
        }

        boolean matchedPrevRow = equals(KeyExtent::prevEndRow, currTablet, currRange.getKey());

        count = matchedPrevRow ? 1 : 0;

        while (!equals(KeyExtent::endRow, currTablet, currRange.getKey()) && pi.hasNext()) {
          currTablet = pi.next();
          count++;
        }

        if (!matchedPrevRow || !equals(KeyExtent::endRow, currTablet, currRange.getKey())) {
          break;
        }
        if (maxNumTablets > 0) {
          int fc = count;
          currRange.getValue()
              .forEach(fileInfo -> fileCounts.merge(fileInfo.getFileName(), fc, Integer::sum));
        }
        currRange = null;
      }

      if (currRange != null || lmi.hasNext()) {
        // merge happened after the mapping was generated and before the table lock was acquired
        throw new AcceptableThriftTableOperationException(tableId, null, TableOperation.BULK_IMPORT,
            TableOperationExceptionType.BULK_CONCURRENT_MERGE, "Concurrent merge happened");
      }

      if (maxNumTablets > 0) {
        fileCounts.values().removeIf(c -> c <= maxNumTablets);
        if (!fileCounts.isEmpty()) {
          throw new AcceptableThriftTableOperationException(tableId, null,
              TableOperation.BULK_IMPORT, TableOperationExceptionType.OTHER,
              "Files overlap the configured max (" + maxNumTablets + ") number of tablets: "
                  + new TreeMap<>(fileCounts));
        }
      }
    } finally {
      tabletIterFactory.close();
    }
  }

  private static void checkFilesPerTablet(String tableId, int maxFilesPerTablet,
      Map.Entry<KeyExtent,Bulk.Files> currRange) throws AcceptableThriftTableOperationException {
    if (maxFilesPerTablet > 0 && currRange.getValue().getSize() > maxFilesPerTablet) {
      throw new AcceptableThriftTableOperationException(tableId, null, TableOperation.BULK_IMPORT,
          TableOperationExceptionType.OTHER,
          "Attempted to import " + currRange.getValue().getSize() + " files into tablets in range "
              + currRange.getKey() + " which exceeds the configured max files per tablet of "
              + maxFilesPerTablet + " from " + Property.TABLE_BULK_MAX_TABLET_FILES.getKey());
    }
  }

  private void checkForMerge(final ServerContext ctx, final FateId fateId) throws Exception {

    VolumeManager fs = ctx.getVolumeManager();
    final Path bulkDir = new Path(bulkInfo.sourceDir);

    var tableConfig = ctx.getTableConfiguration(bulkInfo.tableId);
    int maxTablets = tableConfig.getCount(Property.TABLE_BULK_MAX_TABLETS);
    int maxFilesPerTablet = tableConfig.getCount(Property.TABLE_BULK_MAX_TABLET_FILES);

    try (LoadMappingIterator lmi =
        BulkSerialize.readLoadMapping(bulkDir.toString(), bulkInfo.tableId, fs::open)) {

      TabletIterFactory tabletIterFactory = new TabletIterFactory() {

        TabletsMetadata tm = null;

        @Override
        public Iterator<KeyExtent> newTabletIter(Text startRow) {
          tm = TabletsMetadata.builder(ctx).forTable(bulkInfo.tableId).overlapping(startRow, null)
              .checkConsistency().fetch(PREV_ROW).build();
          return tm.stream().map(TabletMetadata::getExtent).iterator();
        }

        @Override
        public void close() {
          if (tm != null) {
            tm.close();
          }
        }
      };

      int skip = tableConfig.getCount(Property.TABLE_BULK_SKIP_THRESHOLD);
      validateLoadMapping(bulkInfo.tableId.canonical(), lmi, tabletIterFactory, maxTablets,
          maxFilesPerTablet, fateId, skip);
    }
  }

  @Override
  public Repo<FateEnv> call(final FateId fateId, final FateEnv env) throws Exception {
    // now that table lock is acquired check that all splits in load mapping exists in table
    checkForMerge(env.getContext(), fateId);

    VolumeManager fs = env.getVolumeManager();
    final UniqueNameAllocator namer = env.getContext().getUniqueNameAllocator();
    Path sourceDir = new Path(bulkInfo.sourceDir);
    List<FileStatus> files = BulkImport.filterInvalid(fs.listStatus(sourceDir));

    Path bulkDir = createNewBulkDir(env.getContext(), fs, bulkInfo.tableId);
    Path mappingFile = new Path(sourceDir, Constants.BULK_LOAD_MAPPING);

    Map<String,String> oldToNewNameMap = new HashMap<>();

    Iterator<String> names = namer.getNextNames(files.size());

    for (FileStatus file : files) {
      // since these are only valid files we know it has an extension
      String newName = FilePrefix.BULK_IMPORT.createFileName(
          names.next() + "." + FilenameUtils.getExtension(file.getPath().getName()));
      oldToNewNameMap.put(file.getPath().getName(), new Path(bulkDir, newName).getName());
    }

    // also have to move mapping file
    oldToNewNameMap.put(mappingFile.getName(), new Path(bulkDir, mappingFile.getName()).getName());

    BulkSerialize.writeRenameMap(oldToNewNameMap, bulkDir.toString(), fs::create);

    bulkInfo.bulkDir = bulkDir.toString();

    BulkLogger.initiating(fateId, bulkInfo.tableId, bulkInfo.setTime, bulkInfo.sourceDir,
        bulkInfo.bulkDir);

    // return the next step, which will move files
    return new BulkImportMove(bulkInfo);
  }

  private Path createNewBulkDir(ServerContext context, VolumeManager fs, TableId tableId)
      throws IOException {
    Path tableDir = fs.matchingFileSystem(new Path(bulkInfo.sourceDir), context.getTablesDirs());
    if (tableDir == null) {
      throw new IOException(bulkInfo.sourceDir
          + " is not in the same file system as any volume configured for Accumulo");
    }

    Path directory = new Path(tableDir, tableId.canonical());
    fs.mkdirs(directory);

    UniqueNameAllocator namer = context.getUniqueNameAllocator();
    while (true) {
      Path newBulkDir = new Path(directory, Constants.BULK_PREFIX + namer.getNextName());
      if (fs.mkdirs(newBulkDir)) {
        return newBulkDir;
      }
      log.warn("Failed to create {} for unknown reason", newBulkDir);

      sleepUninterruptibly(3, TimeUnit.SECONDS);
    }
  }

  @Override
  public void undo(FateId fateId, FateEnv environment) throws Exception {
    // unreserve sourceDir/error directories
    Utils.unreserveHdfsDirectory(environment.getContext(), bulkInfo.sourceDir, fateId);
    Utils.getReadLock(environment.getContext(), bulkInfo.tableId, fateId, LockRange.infinite())
        .unlock();
  }
}

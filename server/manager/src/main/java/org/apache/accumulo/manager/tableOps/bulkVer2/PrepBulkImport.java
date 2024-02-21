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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
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
import org.apache.accumulo.core.file.FilePrefix;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
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
public class PrepBulkImport extends ManagerRepo {

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
  public long isReady(FateId fateId, Manager manager) throws Exception {
    if (!Utils.getReadLock(manager, bulkInfo.tableId, fateId).tryLock()) {
      return 100;
    }

    if (manager.onlineTabletServers().isEmpty()) {
      return 500;
    }

    return Utils.reserveHdfsDirectory(manager, bulkInfo.sourceDir, fateId);
  }

  @VisibleForTesting
  interface TabletIterFactory extends AutoCloseable {
    Iterator<KeyExtent> newTabletIter(Text startRow);
  }

  private static boolean equals(Function<KeyExtent,Text> extractor, KeyExtent ke1, KeyExtent ke2) {
    return Objects.equals(extractor.apply(ke1), extractor.apply(ke2));
  }

  /**
   * Checks a load mapping to ensure all of the rows in the mapping exists in the table and that no
   * file goes to too many tablets.
   */
  @VisibleForTesting
  static KeyExtent validateLoadMapping(String tableId, LoadMappingIterator lmi,
      TabletIterFactory tabletIterFactory, int maxNumTablets) throws Exception {
    var currRange = lmi.next();

    Text startRow = currRange.getKey().prevEndRow();

    Iterator<KeyExtent> tabletIter = tabletIterFactory.newTabletIter(startRow);

    KeyExtent currTablet = tabletIter.next();

    var fileCounts = new HashMap<String,Integer>();
    int count;

    KeyExtent firstTablet = currRange.getKey();
    KeyExtent lastTablet = currRange.getKey();

    if (!tabletIter.hasNext() && equals(KeyExtent::prevEndRow, currTablet, currRange.getKey())
        && equals(KeyExtent::endRow, currTablet, currRange.getKey())) {
      currRange = null;
    }

    while (tabletIter.hasNext()) {

      if (currRange == null) {
        if (!lmi.hasNext()) {
          break;
        }
        currRange = lmi.next();
        lastTablet = currRange.getKey();
      }

      while (!equals(KeyExtent::prevEndRow, currTablet, currRange.getKey())
          && tabletIter.hasNext()) {
        currTablet = tabletIter.next();
      }

      boolean matchedPrevRow = equals(KeyExtent::prevEndRow, currTablet, currRange.getKey());

      if (matchedPrevRow && firstTablet == null) {
        firstTablet = currTablet;
      }

      count = matchedPrevRow ? 1 : 0;

      while (!equals(KeyExtent::endRow, currTablet, currRange.getKey()) && tabletIter.hasNext()) {
        currTablet = tabletIter.next();
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
        throw new AcceptableThriftTableOperationException(tableId, null, TableOperation.BULK_IMPORT,
            TableOperationExceptionType.OTHER, "Files overlap the configured max (" + maxNumTablets
                + ") number of tablets: " + new TreeMap<>(fileCounts));
      }
    }

    return new KeyExtent(firstTablet.tableId(), lastTablet.endRow(), firstTablet.prevEndRow());
  }

  private static class TabletIterFactoryImpl implements TabletIterFactory {
    private final List<AutoCloseable> resourcesToClose = new ArrayList<>();
    private final Manager manager;
    private final BulkInfo bulkInfo;

    public TabletIterFactoryImpl(Manager manager, BulkInfo bulkInfo) {
      this.manager = manager;
      this.bulkInfo = bulkInfo;
    }

    @Override
    public Iterator<KeyExtent> newTabletIter(Text startRow) {
      TabletsMetadata tabletsMetadata =
          TabletsMetadata.builder(manager.getContext()).forTable(bulkInfo.tableId)
              .overlapping(startRow, null).checkConsistency().fetch(PREV_ROW).build();
      resourcesToClose.add(tabletsMetadata);
      return tabletsMetadata.stream().map(TabletMetadata::getExtent).iterator();
    }

    @Override
    public void close() throws Exception {
      for (AutoCloseable resource : resourcesToClose) {
        resource.close();
      }
    }
  }

  private KeyExtent checkForMerge(final Manager manager) throws Exception {

    VolumeManager fs = manager.getVolumeManager();
    final Path bulkDir = new Path(bulkInfo.sourceDir);

    int maxTablets = manager.getContext().getTableConfiguration(bulkInfo.tableId)
        .getCount(Property.TABLE_BULK_MAX_TABLETS);

    try (
        LoadMappingIterator lmi =
            BulkSerialize.readLoadMapping(bulkDir.toString(), bulkInfo.tableId, fs::open);
        TabletIterFactory tabletIterFactory = new TabletIterFactoryImpl(manager, bulkInfo)) {
      return validateLoadMapping(bulkInfo.tableId.canonical(), lmi, tabletIterFactory, maxTablets);
    }
  }

  @Override
  public Repo<Manager> call(final FateId fateId, final Manager manager) throws Exception {
    // now that table lock is acquired check that all splits in load mapping exists in table
    KeyExtent tabletsRange = checkForMerge(manager);

    bulkInfo.firstSplit =
        Optional.ofNullable(tabletsRange.prevEndRow()).map(Text::getBytes).orElse(null);
    bulkInfo.lastSplit =
        Optional.ofNullable(tabletsRange.endRow()).map(Text::getBytes).orElse(null);

    log.trace("{} first split:{} last split:{}", fateId, tabletsRange.prevEndRow(),
        tabletsRange.endRow());

    VolumeManager fs = manager.getVolumeManager();
    final UniqueNameAllocator namer = manager.getContext().getUniqueNameAllocator();
    Path sourceDir = new Path(bulkInfo.sourceDir);
    List<FileStatus> files = BulkImport.filterInvalid(fs.listStatus(sourceDir));

    Path bulkDir = createNewBulkDir(manager.getContext(), fs, bulkInfo.tableId);
    Path mappingFile = new Path(sourceDir, Constants.BULK_LOAD_MAPPING);

    Map<String,String> oldToNewNameMap = new HashMap<>();

    for (FileStatus file : files) {
      // since these are only valid files we know it has an extension
      String newName = FilePrefix.BULK_IMPORT.toPrefix() + namer.getNextName() + "."
          + FilenameUtils.getExtension(file.getPath().getName());
      oldToNewNameMap.put(file.getPath().getName(), new Path(bulkDir, newName).getName());
    }

    // also have to move mapping file
    oldToNewNameMap.put(mappingFile.getName(), new Path(bulkDir, mappingFile.getName()).getName());

    BulkSerialize.writeRenameMap(oldToNewNameMap, bulkDir.toString(), fs::create);

    bulkInfo.bulkDir = bulkDir.toString();
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
  public void undo(FateId fateId, Manager environment) throws Exception {
    // unreserve sourceDir/error directories
    Utils.unreserveHdfsDirectory(environment, bulkInfo.sourceDir, fateId);
    Utils.getReadLock(environment, bulkInfo.tableId, fateId).unlock();
  }
}

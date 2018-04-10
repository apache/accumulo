/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.master.tableOps;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.util.BulkSerialize;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prepare bulk import directory. This REPO creates a bulk directory in Accumulo, list all the files
 * in the original directory and creates a renaming file for moving the files (which happens next in
 * BulkImportMove). The renaming file has a mapping of originalPath -> newPath. The newPath will be
 * the bulk directory in Accumulo. The renaming file is called {@link Constants.BULK_RENAME_FILE}
 * and is written to the {@link Constants.BULK_PREFIX} bulk directory generated here.
 *
 * @since 2.0.0
 */
public class PrepBulkImport extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(PrepBulkImport.class);

  private final BulkInfo bulkInfo;

  public PrepBulkImport(Table.ID tableId, String sourceDir, boolean setTime) {
    BulkInfo info = new BulkInfo();
    info.tableId = tableId;
    info.sourceDir = sourceDir;
    info.setTime = setTime;
    info.retries = 0;
    this.bulkInfo = info;
  }

  @Override
  public long isReady(long tid, Master master) throws Exception {
    if (master.onlineTabletServers().size() == 0)
      return 500;
    Tables.clearCache(master.getInstance());
    if (Tables.getTableState(master.getInstance(), bulkInfo.tableId) == TableState.ONLINE) {
      return Utils.reserveHdfsDirectory(bulkInfo.sourceDir, tid);
    } else {
      throw new AcceptableThriftTableOperationException(bulkInfo.tableId.canonicalID(), null,
          TableOperation.BULK_IMPORT, TableOperationExceptionType.OFFLINE, null);
    }
  }

  @Override
  public Repo<Master> call(final long tid, final Master master) throws Exception {
    master.updateBulkImportStatus(bulkInfo.sourceDir, BulkImportState.MOVING);
    VolumeManager fs = master.getFileSystem();
    final UniqueNameAllocator namer = UniqueNameAllocator.getInstance();
    Path sourceDir = new Path(bulkInfo.sourceDir);
    FileStatus[] files = fs.listStatus(sourceDir);

    Path bulkDir = createNewBulkDir(fs, bulkInfo.tableId);
    Path mappingFile = new Path(sourceDir, Constants.BULK_LOAD_MAPPING);

    log.debug("tid {} preparing bulkDir import for {}", tid, bulkInfo.sourceDir);
    Map<String,String> oldToNewNameMap = new HashMap<>();
    for (FileStatus file : files) {
      final FileStatus fileStatus = file;
      final Path originalPath = fileStatus.getPath();
      String fileNameParts[] = originalPath.getName().split("\\.");
      String extension = "";
      boolean invalidFileName;
      if (fileNameParts.length > 1) {
        extension = fileNameParts[fileNameParts.length - 1];
        invalidFileName = !FileOperations.getValidExtensions().contains(extension);
      } else {
        invalidFileName = true;
      }
      if (invalidFileName) {
        log.warn("{} does not have a valid extension, ignoring", fileStatus.getPath());
        continue;
      }

      String newName = "I" + namer.getNextName() + "." + extension;
      Path newPath = new Path(bulkDir, newName);
      oldToNewNameMap.put(originalPath.getName(), newPath.getName());
    }

    // also have to move mapping file
    Path newMappingFile = new Path(bulkDir, mappingFile.getName());
    oldToNewNameMap.put(mappingFile.getName(), newMappingFile.getName());
    BulkSerialize.writeRenameMap(oldToNewNameMap, bulkDir.toString(), bulkInfo.tableId, fs);

    bulkInfo.bulkDir = bulkDir.toString();
    // return the next step, which will move files
    return new BulkImportMove(bulkInfo);
  }

  private Path createNewBulkDir(VolumeManager fs, Table.ID tableId) throws IOException {
    Path tempPath = fs.matchingFileSystem(new Path(bulkInfo.sourceDir),
        ServerConstants.getTablesDirs());
    if (tempPath == null)
      throw new IOException(bulkInfo.sourceDir + " is not in a volume configured for Accumulo");

    String tableDir = tempPath.toString();
    if (tableDir == null)
      throw new IOException(bulkInfo.sourceDir + " is not in a volume configured for Accumulo");
    Path directory = new Path(tableDir + "/" + tableId);
    fs.mkdirs(directory);

    UniqueNameAllocator namer = UniqueNameAllocator.getInstance();
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
    Utils.unreserveHdfsDirectory(bulkInfo.sourceDir, tid);
    Utils.getReadLock(bulkInfo.tableId, tid).unlock();
    TransactionWatcher.ZooArbitrator.cleanup(Constants.BULK_ARBITRATOR_TYPE, tid);
  }

}

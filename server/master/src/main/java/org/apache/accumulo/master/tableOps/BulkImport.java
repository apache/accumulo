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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Bulk import makes requests of tablet servers, and those requests can take a
 * long time. Our communications to the tablet server may fail, so we won't know
 * the status of the request. The master will repeat failed requests so now
 * there are multiple requests to the tablet server. The tablet server will not
 * execute the request multiple times, so long as the marker it wrote in the
 * metadata table stays there. The master needs to know when all requests have
 * finished so it can remove the markers. Did it start? Did it finish? We can see
 * that *a* request completed by seeing the flag written into the metadata
 * table, but we won't know if some other rogue thread is still waiting to start
 * a thread and repeat the operation.
 *
 * The master can ask the tablet server if it has any requests still running.
 * Except the tablet server might have some thread about to start a request, but
 * before it has made any bookkeeping about the request. To prevent problems
 * like this, an Arbitrator is used. Before starting any new request, the tablet
 * server checks the Arbitrator to see if the request is still valid.
 *
 */

public class BulkImport extends MasterRepo {
  public static final String FAILURES_TXT = "failures.txt";

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(BulkImport.class);

  private Table.ID tableId;
  private String sourceDir;
  private String errorDir;
  private boolean setTime;

  public BulkImport(Table.ID tableId, String sourceDir, String errorDir, boolean setTime) {
    this.tableId = tableId;
    this.sourceDir = sourceDir;
    this.errorDir = errorDir;
    this.setTime = setTime;
  }

  @Override
  public long isReady(long tid, Master master) throws Exception {
    if (!Utils.getReadLock(tableId, tid).tryLock())
      return 100;

    Tables.clearCache(master.getInstance());
    if (Tables.getTableState(master.getInstance(), tableId) == TableState.ONLINE) {
      long reserve1, reserve2;
      reserve1 = reserve2 = Utils.reserveHdfsDirectory(sourceDir, tid);
      if (reserve1 == 0)
        reserve2 = Utils.reserveHdfsDirectory(errorDir, tid);
      return reserve2;
    } else {
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), null, TableOperation.BULK_IMPORT, TableOperationExceptionType.OFFLINE, null);
    }
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    log.debug(" tid {} sourceDir {}", tid, sourceDir);

    Utils.getReadLock(tableId, tid).lock();

    // check that the error directory exists and is empty
    VolumeManager fs = master.getFileSystem();

    Path errorPath = new Path(errorDir);
    FileStatus errorStatus = null;
    try {
      errorStatus = fs.getFileStatus(errorPath);
    } catch (FileNotFoundException ex) {
      // ignored
    }
    if (errorStatus == null)
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), null, TableOperation.BULK_IMPORT,
          TableOperationExceptionType.BULK_BAD_ERROR_DIRECTORY, errorDir + " does not exist");
    if (!errorStatus.isDirectory())
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), null, TableOperation.BULK_IMPORT,
          TableOperationExceptionType.BULK_BAD_ERROR_DIRECTORY, errorDir + " is not a directory");
    if (fs.listStatus(errorPath).length != 0)
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), null, TableOperation.BULK_IMPORT,
          TableOperationExceptionType.BULK_BAD_ERROR_DIRECTORY, errorDir + " is not empty");

    ZooArbitrator.start(Constants.BULK_ARBITRATOR_TYPE, tid);
    master.updateBulkImportStatus(sourceDir, BulkImportState.MOVING);
    // move the files into the directory
    try {
      String bulkDir = prepareBulkImport(master, fs, sourceDir, tableId);
      log.debug(" tid {} bulkDir {}", tid, bulkDir);
      return new LoadFiles(tableId, sourceDir, bulkDir, errorDir, setTime);
    } catch (IOException ex) {
      log.error("error preparing the bulk import directory", ex);
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), null, TableOperation.BULK_IMPORT,
          TableOperationExceptionType.BULK_BAD_INPUT_DIRECTORY, sourceDir + ": " + ex);
    }
  }

  private Path createNewBulkDir(VolumeManager fs, Table.ID tableId) throws IOException {
    Path tempPath = fs.matchingFileSystem(new Path(sourceDir), ServerConstants.getTablesDirs());
    if (tempPath == null)
      throw new IOException(sourceDir + " is not in a volume configured for Accumulo");

    String tableDir = tempPath.toString();
    if (tableDir == null)
      throw new IOException(sourceDir + " is not in a volume configured for Accumulo");
    Path directory = new Path(tableDir + "/" + tableId);
    fs.mkdirs(directory);

    // only one should be able to create the lock file
    // the purpose of the lock file is to avoid a race
    // condition between the call to fs.exists() and
    // fs.mkdirs()... if only hadoop had a mkdir() function
    // that failed when the dir existed

    UniqueNameAllocator namer = UniqueNameAllocator.getInstance();

    while (true) {
      Path newBulkDir = new Path(directory, Constants.BULK_PREFIX + namer.getNextName());
      if (fs.exists(newBulkDir)) // sanity check
        throw new IOException("Dir exist when it should not " + newBulkDir);
      if (fs.mkdirs(newBulkDir))
        return newBulkDir;
      log.warn("Failed to create {} for unknown reason", newBulkDir);

      sleepUninterruptibly(3, TimeUnit.SECONDS);
    }
  }

  private String prepareBulkImport(Master master, final VolumeManager fs, String dir, Table.ID tableId) throws Exception {
    final Path bulkDir = createNewBulkDir(fs, tableId);

    MetadataTableUtil.addBulkLoadInProgressFlag(master, "/" + bulkDir.getParent().getName() + "/" + bulkDir.getName());

    Path dirPath = new Path(dir);
    FileStatus[] mapFiles = fs.listStatus(dirPath);

    final UniqueNameAllocator namer = UniqueNameAllocator.getInstance();

    int workerCount = master.getConfiguration().getCount(Property.MASTER_BULK_RENAME_THREADS);
    SimpleThreadPool workers = new SimpleThreadPool(workerCount, "bulk move");
    List<Future<Exception>> results = new ArrayList<>();

    for (FileStatus file : mapFiles) {
      final FileStatus fileStatus = file;
      results.add(workers.submit(new Callable<Exception>() {
        @Override
        public Exception call() throws Exception {
          try {
            String sa[] = fileStatus.getPath().getName().split("\\.");
            String extension = "";
            if (sa.length > 1) {
              extension = sa[sa.length - 1];

              if (!FileOperations.getValidExtensions().contains(extension)) {
                log.warn("{} does not have a valid extension, ignoring", fileStatus.getPath());
                return null;
              }
            } else {
              // assume it is a map file
              extension = Constants.MAPFILE_EXTENSION;
            }

            if (extension.equals(Constants.MAPFILE_EXTENSION)) {
              if (!fileStatus.isDirectory()) {
                log.warn("{} is not a map file, ignoring", fileStatus.getPath());
                return null;
              }

              if (fileStatus.getPath().getName().equals("_logs")) {
                log.info("{} is probably a log directory from a map/reduce task, skipping", fileStatus.getPath());
                return null;
              }
              try {
                FileStatus dataStatus = fs.getFileStatus(new Path(fileStatus.getPath(), MapFile.DATA_FILE_NAME));
                if (dataStatus.isDirectory()) {
                  log.warn("{} is not a map file, ignoring", fileStatus.getPath());
                  return null;
                }
              } catch (FileNotFoundException fnfe) {
                log.warn("{} is not a map file, ignoring", fileStatus.getPath());
                return null;
              }
            }

            String newName = "I" + namer.getNextName() + "." + extension;
            Path newPath = new Path(bulkDir, newName);
            try {
              fs.rename(fileStatus.getPath(), newPath);
              log.debug("Moved {} to {}", fileStatus.getPath(), newPath);
            } catch (IOException E1) {
              log.error("Could not move: {} {}", fileStatus.getPath().toString(), E1.getMessage());
            }

          } catch (Exception ex) {
            return ex;
          }
          return null;
        }
      }));
    }
    workers.shutdown();
    while (!workers.awaitTermination(1000L, TimeUnit.MILLISECONDS)) {}

    for (Future<Exception> ex : results) {
      if (ex.get() != null) {
        throw ex.get();
      }
    }
    return bulkDir.toString();
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    // unreserve source/error directories
    Utils.unreserveHdfsDirectory(sourceDir, tid);
    Utils.unreserveHdfsDirectory(errorDir, tid);
    Utils.getReadLock(tableId, tid).unlock();
    ZooArbitrator.cleanup(Constants.BULK_ARBITRATOR_TYPE, tid);
  }
}

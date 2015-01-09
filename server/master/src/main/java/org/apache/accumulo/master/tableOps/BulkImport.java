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

import static com.google.common.base.Charsets.UTF_8;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.ServerClient;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.ClientService.Client;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.accumulo.trace.instrument.TraceExecutorService;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

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

  private static final Logger log = Logger.getLogger(BulkImport.class);

  private String tableId;
  private String sourceDir;
  private String errorDir;
  private boolean setTime;

  public BulkImport(String tableId, String sourceDir, String errorDir, boolean setTime) {
    this.tableId = tableId;
    this.sourceDir = sourceDir;
    this.errorDir = errorDir;
    this.setTime = setTime;
  }

  @Override
  public long isReady(long tid, Master master) throws Exception {
    if (!Utils.getReadLock(tableId, tid).tryLock())
      return 100;

    Instance instance = HdfsZooInstance.getInstance();
    Tables.clearCache(instance);
    if (Tables.getTableState(instance, tableId) == TableState.ONLINE) {
      long reserve1, reserve2;
      reserve1 = reserve2 = Utils.reserveHdfsDirectory(sourceDir, tid);
      if (reserve1 == 0)
        reserve2 = Utils.reserveHdfsDirectory(errorDir, tid);
      return reserve2;
    } else {
      throw new ThriftTableOperationException(tableId, null, TableOperation.BULK_IMPORT, TableOperationExceptionType.OFFLINE, null);
    }
  }

  @Override
  // TODO Remove deprecation warning suppression when Hadoop1 support is dropped
  @SuppressWarnings("deprecation")
  public Repo<Master> call(long tid, Master master) throws Exception {
    log.debug(" tid " + tid + " sourceDir " + sourceDir);

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
      throw new ThriftTableOperationException(tableId, null, TableOperation.BULK_IMPORT, TableOperationExceptionType.BULK_BAD_ERROR_DIRECTORY, errorDir
          + " does not exist");
    if (!errorStatus.isDir())
      throw new ThriftTableOperationException(tableId, null, TableOperation.BULK_IMPORT, TableOperationExceptionType.BULK_BAD_ERROR_DIRECTORY, errorDir
          + " is not a directory");
    if (fs.listStatus(errorPath).length != 0)
      throw new ThriftTableOperationException(tableId, null, TableOperation.BULK_IMPORT, TableOperationExceptionType.BULK_BAD_ERROR_DIRECTORY, errorDir
          + " is not empty");

    ZooArbitrator.start(Constants.BULK_ARBITRATOR_TYPE, tid);

    // move the files into the directory
    try {
      String bulkDir = prepareBulkImport(fs, sourceDir, tableId);
      log.debug(" tid " + tid + " bulkDir " + bulkDir);
      return new LoadFiles(tableId, sourceDir, bulkDir, errorDir, setTime);
    } catch (IOException ex) {
      log.error("error preparing the bulk import directory", ex);
      throw new ThriftTableOperationException(tableId, null, TableOperation.BULK_IMPORT, TableOperationExceptionType.BULK_BAD_INPUT_DIRECTORY, sourceDir + ": "
          + ex);
    }
  }

  private Path createNewBulkDir(VolumeManager fs, String tableId) throws IOException {
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
      log.warn("Failed to create " + newBulkDir + " for unknown reason");

      UtilWaitThread.sleep(3000);
    }
  }

  // TODO Remove deprecation warning suppression when Hadoop1 support is dropped
  @SuppressWarnings("deprecation")
  private String prepareBulkImport(VolumeManager fs, String dir, String tableId) throws IOException {
    Path bulkDir = createNewBulkDir(fs, tableId);

    MetadataTableUtil.addBulkLoadInProgressFlag("/" + bulkDir.getParent().getName() + "/" + bulkDir.getName());

    Path dirPath = new Path(dir);
    FileStatus[] mapFiles = fs.listStatus(dirPath);

    UniqueNameAllocator namer = UniqueNameAllocator.getInstance();

    for (FileStatus fileStatus : mapFiles) {
      String sa[] = fileStatus.getPath().getName().split("\\.");
      String extension = "";
      if (sa.length > 1) {
        extension = sa[sa.length - 1];

        if (!FileOperations.getValidExtensions().contains(extension)) {
          log.warn(fileStatus.getPath() + " does not have a valid extension, ignoring");
          continue;
        }
      } else {
        // assume it is a map file
        extension = Constants.MAPFILE_EXTENSION;
      }

      if (extension.equals(Constants.MAPFILE_EXTENSION)) {
        if (!fileStatus.isDir()) {
          log.warn(fileStatus.getPath() + " is not a map file, ignoring");
          continue;
        }

        if (fileStatus.getPath().getName().equals("_logs")) {
          log.info(fileStatus.getPath() + " is probably a log directory from a map/reduce task, skipping");
          continue;
        }
        try {
          FileStatus dataStatus = fs.getFileStatus(new Path(fileStatus.getPath(), MapFile.DATA_FILE_NAME));
          if (dataStatus.isDir()) {
            log.warn(fileStatus.getPath() + " is not a map file, ignoring");
            continue;
          }
        } catch (FileNotFoundException fnfe) {
          log.warn(fileStatus.getPath() + " is not a map file, ignoring");
          continue;
        }
      }

      String newName = "I" + namer.getNextName() + "." + extension;
      Path newPath = new Path(bulkDir, newName);
      try {
        fs.rename(fileStatus.getPath(), newPath);
        log.debug("Moved " + fileStatus.getPath() + " to " + newPath);
      } catch (IOException E1) {
        log.error("Could not move: " + fileStatus.getPath().toString() + " " + E1.getMessage());
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
  }
}

class CleanUpBulkImport extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = Logger.getLogger(CleanUpBulkImport.class);

  private String tableId;
  private String source;
  private String bulk;
  private String error;

  public CleanUpBulkImport(String tableId, String source, String bulk, String error) {
    this.tableId = tableId;
    this.source = source;
    this.bulk = bulk;
    this.error = error;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    log.debug("removing the bulk processing flag file in " + bulk);
    Path bulkDir = new Path(bulk);
    MetadataTableUtil.removeBulkLoadInProgressFlag("/" + bulkDir.getParent().getName() + "/" + bulkDir.getName());
    MetadataTableUtil.addDeleteEntry(tableId, bulkDir.toString());
    log.debug("removing the metadata table markers for loaded files");
    Connector conn = master.getConnector();
    MetadataTableUtil.removeBulkLoadEntries(conn, tableId, tid);
    log.debug("releasing HDFS reservations for " + source + " and " + error);
    Utils.unreserveHdfsDirectory(source, tid);
    Utils.unreserveHdfsDirectory(error, tid);
    Utils.getReadLock(tableId, tid).unlock();
    log.debug("completing bulk import transaction " + tid);
    ZooArbitrator.cleanup(Constants.BULK_ARBITRATOR_TYPE, tid);
    return null;
  }
}

class CompleteBulkImport extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private String tableId;
  private String source;
  private String bulk;
  private String error;

  public CompleteBulkImport(String tableId, String source, String bulk, String error) {
    this.tableId = tableId;
    this.source = source;
    this.bulk = bulk;
    this.error = error;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    ZooArbitrator.stop(Constants.BULK_ARBITRATOR_TYPE, tid);
    return new CopyFailed(tableId, source, bulk, error);
  }
}

class CopyFailed extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private String tableId;
  private String source;
  private String bulk;
  private String error;

  public CopyFailed(String tableId, String source, String bulk, String error) {
    this.tableId = tableId;
    this.source = source;
    this.bulk = bulk;
    this.error = error;
  }

  @Override
  public long isReady(long tid, Master master) throws Exception {
    Set<TServerInstance> finished = new HashSet<TServerInstance>();
    Set<TServerInstance> running = master.onlineTabletServers();
    for (TServerInstance server : running) {
      try {
        TServerConnection client = master.getConnection(server);
        if (client != null && !client.isActive(tid))
          finished.add(server);
      } catch (TException ex) {
        log.info("Ignoring error trying to check on tid " + tid + " from server " + server + ": " + ex);
      }
    }
    if (finished.containsAll(running))
      return 0;
    return 500;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    // This needs to execute after the arbiter is stopped

    VolumeManager fs = master.getFileSystem();

    if (!fs.exists(new Path(error, BulkImport.FAILURES_TXT)))
      return new CleanUpBulkImport(tableId, source, bulk, error);

    HashMap<FileRef,String> failures = new HashMap<FileRef,String>();
    HashMap<FileRef,String> loadedFailures = new HashMap<FileRef,String>();

    FSDataInputStream failFile = fs.open(new Path(error, BulkImport.FAILURES_TXT));
    BufferedReader in = new BufferedReader(new InputStreamReader(failFile, UTF_8));
    try {
      String line = null;
      while ((line = in.readLine()) != null) {
        Path path = new Path(line);
        if (!fs.exists(new Path(error, path.getName())))
          failures.put(new FileRef(line, path), line);
      }
    } finally {
      failFile.close();
    }

    /*
     * I thought I could move files that have no file references in the table. However its possible a clone references a file. Therefore only move files that
     * have no loaded markers.
     */

    // determine which failed files were loaded
    Connector conn = master.getConnector();
    Scanner mscanner = new IsolatedScanner(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY));
    mscanner.setRange(new KeyExtent(new Text(tableId), null, null).toMetadataRange());
    mscanner.fetchColumnFamily(TabletsSection.BulkFileColumnFamily.NAME);

    for (Entry<Key,Value> entry : mscanner) {
      if (Long.parseLong(entry.getValue().toString()) == tid) {
        FileRef loadedFile = new FileRef(fs, entry.getKey());
        String absPath = failures.remove(loadedFile);
        if (absPath != null) {
          loadedFailures.put(loadedFile, absPath);
        }
      }
    }

    // move failed files that were not loaded
    for (String failure : failures.values()) {
      Path orig = new Path(failure);
      Path dest = new Path(error, orig.getName());
      fs.rename(orig, dest);
      log.debug("tid " + tid + " renamed " + orig + " to " + dest + ": import failed");
    }

    if (loadedFailures.size() > 0) {
      DistributedWorkQueue bifCopyQueue = new DistributedWorkQueue(Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID()
          + Constants.ZBULK_FAILED_COPYQ);

      HashSet<String> workIds = new HashSet<String>();

      for (String failure : loadedFailures.values()) {
        Path orig = new Path(failure);
        Path dest = new Path(error, orig.getName());

        if (fs.exists(dest))
          continue;

        bifCopyQueue.addWork(orig.getName(), (failure + "," + dest).getBytes(UTF_8));
        workIds.add(orig.getName());
        log.debug("tid " + tid + " added to copyq: " + orig + " to " + dest + ": failed");
      }

      bifCopyQueue.waitUntilDone(workIds);
    }

    fs.deleteRecursively(new Path(error, BulkImport.FAILURES_TXT));
    return new CleanUpBulkImport(tableId, source, bulk, error);
  }

}

class LoadFiles extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private static ExecutorService threadPool = null;
  private static final Logger log = Logger.getLogger(BulkImport.class);

  private String tableId;
  private String source;
  private String bulk;
  private String errorDir;
  private boolean setTime;

  public LoadFiles(String tableId, String source, String bulk, String errorDir, boolean setTime) {
    this.tableId = tableId;
    this.source = source;
    this.bulk = bulk;
    this.errorDir = errorDir;
    this.setTime = setTime;
  }

  @Override
  public long isReady(long tid, Master master) throws Exception {
    if (master.onlineTabletServers().size() == 0)
      return 500;
    return 0;
  }

  private static synchronized ExecutorService getThreadPool(Master master) {
    if (threadPool == null) {
      int threadPoolSize = master.getSystemConfiguration().getCount(Property.MASTER_BULK_THREADPOOL_SIZE);
      ThreadPoolExecutor pool = new SimpleThreadPool(threadPoolSize, "bulk import");
      pool.allowCoreThreadTimeOut(true);
      threadPool = new TraceExecutorService(pool);
    }
    return threadPool;
  }

  @Override
  public Repo<Master> call(final long tid, final Master master) throws Exception {
    ExecutorService executor = getThreadPool(master);
    final SiteConfiguration conf = ServerConfiguration.getSiteConfiguration();
    VolumeManager fs = master.getFileSystem();
    List<FileStatus> files = new ArrayList<FileStatus>();
    for (FileStatus entry : fs.listStatus(new Path(bulk))) {
      files.add(entry);
    }
    log.debug("tid " + tid + " importing " + files.size() + " files");

    Path writable = new Path(this.errorDir, ".iswritable");
    if (!fs.createNewFile(writable)) {
      // Maybe this is a re-try... clear the flag and try again
      fs.delete(writable);
      if (!fs.createNewFile(writable))
        throw new ThriftTableOperationException(tableId, null, TableOperation.BULK_IMPORT, TableOperationExceptionType.BULK_BAD_ERROR_DIRECTORY,
            "Unable to write to " + this.errorDir);
    }
    fs.delete(writable);

    final Set<String> filesToLoad = Collections.synchronizedSet(new HashSet<String>());
    for (FileStatus f : files)
      filesToLoad.add(f.getPath().toString());

    final int RETRIES = Math.max(1, conf.getCount(Property.MASTER_BULK_RETRIES));
    for (int attempt = 0; attempt < RETRIES && filesToLoad.size() > 0; attempt++) {
      List<Future<List<String>>> results = new ArrayList<Future<List<String>>>();

      if (master.onlineTabletServers().size() == 0)
        log.warn("There are no tablet server to process bulk import, waiting (tid = " + tid + ")");

      while (master.onlineTabletServers().size() == 0) {
        UtilWaitThread.sleep(500);
      }

      // Use the threadpool to assign files one-at-a-time to the server
      final List<String> loaded = Collections.synchronizedList(new ArrayList<String>());
      for (final String file : filesToLoad) {
        results.add(executor.submit(new Callable<List<String>>() {
          @Override
          public List<String> call() {
            List<String> failures = new ArrayList<String>();
            ClientService.Client client = null;
            String server = null;
            try {
              // get a connection to a random tablet server, do not prefer cached connections because
              // this is running on the master and there are lots of connections to tablet servers
              // serving the metadata tablets
              long timeInMillis = master.getConfiguration().getConfiguration().getTimeInMillis(Property.MASTER_BULK_TIMEOUT);
              Pair<String,Client> pair = ServerClient.getConnection(master.getInstance(), false, timeInMillis);
              client = pair.getSecond();
              server = pair.getFirst();
              List<String> attempt = Collections.singletonList(file);
              log.debug("Asking " + pair.getFirst() + " to bulk import " + file);
              List<String> fail = client.bulkImportFiles(Tracer.traceInfo(), SystemCredentials.get().toThrift(master.getInstance()), tid, tableId, attempt,
                  errorDir, setTime);
              if (fail.isEmpty()) {
                loaded.add(file);
              } else {
                failures.addAll(fail);
              }
            } catch (Exception ex) {
              log.error("rpc failed server:" + server + ", tid:" + tid + " " + ex);
            } finally {
              ServerClient.close(client);
            }
            return failures;
          }
        }));
      }
      Set<String> failures = new HashSet<String>();
      for (Future<List<String>> f : results)
        failures.addAll(f.get());
      filesToLoad.removeAll(loaded);
      if (filesToLoad.size() > 0) {
        log.debug("tid " + tid + " attempt " + (attempt + 1) + " " + sampleList(filesToLoad, 10) + " failed");
        UtilWaitThread.sleep(100);
      }
    }

    FSDataOutputStream failFile = fs.create(new Path(errorDir, BulkImport.FAILURES_TXT), true);
    BufferedWriter out = new BufferedWriter(new OutputStreamWriter(failFile, UTF_8));
    try {
      for (String f : filesToLoad) {
        out.write(f);
        out.write("\n");
      }
    } finally {
      out.close();
    }

    // return the next step, which will perform cleanup
    return new CompleteBulkImport(tableId, source, bulk, errorDir);
  }

  static String sampleList(Collection<?> potentiallyLongList, int max) {
    StringBuffer result = new StringBuffer();
    result.append("[");
    int i = 0;
    for (Object obj : potentiallyLongList) {
      result.append(obj);
      if (i >= max) {
        result.append("...");
        break;
      } else {
        result.append(", ");
      }
      i++;
    }
    if (i < max)
      result.delete(result.length() - 2, result.length());
    result.append("]");
    return result.toString();
  }

}

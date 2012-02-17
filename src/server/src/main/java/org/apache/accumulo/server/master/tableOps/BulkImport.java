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
package org.apache.accumulo.server.master.tableOps;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ServerClient;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fate.Repo;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.tabletserver.UniqueNameAllocator;
import org.apache.accumulo.server.trace.TraceFileSystem;
import org.apache.accumulo.server.util.MetadataTable;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import cloudtrace.instrument.TraceExecutorService;

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
    log.debug(this.getDescription());
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
  public Repo<Master> call(long tid, Master master) throws Exception {
    log.debug(" tid " + tid + " sourceDir " + sourceDir);
    
    Utils.getReadLock(tableId, tid).lock();
    
    // check that the error directory exists and is empty
    FileSystem fs = TraceFileSystem.wrap(org.apache.accumulo.core.file.FileUtil.getFileSystem(CachedConfiguration.getInstance(),
        ServerConfiguration.getSiteConfiguration()));
    ;
    Path errorPath = new Path(errorDir);
    FileStatus errorStatus = fs.getFileStatus(errorPath);
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
  
  private Path createNewBulkDir(FileSystem fs, String tableId) throws IOException {
    Path directory = new Path(ServerConstants.getTablesDir() + "/" + tableId);
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
        throw new IllegalStateException("Dir exist when it should not " + newBulkDir);
      if (fs.mkdirs(newBulkDir))
        return newBulkDir;
      log.warn("Failed to create " + newBulkDir + " for unknown reason");
      
      UtilWaitThread.sleep(3000);
    }
  }
  
  private String prepareBulkImport(FileSystem fs, String dir, String tableId) throws IOException {
    Path bulkDir = createNewBulkDir(fs, tableId);
    
    MetadataTable.addBulkLoadInProgressFlag("/" + bulkDir.getParent().getName() + "/" + bulkDir.getName());
    
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
  public long isReady(long tid, Master master) throws Exception {
    Set<TServerInstance> finished = new HashSet<TServerInstance>();
    Set<TServerInstance> running = master.onlineTabletServers();
    for (TServerInstance server : running) {
      try {
        if (!master.getConnection(server).isActive(tid))
          finished.add(server);
      } catch (TException ex) {
        log.info("Ignoring error trying to check on tid " + tid + " from server " + server + ": " + ex);
      }
    }
    if (finished.containsAll(running))
      return 0;
    return 1000;
  }
  
  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    log.debug("removing the bulk processing flag file in " + bulk);
    Path bulkDir = new Path(bulk);
    MetadataTable.removeBulkLoadInProgressFlag("/" + bulkDir.getParent().getName() + "/" + bulkDir.getName());
    MetadataTable.addDeleteEntry(tableId, "/" + bulkDir.getName());
    log.debug("removing the metadata table markers for loaded files");
    AuthInfo creds = SecurityConstants.getSystemCredentials();
    Connector conn = HdfsZooInstance.getInstance().getConnector(creds.user, creds.password);
    MetadataTable.removeBulkLoadEntries(conn, tableId, tid);
    log.debug("releasing HDFS reservations for " + source + " and " + error);
    Utils.unreserveHdfsDirectory(source, tid);
    Utils.unreserveHdfsDirectory(error, tid);
    Utils.getReadLock(tableId, tid).unlock();
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
    return new CleanUpBulkImport(tableId, source, bulk, error);
  }
}

class LoadFiles extends MasterRepo {
  
  private static final long serialVersionUID = 1L;
  final static int THREAD_POOL_SIZE = ServerConfiguration.getSystemConfiguration().getCount(Property.MASTER_BULK_THREADPOOL_SIZE);
  
  private static ExecutorService threadPool = null;
  static {
    if (threadPool == null) {
      ThreadFactory threadFactory = new ThreadFactory() {
        int count = 0;
        
        @Override
        public Thread newThread(Runnable r) {
          return new Daemon(r, "bulk loader " + count++);
        }
      };
      ThreadPoolExecutor pool = new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE, 1l, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
          threadFactory);
      pool.allowCoreThreadTimeOut(true);
      threadPool = new TraceExecutorService(pool);
    }
  }
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
  public Repo<Master> call(final long tid, Master master) throws Exception {
    final SiteConfiguration conf = ServerConfiguration.getSiteConfiguration();
    FileSystem fs = TraceFileSystem.wrap(org.apache.accumulo.core.file.FileUtil.getFileSystem(CachedConfiguration.getInstance(),
        ServerConfiguration.getSiteConfiguration()));
    List<FileStatus> files = new ArrayList<FileStatus>();
    for (FileStatus entry : fs.listStatus(new Path(bulk))) {
      files.add(entry);
    }
    log.debug("tid " + tid + " importing " + files.size() + " files");
    
    Path writable = new Path(this.errorDir, ".iswritable");
    if (!fs.createNewFile(writable)) {
      // Maybe this is a re-try... clear the flag and try again
      fs.delete(writable, false);
      if (!fs.createNewFile(writable))
        throw new ThriftTableOperationException(tableId, null, TableOperation.BULK_IMPORT, TableOperationExceptionType.BULK_BAD_ERROR_DIRECTORY,
            "Unable to write to " + this.errorDir);
    }
    fs.delete(writable, false);
    
    final List<String> filesToLoad = Collections.synchronizedList(new ArrayList<String>());
    for (FileStatus f : files)
      filesToLoad.add(f.getPath().toString());
    

    final int RETRIES = Math.max(1, conf.getCount(Property.MASTER_BULK_RETRIES));
    for (int attempt = 0; attempt < RETRIES && filesToLoad.size() > 0; attempt++) {
      List<Future<List<String>>> results = new ArrayList<Future<List<String>>>();
      
      // Figure out which files will be sent to which server
      Set<TServerInstance> currentServers = Collections.synchronizedSet(new HashSet<TServerInstance>(master.onlineTabletServers()));
      Map<String,List<String>> loadAssignments = new HashMap<String,List<String>>();
      for (TServerInstance server : currentServers) {
        loadAssignments.put(server.hostPort(), new ArrayList<String>());
      }
      int i = 0;
      List<Entry<String,List<String>>> entries = new ArrayList<Entry<String,List<String>>>(loadAssignments.entrySet());
      for (String file : filesToLoad) {
        entries.get(i % entries.size()).getValue().add(file);
        i++;
      }
      
      // Use the threadpool to assign files one-at-a-time to the server
      for (Entry<String,List<String>> entry : entries) {
        if (entry.getValue().isEmpty()) {
          continue;
        }
        final Entry<String,List<String>> finalEntry = entry;
        results.add(threadPool.submit(new Callable<List<String>>() {
          @Override
          public List<String> call() {
            if (log.isDebugEnabled()) {
              log.debug("Asking " + finalEntry.getKey() + " to load " + sampleList(finalEntry.getValue(), 10));
            }
            List<String> failures = new ArrayList<String>();
            ClientService.Iface client = null;
            try {
              client = ThriftUtil.getTServerClient(finalEntry.getKey(), conf);
              for (String file : finalEntry.getValue()) {
                List<String> attempt = Collections.singletonList(file);
                log.debug("Asking " + finalEntry.getKey() + " to bulk import " + file);
                List<String> fail = client.bulkImportFiles(null, SecurityConstants.getSystemCredentials(), tid, tableId, attempt, errorDir, setTime);
                if (fail.isEmpty()) {
                  filesToLoad.remove(file);
                } else {
                  failures.addAll(fail);
                }
              }
            } catch (Exception ex) {
              log.error(ex, ex);
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
      if (filesToLoad.size() > 0) {
        log.debug("tid " + tid + " attempt " + (i + 1) + " " + sampleList(filesToLoad, 10) + " failed");
        UtilWaitThread.sleep(100);
      }
    }
    // Copy/Create failed file markers
    for (String f : filesToLoad) {
      Path orig = new Path(f);
      Path dest = new Path(errorDir, orig.getName());
      try {
        FileUtil.copy(fs, orig, fs, dest, false, true, CachedConfiguration.getInstance());
        log.debug("tid " + tid + " copied " + orig + " to " + dest + ": failed");
      } catch (IOException ex) {
        try {
          fs.create(dest).close();
          log.debug("tid " + tid + " marked " + dest + " failed");
        } catch (IOException e) {
          log.error("Unable to create failure flag file " + dest, e);
        }
      }
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

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
package org.apache.accumulo.server.gc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.gc.thrift.GCMonitorService.Iface;
import org.apache.accumulo.core.gc.thrift.GCMonitorService.Processor;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.SecurityUtil;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.master.state.tables.TableManager;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.TServerUtils;
import org.apache.accumulo.server.util.TabletIterator;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.trace.instrument.CountSampler;
import org.apache.accumulo.trace.instrument.Sampler;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.accumulo.trace.instrument.thrift.TraceWrap;
import org.apache.accumulo.trace.thrift.TInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.beust.jcommander.Parameter;

public class SimpleGarbageCollector implements Iface {
  private static final Text EMPTY_TEXT = new Text();
  
  static class Opts extends ServerOpts {
    @Parameter(names = {"-v", "--verbose"}, description = "extra information will get printed to stdout also")
    boolean verbose = false;
    @Parameter(names = {"-s", "--safemode"}, description = "safe mode will not delete files")
    boolean safeMode = false;
    @Parameter(names = {"-o", "--offline"},
        description = "offline mode will run once and check data files directly; this is dangerous if accumulo is running or not shut down properly")
    boolean offline = false;
  }
  
  // how much of the JVM's available memory should it use gathering candidates
  private static final float CANDIDATE_MEMORY_PERCENTAGE = 0.75f;
  private boolean candidateMemExceeded;
  
  private static final Logger log = Logger.getLogger(SimpleGarbageCollector.class);
  
  private Credentials credentials;
  private long gcStartDelay;
  private boolean checkForBulkProcessingFiles;
  private VolumeManager fs;
  private boolean useTrash = true;
  private Opts opts = new Opts();
  private ZooLock lock;
  private Key continueKey = null;
  
  private GCStatus status = new GCStatus(new GcCycleStats(), new GcCycleStats(), new GcCycleStats(), new GcCycleStats());
  
  private int numDeleteThreads;
  
  private Instance instance;
  
  public static void main(String[] args) throws UnknownHostException, IOException {
    SecurityUtil.serverLogin();
    Instance instance = HdfsZooInstance.getInstance();
    ServerConfiguration serverConf = new ServerConfiguration(instance);
    final VolumeManager fs = VolumeManagerImpl.get();
    Accumulo.init(fs, serverConf, "gc");
    Opts opts = new Opts();
    opts.parseArgs("gc", args);
    SimpleGarbageCollector gc = new SimpleGarbageCollector(opts);
    
    gc.init(fs, instance, SystemCredentials.get(), serverConf.getConfiguration().getBoolean(Property.GC_TRASH_IGNORE));
    Accumulo.enableTracing(opts.getAddress(), "gc");
    gc.run();
  }
  
  public SimpleGarbageCollector(Opts opts) {
    this.opts = opts;
  }
  
  public void init(VolumeManager fs, Instance instance, Credentials credentials, boolean noTrash) throws IOException {
    this.fs = fs;
    this.credentials = credentials;
    this.instance = instance;
    
    gcStartDelay = instance.getConfiguration().getTimeInMillis(Property.GC_CYCLE_START);
    long gcDelay = instance.getConfiguration().getTimeInMillis(Property.GC_CYCLE_DELAY);
    numDeleteThreads = instance.getConfiguration().getCount(Property.GC_DELETE_THREADS);
    log.info("start delay: " + (opts.offline ? 0 + " sec (offline)" : gcStartDelay + " milliseconds"));
    log.info("time delay: " + gcDelay + " milliseconds");
    log.info("safemode: " + opts.safeMode);
    log.info("offline: " + opts.offline);
    log.info("verbose: " + opts.verbose);
    log.info("memory threshold: " + CANDIDATE_MEMORY_PERCENTAGE + " of " + Runtime.getRuntime().maxMemory() + " bytes");
    log.info("delete threads: " + numDeleteThreads);
    useTrash = !noTrash;
  }
  
  private void run() {
    long tStart, tStop;
    
    // Sleep for an initial period, giving the master time to start up and
    // old data files to be unused
    if (!opts.offline) {
      try {
        getZooLock(startStatsService());
      } catch (Exception ex) {
        log.error(ex, ex);
        System.exit(1);
      }
      
      try {
        log.debug("Sleeping for " + gcStartDelay + " milliseconds before beginning garbage collection cycles");
        Thread.sleep(gcStartDelay);
      } catch (InterruptedException e) {
        log.warn(e, e);
        return;
      }
    }
    
    Sampler sampler = new CountSampler(100);
    
    while (true) {
      if (sampler.next())
        Trace.on("gc");
      
      Span gcSpan = Trace.start("loop");
      tStart = System.currentTimeMillis();
      try {
        // STEP 1: gather candidates
        System.gc(); // make room
        candidateMemExceeded = false;
        checkForBulkProcessingFiles = false;
        
        Span candidatesSpan = Trace.start("getCandidates");
        status.current.started = System.currentTimeMillis();
        SortedSet<String> candidates;
        try {
          candidates = getCandidates();
          status.current.candidates = candidates.size();
        } finally {
          candidatesSpan.stop();
        }
        
        // STEP 2: confirm deletes
        // WARNING: This line is EXTREMELY IMPORTANT.
        // You MUST confirm candidates are okay to delete
        Span confirmDeletesSpan = Trace.start("confirmDeletes");
        try {
          confirmDeletes(candidates);
        status.current.inUse = status.current.candidates - candidates.size();
        } finally {
          confirmDeletesSpan.stop();
        }
        
        // STEP 3: delete files
        if (opts.safeMode) {
          if (opts.verbose)
            System.out.println("SAFEMODE: There are " + candidates.size() + " data file candidates marked for deletion.%n"
                + "          Examine the log files to identify them.%n" + "          They can be removed by executing: bin/accumulo gc --offline%n"
                + "WARNING:  Do not run the garbage collector in offline mode unless you are positive%n"
                + "          that the accumulo METADATA table is in a clean state, or that accumulo%n"
                + "          has not yet been run, in the case of an upgrade.");
          log.info("SAFEMODE: Listing all data file candidates for deletion");
          for (String s : candidates)
            log.info("SAFEMODE: " + s);
          log.info("SAFEMODE: End candidates for deletion");
        } else {
          Span deleteSpan = Trace.start("deleteFiles");
          try {
            deleteFiles(candidates);
            log.info("Number of data file candidates for deletion: " + status.current.candidates);
            log.info("Number of data file candidates still in use: " + status.current.inUse);
            log.info("Number of successfully deleted data files: " + status.current.deleted);
            log.info("Number of data files delete failures: " + status.current.errors);
          } finally {
            deleteSpan.stop();
          }
          
          // delete empty dirs of deleted tables
          // this can occur as a result of cloning
          cleanUpDeletedTableDirs(candidates);
        }
        
        status.current.finished = System.currentTimeMillis();
        status.last = status.current;
        status.current = new GcCycleStats();
        
      } catch (Exception e) {
        log.error(e, e);
      }
      tStop = System.currentTimeMillis();
      log.info(String.format("Collect cycle took %.2f seconds", ((tStop - tStart) / 1000.0)));
      
      if (opts.offline)
        break;
      
      if (candidateMemExceeded) {
        log.info("Gathering of candidates was interrupted due to memory shortage. Bypassing cycle delay to collect the remaining candidates.");
        continue;
      }
      
      // Clean up any unused write-ahead logs
      Span waLogs = Trace.start("walogs");
      try {
        GarbageCollectWriteAheadLogs walogCollector = new GarbageCollectWriteAheadLogs(instance, fs, useTrash);
        log.info("Beginning garbage collection of write-ahead logs");
        walogCollector.collect(status);
      } catch (Exception e) {
        log.error(e, e);
      } finally {
        waLogs.stop();
      }
      gcSpan.stop();
      
      // we just made a lot of changes to the !METADATA table: flush them out
      try {
        Connector connector = instance.getConnector(credentials.getPrincipal(), credentials.getToken());
        connector.tableOperations().compact(MetadataTable.NAME, null, null, true, true);
        connector.tableOperations().compact(RootTable.NAME, null, null, true, true);
      } catch (Exception e) {
        log.warn(e, e);
      }
      
      Trace.offNoFlush();
      try {
        long gcDelay = instance.getConfiguration().getTimeInMillis(Property.GC_CYCLE_DELAY);
        log.debug("Sleeping for " + gcDelay + " milliseconds");
        Thread.sleep(gcDelay);
      } catch (InterruptedException e) {
        log.warn(e, e);
        return;
      }
    }
  }
  
  private boolean moveToTrash(Path path) throws IOException {
    if (!useTrash)
      return false;
    try {
      return fs.moveToTrash(path);
    } catch (FileNotFoundException ex) {
      return false;
    }
  }
  
  /*
   * this method removes deleted table dirs that are empty
   */
  private void cleanUpDeletedTableDirs(SortedSet<String> candidates) throws Exception {
    
    HashSet<String> tableIdsWithDeletes = new HashSet<String>();
    
    // find the table ids that had dirs deleted
    for (String delete : candidates) {
      if (isDir(delete)) {
        String tableId = delete.split("/")[1];
        tableIdsWithDeletes.add(tableId);
      }
    }
    
    Tables.clearCache(instance);
    Set<String> tableIdsInZookeeper = Tables.getIdToNameMap(instance).keySet();
    
    tableIdsWithDeletes.removeAll(tableIdsInZookeeper);
    
    // tableIdsWithDeletes should now contain the set of deleted tables that had dirs deleted
    
    for (String delTableId : tableIdsWithDeletes) {
      // if dir exist and is empty, then empty list is returned...
      // hadoop 1.0 will return null if the file doesn't exist
      // hadoop 2.0 will throw an exception if the file does not exist
      for (String dir : ServerConstants.getTablesDirs()) {
        FileStatus[] tabletDirs = null;
        try {
          tabletDirs = fs.listStatus(new Path(dir + "/" + delTableId));
        } catch (FileNotFoundException ex) {
          // ignored
        }
        if (tabletDirs == null)
          continue;
        
        if (tabletDirs.length == 0) {
          Path p = new Path(dir + "/" + delTableId);
          if (!moveToTrash(p))
            fs.delete(p);
        }
      }
    }
  }
  
  private void getZooLock(InetSocketAddress addr) throws KeeperException, InterruptedException {
    String address = addr.getHostName() + ":" + addr.getPort();
    String path = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZGC_LOCK;
    
    LockWatcher lockWatcher = new LockWatcher() {
      @Override
      public void lostLock(LockLossReason reason) {
        Halt.halt("GC lock in zookeeper lost (reason = " + reason + "), exiting!");
      }
      
      @Override
      public void unableToMonitorLockNode(final Throwable e) {
        Halt.halt(-1, new Runnable() {
          
          @Override
          public void run() {
            log.fatal("No longer able to monitor lock node ", e);
          }
        });
        
      }
    };
    
    while (true) {
      lock = new ZooLock(path);
      if (lock.tryLock(lockWatcher, new ServerServices(address, Service.GC_CLIENT).toString().getBytes())) {
        break;
      }
      UtilWaitThread.sleep(1000);
    }
  }
  
  private InetSocketAddress startStatsService() throws UnknownHostException {
    Processor<Iface> processor = new Processor<Iface>(TraceWrap.service(this));
    int port = instance.getConfiguration().getPort(Property.GC_PORT);
    long maxMessageSize = instance.getConfiguration().getMemoryInBytes(Property.GENERAL_MAX_MESSAGE_SIZE);
    InetSocketAddress result = new InetSocketAddress(opts.getAddress(), port);
    try {
      TServerUtils.startTServer(result, processor, this.getClass().getSimpleName(), "GC Monitor Service", 2, 1000, maxMessageSize);
    } catch (Exception ex) {
      log.fatal(ex, ex);
      throw new RuntimeException(ex);
    }
    return result;
  }
  
  /**
   * This method gets a set of candidates for deletion by scanning the METADATA table deleted flag keyspace
   */
  SortedSet<String> getCandidates() throws Exception {
    TreeSet<String> candidates = new TreeSet<String>();
    
    if (opts.offline) {
      checkForBulkProcessingFiles = true;
      try {
        for (String validExtension : FileOperations.getValidExtensions()) {
          for (String dir : ServerConstants.getTablesDirs()) {
            for (FileStatus stat : fs.globStatus(new Path(dir + "/*/*/*." + validExtension))) {
              String cand = stat.getPath().toUri().getPath();
              if (cand.contains(ServerConstants.getRootTabletDir()))
                continue;
              candidates.add(cand.substring(dir.length()));
              log.debug("Offline candidate: " + cand);
            }
          }
        }
      } catch (IOException e) {
        log.error("Unable to check the filesystem for offline candidates. Removing all candidates for deletion to be safe.", e);
        candidates.clear();
      }
      return candidates;
    }
    
    checkForBulkProcessingFiles = false;
    candidates.addAll(getBatch(RootTable.NAME));
    if (candidateMemExceeded)
      return candidates;
    
    candidates.addAll(getBatch(MetadataTable.NAME));
    return candidates;
  }
  
  /**
   * Gets a batch of delete markers from the specified table
   * 
   * @param tableName
   *          the name of the system table to scan (either {@link RootTable.NAME} or {@link MetadataTable.NAME})
   */
  private Collection<String> getBatch(String tableName) throws Exception {
    // want to ensure GC makes progress... if the 1st N deletes are stable and we keep processing them,
    // then will never inspect deletes after N
    Range range = MetadataSchema.DeletesSection.getRange();
    if (continueKey != null) {
      if (!range.contains(continueKey)) {
        // continue key is for some other range
        return Collections.emptyList();
      }
      range = new Range(continueKey, true, range.getEndKey(), range.isEndKeyInclusive());
      continueKey = null;
    }
    
    Scanner scanner = instance.getConnector(credentials.getPrincipal(), credentials.getToken()).createScanner(tableName, Authorizations.EMPTY);
    scanner.setRange(range);
    List<String> result = new ArrayList<String>();
    // find candidates for deletion; chop off the prefix
    for (Entry<Key,Value> entry : scanner) {
      String cand = entry.getKey().getRow().toString().substring(MetadataSchema.DeletesSection.getRowPrefix().length());
      result.add(cand);
      checkForBulkProcessingFiles |= cand.toLowerCase(Locale.ENGLISH).contains(Constants.BULK_PREFIX);
      if (almostOutOfMemory()) {
        candidateMemExceeded = true;
        log.info("List of delete candidates has exceeded the memory threshold. Attempting to delete what has been gathered so far.");
        continueKey = entry.getKey();
        break;
      }
    }
    
    return result;
  }
  
  static public boolean almostOutOfMemory() {
    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory() > CANDIDATE_MEMORY_PERCENTAGE * runtime.maxMemory();
  }
  
  /**
   * This method removes candidates from the candidate list under two conditions: 1. They are in the same folder as a bulk processing file, if that option is
   * selected 2. They are still in use in the file column family in the METADATA table
   */
  public void confirmDeletes(SortedSet<String> candidates) throws AccumuloException {
    confirmDeletes(RootTable.NAME, candidates);
    confirmDeletes(MetadataTable.NAME, candidates);
  }
  
  private void confirmDeletes(String tableName, SortedSet<String> candidates) throws AccumuloException {
    Scanner scanner;
    if (opts.offline) {
      // TODO
      throw new RuntimeException("Offline scanner no longer supported");
      // try {
      // scanner = new OfflineMetadataScanner(instance.getConfiguration(), fs);
      // } catch (IOException e) {
      // throw new IllegalStateException("Unable to create offline metadata scanner", e);
      // }
    } else {
      try {
        scanner = new IsolatedScanner(instance.getConnector(credentials.getPrincipal(), credentials.getToken()).createScanner(tableName, Authorizations.EMPTY));
      } catch (AccumuloSecurityException ex) {
        throw new AccumuloException(ex);
      } catch (TableNotFoundException ex) {
        throw new AccumuloException(ex);
      }
    }
    
    // skip candidates that are in a bulk processing folder
    if (checkForBulkProcessingFiles) {
      
      log.debug("Checking for bulk processing flags");
      
      scanner.setRange(MetadataSchema.BlipSection.getRange());
      
      // WARNING: This block is IMPORTANT
      // You MUST REMOVE candidates that are in the same folder as a bulk
      // processing flag!
      
      for (Entry<Key,Value> entry : scanner) {
        String blipPath = entry.getKey().getRow().toString().substring(MetadataSchema.BlipSection.getRowPrefix().length());
        Iterator<String> tailIter = candidates.tailSet(blipPath).iterator();
        int count = 0;
        while (tailIter.hasNext()) {
          if (tailIter.next().startsWith(blipPath)) {
            count++;
            tailIter.remove();
          } else {
            break;
          }
        }
        
        if (count > 0)
          log.debug("Folder has bulk processing flag: " + blipPath);
        
      }
    }
    
    // skip candidates that are still in use in the file column family in
    // the metadata table
    scanner.clearColumns();
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    scanner.fetchColumnFamily(ScanFileColumnFamily.NAME);
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
    TabletIterator tabletIterator = new TabletIterator(scanner, MetadataSchema.TabletsSection.getRange(), false, true);
    
    while (tabletIterator.hasNext()) {
      Map<Key,Value> tabletKeyValues = tabletIterator.next();
      
      for (Entry<Key,Value> entry : tabletKeyValues.entrySet()) {
        if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME) || entry.getKey().getColumnFamily().equals(ScanFileColumnFamily.NAME)) {
          
          String cf = entry.getKey().getColumnQualifier().toString();
          String delete = cf;
          if (!cf.contains(":")) {
            if (cf.startsWith("../")) {
              delete = cf.substring(2);
            } else {
              String table = new String(KeyExtent.tableOfMetadataRow(entry.getKey().getRow()));
              if (cf.startsWith("/"))
                delete = "/" + table + cf;
              else
                delete = "/" + table + "/" + cf;
            }
          }
          // WARNING: This line is EXTREMELY IMPORTANT.
          // You MUST REMOVE candidates that are still in use
          if (candidates.remove(delete))
            log.debug("Candidate was still in use in the " + tableName + " table: " + delete);
          
          String path = delete.substring(0, delete.lastIndexOf('/'));
          if (candidates.remove(path))
            log.debug("Candidate was still in use in the " + tableName + " table: " + path);
        } else if (TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.hasColumns(entry.getKey())) {
          String table = new String(KeyExtent.tableOfMetadataRow(entry.getKey().getRow()));
          String delete = "/" + table + entry.getValue().toString();
          if (candidates.remove(delete))
            log.debug("Candidate was still in use in the " + tableName + " table: " + delete);
        } else
          throw new AccumuloException("Scanner over metadata table returned unexpected column : " + entry.getKey());
      }
    }
  }
  
  final static String METADATA_TABLE_DIR = "/" + MetadataTable.ID;
  
  private static void putMarkerDeleteMutation(final String delete, final BatchWriter metadataWriter, final BatchWriter rootWriter)
      throws MutationsRejectedException {
    BatchWriter writer = delete.contains(METADATA_TABLE_DIR) ? rootWriter : metadataWriter;
    Mutation m = new Mutation(MetadataSchema.DeletesSection.getRowPrefix() + delete);
    m.putDelete(EMPTY_TEXT, EMPTY_TEXT);
    writer.addMutation(m);
  }
  
  /**
   * This method attempts to do its best to remove files from the filesystem that have been confirmed for deletion.
   */
  private void deleteFiles(SortedSet<String> confirmedDeletes) {
    // create a batchwriter to remove the delete flags for successful
    // deletes; Need separate writer for the root tablet.
    BatchWriter writer = null;
    BatchWriter rootWriter = null;
    if (!opts.offline) {
      Connector c;
      try {
        c = instance.getConnector(SystemCredentials.get().getPrincipal(), SystemCredentials.get().getToken());
        writer = c.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
        rootWriter = c.createBatchWriter(RootTable.NAME, new BatchWriterConfig());
      } catch (AccumuloException e) {
        log.error("Unable to connect to Accumulo to write deletes", e);
      } catch (AccumuloSecurityException e) {
        log.error("Unable to connect to Accumulo to write deletes", e);
      } catch (TableNotFoundException e) {
        log.error("Unable to create writer to remove file from the " + e.getTableName() + " table", e);
      }
    }
    // when deleting a dir and all files in that dir, only need to delete the dir
    // the dir will sort right before the files... so remove the files in this case
    // to minimize namenode ops
    Iterator<String> cdIter = confirmedDeletes.iterator();
    String lastDir = null;
    while (cdIter.hasNext()) {
      String delete = cdIter.next();
      if (isDir(delete)) {
        lastDir = delete;
      } else if (lastDir != null) {
        if (delete.startsWith(lastDir)) {
          log.debug("Ignoring " + delete + " because " + lastDir + " exist");
          try {
            putMarkerDeleteMutation(delete, writer, rootWriter);
          } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
          }
          cdIter.remove();
        } else {
          lastDir = null;
        }
        
      }
    }
    
    final BatchWriter finalWriter = writer;
    final BatchWriter finalRootWriter = rootWriter;
    
    ExecutorService deleteThreadPool = Executors.newFixedThreadPool(numDeleteThreads, new NamingThreadFactory("deleting"));
    
    for (final String delete : confirmedDeletes) {
      
      Runnable deleteTask = new Runnable() {
        @Override
        public void run() {
          boolean removeFlag;
          
          try {
            Path fullPath;
            
            if (delete.contains(":"))
              fullPath = new Path(delete);
            else
              fullPath = fs.getFullPath(ServerConstants.getTablesDirs(), delete);
            log.debug("Deleting " + fullPath);
            
            if (moveToTrash(fullPath) || fs.deleteRecursively(fullPath)) {
              // delete succeeded, still want to delete
              removeFlag = true;
              synchronized (SimpleGarbageCollector.this) {
                ++status.current.deleted;
              }
            } else if (fs.exists(fullPath)) {
              // leave the entry in the METADATA table; we'll try again
              // later
              removeFlag = false;
              synchronized (SimpleGarbageCollector.this) {
                ++status.current.errors;
              }
              log.warn("File exists, but was not deleted for an unknown reason: " + fullPath);
            } else {
              // this failure, we still want to remove the METADATA table
              // entry
              removeFlag = true;
              synchronized (SimpleGarbageCollector.this) {
                ++status.current.errors;
              }
              String parts[] = delete.split("/");
              if (parts.length > 2) {
                String tableId = parts[parts.length - 3];
                String tabletDir = parts[parts.length - 2];
                TableManager.getInstance().updateTableStateCache(tableId);
                TableState tableState = TableManager.getInstance().getTableState(tableId);
                if (tableState != null && tableState != TableState.DELETING) {
                  // clone directories don't always exist
                  if (!tabletDir.startsWith("c-"))
                    log.warn("File doesn't exist: " + fullPath);
                }
              } else {
                log.warn("Very strange path name: " + delete);
              }
            }
            
            // proceed to clearing out the flags for successful deletes and
            // non-existent files
            if (removeFlag && finalWriter != null) {
              putMarkerDeleteMutation(delete, finalWriter, finalRootWriter);
            }
          } catch (Exception e) {
            log.error(e, e);
          }
          
        }
        
      };
      
      deleteThreadPool.execute(deleteTask);
    }
    
    deleteThreadPool.shutdown();
    
    try {
      while (!deleteThreadPool.awaitTermination(1000, TimeUnit.MILLISECONDS)) {}
    } catch (InterruptedException e1) {
      log.error(e1, e1);
    }
    
    if (writer != null) {
      try {
        writer.close();
      } catch (MutationsRejectedException e) {
        log.error("Problem removing entries from the metadata table: ", e);
      }
    }
    if (rootWriter != null) {
      try {
        rootWriter.close();
      } catch (MutationsRejectedException e) {
        log.error("Problem removing entries from the metadata table: ", e);
      }
    }
  }
  
  private boolean isDir(String delete) {
    int slashCount = 0;
    for (int i = 0; i < delete.length(); i++)
      if (delete.charAt(i) == '/')
        slashCount++;
    return slashCount == 2;
  }
  
  @Override
  public GCStatus getStatus(TInfo info, TCredentials credentials) {
    return status;
  }
}

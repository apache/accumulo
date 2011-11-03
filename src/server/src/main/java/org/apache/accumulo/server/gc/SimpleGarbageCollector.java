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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.gc.thrift.GCMonitorService;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.gc.thrift.GCMonitorService.Iface;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.zookeeper.ZooLock;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.core.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.master.state.tables.TableManager;
import org.apache.accumulo.server.master.state.tables.TableState;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.OfflineMetadataScanner;
import org.apache.accumulo.server.util.TServerUtils;
import org.apache.accumulo.server.util.TabletIterator;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TServerTransport;
import org.apache.zookeeper.KeeperException;

import cloudtrace.instrument.CountSampler;
import cloudtrace.instrument.Sampler;
import cloudtrace.instrument.Span;
import cloudtrace.instrument.Trace;
import cloudtrace.instrument.thrift.TraceWrap;
import cloudtrace.thrift.TInfo;

public class SimpleGarbageCollector implements Iface {
  private static final Text EMPTY_TEXT = new Text();
  
  // how much of the JVM's available memory should it use gathering candidates
  private static final float CANDIDATE_MEMORY_PERCENTAGE = 0.75f;
  private boolean candidateMemExceeded;
  
  private static final Logger log = Logger.getLogger(SimpleGarbageCollector.class);
  
  private Instance instance;
  private AuthInfo credentials;
  private long gcStartDelay, gcDelay;
  private boolean checkForBulkProcessingFiles;
  private FileSystem fs;
  private Option optSafeMode, optOffline, optVerboseMode, optAddress;
  private boolean safemode, offline, verbose;
  private String address;
  private CommandLine commandLine;
  private ZooLock lock;
  private Key continueKey = null;
  
  private GCStatus status = new GCStatus(new GcCycleStats(), new GcCycleStats(), new GcCycleStats(), new GcCycleStats());
  
  private int numDeleteThreads;
  
  public static void main(String[] args) throws UnknownHostException, IOException {
    Accumulo.init("gc");
    SimpleGarbageCollector gc = new SimpleGarbageCollector(args);
    gc.run();
  }
  
  public SimpleGarbageCollector(String[] args) throws UnknownHostException {
    Options opts = new Options();
    optVerboseMode = new Option("v", "verbose", false, "extra information will get printed to stdout also");
    optSafeMode = new Option("s", "safemode", false, "safe mode will not delete files");
    optOffline = new Option("o", "offline", false,
        "offline mode will run once and check data files directly; this is dangerous if accumulo is running or not shut down properly");
    optAddress = new Option("a", "address", true, "specify our local address");
    opts.addOption(optVerboseMode);
    opts.addOption(optSafeMode);
    opts.addOption(optOffline);
    opts.addOption(optAddress);
    
    try {
      fs = FileSystem.get(CachedConfiguration.getInstance());
      commandLine = new BasicParser().parse(opts, args);
      if (commandLine.getArgs().length != 0)
        throw new ParseException("Extraneous arguments");
      
      safemode = commandLine.hasOption(optSafeMode.getOpt());
      offline = commandLine.hasOption(optOffline.getOpt());
      verbose = commandLine.hasOption(optVerboseMode.getOpt());
      address = commandLine.getOptionValue(optAddress.getOpt());
    } catch (ParseException e) {
      String str = "Can't parse the command line options";
      log.fatal(str, e);
      throw new IllegalArgumentException(str, e);
    } catch (IOException e) {
      String str = "Can't get default file system";
      log.fatal(str, e);
      throw new IllegalStateException(str, e);
    }
    
    instance = HdfsZooInstance.getInstance();
    credentials = SecurityConstants.systemCredentials;
    
    gcStartDelay = AccumuloConfiguration.getSystemConfiguration().getTimeInMillis(Property.GC_CYCLE_START);
    gcDelay = AccumuloConfiguration.getSystemConfiguration().getTimeInMillis(Property.GC_CYCLE_DELAY);
    numDeleteThreads = AccumuloConfiguration.getSystemConfiguration().getCount(Property.GC_DELETE_THREADS);
    log.info("start delay: " + (offline ? 0 + " sec (offline)" : gcStartDelay + " milliseconds"));
    log.info("time delay: " + gcDelay + " milliseconds");
    log.info("safemode: " + safemode);
    log.info("offline: " + offline);
    log.info("verbose: " + verbose);
    log.info("memory threshold: " + CANDIDATE_MEMORY_PERCENTAGE + " of " + Runtime.getRuntime().maxMemory() + " bytes");
    log.info("delete threads: " + numDeleteThreads);
    Accumulo.enableTracing(address, "gc");
  }
  
  private void run() {
    long tStart, tStop;
    
    // Sleep for an initial period, giving the master time to start up and
    // old data files to be unused
    if (!offline) {
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
        SortedSet<String> candidates = getCandidates();
        status.current.candidates = candidates.size();
        candidatesSpan.stop();
        
        // STEP 2: confirm deletes
        // WARNING: This line is EXTREMELY IMPORTANT.
        // You MUST confirm candidates are okay to delete
        Span confirmDeletesSpan = Trace.start("confirmDeletes");
        confirmDeletes(candidates);
        status.current.inUse = status.current.candidates - candidates.size();
        confirmDeletesSpan.stop();
        
        // STEP 3: delete files
        if (safemode) {
          if (verbose)
            System.out.println("SAFEMODE: There are " + candidates.size() + " data file candidates marked for deletion.\n"
                + "          Examine the log files to identify them.\n" + "          They can be removed by executing: bin/accumulo gc --offline\n"
                + "WARNING:  Do not run the garbage collector in offline mode unless you are positive\n"
                + "          that the accumulo METADATA table is in a clean state, or that accumulo\n"
                + "          has not yet been run, in the case of an upgrade.");
          log.info("SAFEMODE: Listing all data file candidates for deletion");
          for (String s : candidates)
            log.info("SAFEMODE: " + s);
          log.info("SAFEMODE: End candidates for deletion");
        } else {
          Span deleteSpan = Trace.start("deleteFiles");
          deleteFiles(candidates);
          log.info("Number of data file candidates for deletion: " + status.current.candidates);
          log.info("Number of data file candidates still in use: " + status.current.inUse);
          log.info("Number of successfully deleted data files: " + status.current.deleted);
          log.info("Number of data files delete failures: " + status.current.errors);
          deleteSpan.stop();
          
          // check bulk dirs we just to deleted files from to see if empty
          deleteEmptyBulkDirs(candidates);
        }
        
        status.current.finished = System.currentTimeMillis();
        status.last = status.current;
        status.current = new GcCycleStats();
        
      } catch (Exception e) {
        log.error(e, e);
      }
      tStop = System.currentTimeMillis();
      log.info(String.format("Collect cycle took %.2f seconds", ((tStop - tStart) / 1000.0)));
      
      if (offline)
        break;
      
      if (candidateMemExceeded) {
        log.info("Gathering of candidates was interrupted due to memory shortage. Bypassing cycle delay to collect the remaining candidates.");
        continue;
      }
      
      // Clean up any unused write-ahead logs
      Span waLogs = Trace.start("walogs");
      try {
        log.info("Beginning garbage collection of write-ahead logs");
        GarbageCollectWriteAheadLogs.collect(fs, status);
      } catch (Exception e) {
        log.error(e, e);
      }
      waLogs.stop();
      gcSpan.stop();
      
      Trace.off();
      try {
        log.debug("Sleeping for " + gcDelay + " milliseconds");
        Thread.sleep(gcDelay);
      } catch (InterruptedException e) {
        log.warn(e, e);
        return;
      }
    }
  }
  
  private void getZooLock(InetSocketAddress addr) throws KeeperException, InterruptedException {
    String address = addr.getHostName() + ":" + addr.getPort();
    String path = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZGC_LOCK;
    
    LockWatcher lockWatcher = new LockWatcher() {
      public void lostLock(LockLossReason reason) {
        Halt.halt("GC lock in zookeeper lost (reason = " + reason + "), exiting!");
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
    GCMonitorService.Processor processor = new GCMonitorService.Processor(TraceWrap.service(this));
    TServerTransport serverTransport = null;
    int port = AccumuloConfiguration.getSystemConfiguration().getPort(Property.GC_PORT);
    try {
      serverTransport = TServerUtils.openPort(port);
    } catch (Exception ex) {
      log.fatal(ex, ex);
      throw new RuntimeException(ex);
    }
    TServerUtils.startTServer(processor, serverTransport, this.getClass().getSimpleName(), "GC Monitor Service", -1);
    return new InetSocketAddress(Accumulo.getLocalAddress(new String[] {"--address", address}), port);
  }
  
  /**
   * This method gets a set of candidates for deletion by scanning the METADATA table deleted flag keyspace
   */
  private SortedSet<String> getCandidates() {
    TreeSet<String> candidates = new TreeSet<String>();
    
    if (offline) {
      checkForBulkProcessingFiles = true;
      try {
        for (String validExtension : FileOperations.getValidExtensions()) {
          for (FileStatus stat : fs.globStatus(new Path(Constants.getTablesDir() + "/*/*/*." + validExtension))) {
            String cand = stat.getPath().toUri().getPath();
            if (!cand.contains(Constants.getRootTabletDir())) {
              candidates.add(cand.substring(Constants.getTablesDir().length()));
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
    
    Scanner scanner = new ScannerImpl(instance, credentials, Constants.METADATA_TABLE_ID, Constants.NO_AUTHS);
    
    // scan the reserved keyspace for deletes
    scanner.setRange(Constants.METADATA_DELETES_KEYSPACE);
    
    if (continueKey != null) {
      // want to ensure GC makes progress... if the 1st N deletes are stable and we keep processing them, then will never inspect deletes after N
      scanner.setRange(new Range(continueKey, true, Constants.METADATA_DELETES_KEYSPACE.getEndKey(), Constants.METADATA_DELETES_KEYSPACE.isEndKeyInclusive()));
      continueKey = null;
    } else {
      // scan the reserved keyspace for deletes
      scanner.setRange(Constants.METADATA_DELETES_KEYSPACE);
    }
    
    // find candidates for deletion; chop off the prefix
    checkForBulkProcessingFiles = false;
    for (Entry<Key,Value> entry : scanner) {
      String cand = entry.getKey().getRow().toString().substring(Constants.METADATA_DELETE_FLAG_PREFIX.length());
      candidates.add(cand);
      checkForBulkProcessingFiles |= cand.toLowerCase(Locale.ENGLISH).contains("bulk");
      if (almostOutOfMemory()) {
        candidateMemExceeded = true;
        log.info("List of delete candidates has exceeded the memory threshold. Attempting to delete what has been gathered so far.");
        continueKey = entry.getKey();
        break;
      }
    }
    
    return candidates;
  }
  
  static public boolean almostOutOfMemory() {
    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory() > CANDIDATE_MEMORY_PERCENTAGE * runtime.maxMemory();
  }
  
  /**
   * This method removes candidates from the candidate list under two conditions: 1. They are in the same folder as a bulk processing file, if that option is
   * selected 2. They are still in use in the file column family in the METADATA table
   */
  private void confirmDeletes(SortedSet<String> candidates) throws AccumuloException {
    // skip candidates that are in a bulk processing folder
    if (checkForBulkProcessingFiles) {
      log.debug("Checking for bulk processing files");
      HashSet<String> bulks = new HashSet<String>();
      for (String candidate : candidates) {
        if (candidate.contains("/bulk_"))
          bulks.add(candidate.substring(0, candidate.lastIndexOf('/')));
      }
      log.debug("... looking at " + bulks.size() + " bulk directories");
      TreeSet<String> processing = new TreeSet<String>();
      try {
        for (String bulk : bulks) {
          Path glob = new Path(Constants.getTablesDir() + bulk + "/processing_proc_*");
          log.debug("Looking for processing flags in " + glob);
          FileStatus[] flags = fs.globStatus(glob);
          if (flags != null && flags.length > 0) {
            String parent = flags[0].getPath().getParent().toUri().getPath();
            processing.add(parent);
            log.debug("Folder contains bulk processing file: " + parent);
          }
        }
      } catch (IOException e) {
        log.error("Unable to check the filesystem for bulk processing files. Removing all candidates for deletion to be safe.", e);
        candidates.clear();
        return;
      }
      log.debug("Found " + processing.size() + " processing files");
      
      // WARNING: This block is IMPORTANT
      // You MUST REMOVE candidates that are in the same folder as a bulk
      // processing file!
      Iterator<String> iter = candidates.iterator();
      while (iter.hasNext()) {
        String next = Constants.getTablesDir() + iter.next();
        if (processing.contains(new Path(next).getParent().toUri().getPath())) {
          iter.remove();
          log.debug("Candidate is in a bulk folder with a processing file: " + next);
        }
      }
    }
    
    // skip candidates that are still in use in the file column family in
    // the metadata table
    Scanner scanner;
    if (offline) {
      try {
        scanner = new OfflineMetadataScanner();
      } catch (IOException e) {
        throw new IllegalStateException("Unable to create offline metadata scanner", e);
      }
    } else {
      scanner = new IsolatedScanner(new ScannerImpl(instance, credentials, Constants.METADATA_TABLE_ID, Constants.NO_AUTHS));
    }
    
    scanner.setRange(Constants.METADATA_KEYSPACE);
    scanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
    scanner.fetchColumnFamily(Constants.METADATA_SCANFILE_COLUMN_FAMILY);
    
    TabletIterator tabletIterator = new TabletIterator(scanner, false, false);
    
    while (tabletIterator.hasNext()) {
      Map<Key,Value> tabletKeyValues = tabletIterator.next();
      
      for (Entry<Key,Value> entry : tabletKeyValues.entrySet()) {
        if (entry.getKey().getColumnFamily().equals(Constants.METADATA_DATAFILE_COLUMN_FAMILY)
            || entry.getKey().getColumnFamily().equals(Constants.METADATA_SCANFILE_COLUMN_FAMILY)) {
          String table = new String(KeyExtent.tableOfMetadataRow(entry.getKey().getRow()));
          String delete = "/" + table + entry.getKey().getColumnQualifier().toString();
          
          // WARNING: This line is EXTREMELY IMPORTANT.
          // You MUST REMOVE candidates that are still in use
          if (candidates.remove(delete))
            log.debug("Candidate was still in use in the METADATA table: " + delete);
        } else
          throw new AccumuloException("Scanner over metadata table returned unexpected column : " + entry.getKey());
      }
    }
  }
  
  /**
   * This method attempts to do its best to remove files from the filesystem that have been confirmed for deletion.
   */
  private void deleteFiles(SortedSet<String> confirmedDeletes) {
    // create a batchwriter to remove the delete flags for successful
    // deletes
    BatchWriter writer = null;
    if (!offline) {
      Connector c;
      try {
        c = instance.getConnector(SecurityConstants.SYSTEM_USERNAME, SecurityConstants.systemCredentials.password);
        writer = c.createBatchWriter(Constants.METADATA_TABLE_NAME, 10000000, 60000l, 3);
      } catch (Exception e) {
        log.error("Unable to create writer to remove file from the !METADATA table", e);
      }
    }
    
    final BatchWriter finalWriter = writer;
    
    ExecutorService deleteThreadPool = Executors.newFixedThreadPool(numDeleteThreads);
    
    for (final String file : confirmedDeletes) {
      
      Runnable deleteTask = new Runnable() {
        @Override
        public void run() {
          boolean removeFlag;
          
          log.debug("Deleting " + Constants.getTablesDir() + file);
          try {
            
            Path p = new Path(Constants.getTablesDir() + file);
            
            if (fs.delete(p, true)) {
              // delete succeeded, still want to delete
              removeFlag = true;
              synchronized (SimpleGarbageCollector.this) {
                ++status.current.deleted;
              }
            } else if (fs.exists(p)) {
              // leave the entry in the METADATA table; we'll try again
              // later
              removeFlag = false;
              synchronized (SimpleGarbageCollector.this) {
                ++status.current.errors;
              }
              log.warn("File exists, but was not deleted for an unknown reason: " + p);
            } else {
              // this failure, we still want to remove the METADATA table
              // entry
              removeFlag = true;
              synchronized (SimpleGarbageCollector.this) {
                ++status.current.errors;
              }
              String parts[] = file.split("/");
              if (parts.length > 1) {
                String tableId = parts[1];
                TableManager.getInstance().updateTableStateCache(tableId);
                TableState tableState = TableManager.getInstance().getTableState(tableId);
                if (tableState != null && tableState != TableState.DELETING)
                  log.warn("File doesn't exist: " + p);
              } else {
                log.warn("Very strange path name: " + file);
              }
            }
            
            // proceed to clearing out the flags for successful deletes and
            // non-existent files
            if (removeFlag && finalWriter != null) {
              Mutation m = new Mutation(new Text(Constants.METADATA_DELETE_FLAG_PREFIX + file));
              m.putDelete(EMPTY_TEXT, EMPTY_TEXT);
              finalWriter.addMutation(m);
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
  }
  
  private void deleteEmptyBulkDirs(SortedSet<String> candidates) {
    HashSet<String> bulkDirs = new HashSet<String>();
    
    // get unique set of bulk dirs inorder to avoid unneeded calls to namenode
    for (String candidate : candidates) {
      Path parent = new Path(candidate).getParent();
      if (parent.getName().startsWith("bulk_")) {
        bulkDirs.add(parent.toString());
      }
    }
    
    for (String bulkDir : bulkDirs) {
      try {
        Path path = new Path(Constants.getTablesDir() + bulkDir);
        FileStatus[] entries = fs.listStatus(path);
        if (entries != null && entries.length == 0) {
          log.debug("Deleting empty bulk dir " + bulkDir);
          if (!fs.delete(path, false)) {
            log.warn("Empty bulk dir " + bulkDir + " was not deleted");
          }
        }
        
      } catch (IOException e) {
        log.warn("Failed to list files in bulk dir " + bulkDir, e);
      }
      
    }
  }
  
  @Override
  public GCStatus getStatus(TInfo info, AuthInfo credentials) {
    return status;
  }
}

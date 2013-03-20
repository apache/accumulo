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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.util.AddressUtil;
import org.apache.accumulo.server.util.MetadataTable;
import org.apache.accumulo.server.util.MetadataTable.LogEntry;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;


public class GarbageCollectWriteAheadLogs {
  private static final Logger log = Logger.getLogger(GarbageCollectWriteAheadLogs.class);
  
  private final Instance instance;
  private final FileSystem fs;

  private Trash trash;
  
  GarbageCollectWriteAheadLogs(Instance instance, FileSystem fs, boolean noTrash) throws IOException {
    this.instance = instance;
    this.fs = fs;
    if (!noTrash)
      this.trash = new Trash(fs, fs.getConf());
  }

  public void collect(GCStatus status) {
    
    Span span = Trace.start("scanServers");
    try {
      
      Set<String> sortedWALogs = getSortedWALogs();

      status.currentLog.started = System.currentTimeMillis();
      
      Map<String,String> fileToServerMap = new HashMap<String,String>();
      int count = scanServers(fileToServerMap);
      long fileScanStop = System.currentTimeMillis();
      log.info(String.format("Fetched %d files from %d servers in %.2f seconds", fileToServerMap.size(), count,
          (fileScanStop - status.currentLog.started) / 1000.));
      status.currentLog.candidates = fileToServerMap.size();
      span.stop();
      
      span = Trace.start("removeMetadataEntries");
      try {
        count = removeMetadataEntries(fileToServerMap, sortedWALogs, status);
      } catch (Exception ex) {
        log.error("Unable to scan metadata table", ex);
        return;
      } finally {
        span.stop();
      }
      
      long logEntryScanStop = System.currentTimeMillis();
      log.info(String.format("%d log entries scanned in %.2f seconds", count, (logEntryScanStop - fileScanStop) / 1000.));
      
      span = Trace.start("removeFiles");
      Map<String,ArrayList<String>> serverToFileMap = mapServersToFiles(fileToServerMap);
      
      count = removeFiles(serverToFileMap, sortedWALogs, status);
      
      long removeStop = System.currentTimeMillis();
      log.info(String.format("%d total logs removed from %d servers in %.2f seconds", count, serverToFileMap.size(), (removeStop - logEntryScanStop) / 1000.));
      status.currentLog.finished = removeStop;
      status.lastLog = status.currentLog;
      status.currentLog = new GcCycleStats();
      span.stop();
      
    } catch (Exception e) {
      log.error("exception occured while garbage collecting write ahead logs", e);
      span.stop();
    }
  }
  
  boolean holdsLock(InetSocketAddress addr) {
    try {
      String zpath = ZooUtil.getRoot(instance) + Constants.ZTSERVERS + "/" + org.apache.accumulo.core.util.AddressUtil.toString(addr);
      List<String> children = ZooReaderWriter.getInstance().getChildren(zpath);
      return !(children == null || children.isEmpty());
    } catch (KeeperException.NoNodeException ex) {
      return false;
    } catch (Exception ex) {
      log.debug(ex, ex);
      return true;
    }
  }

  private int removeFiles(Map<String,ArrayList<String>> serverToFileMap, Set<String> sortedWALogs, final GCStatus status) {
    AccumuloConfiguration conf = instance.getConfiguration();
    for (Entry<String,ArrayList<String>> entry : serverToFileMap.entrySet()) {
      if (entry.getKey().length() == 0) {
        // old-style log entry, just remove it
        for (String filename : entry.getValue()) {
          log.debug("Removing old-style WAL " + entry.getValue());
          try {
            Path path = new Path(Constants.getWalDirectory(conf), filename);
            if (trash == null || !trash.moveToTrash(path))
              fs.delete(path, true);
          } catch (FileNotFoundException ex) {
            // ignored
          } catch (IOException ex) {
            log.error("Unable to delete wal " + filename + ": " + ex);
          }
        }
      } else {
        InetSocketAddress address = AddressUtil.parseAddress(entry.getKey(), Property.TSERV_CLIENTPORT);
        if (!holdsLock(address)) {
          Path serverPath = new Path(Constants.getWalDirectory(conf), entry.getKey());
          for (String filename : entry.getValue()) {
            log.debug("Removing WAL for offline server " + filename);
            try {
              Path path = new Path(serverPath, filename);
              if (trash == null || !trash.moveToTrash(path))
                fs.delete(path, true);
            } catch (FileNotFoundException ex) {
              // ignored
            } catch (IOException ex) {
              log.error("Unable to delete wal " + filename + ": " + ex);
            }
          }
          continue;
        } else {
          Client tserver = null;
          try {
            tserver = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
            tserver.removeLogs(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), entry.getValue());
            log.debug("deleted " + entry.getValue() + " from " + entry.getKey());
            status.currentLog.deleted += entry.getValue().size();
          } catch (TException e) {
            log.warn("Error talking to " + address + ": " + e);
          } finally {
            if (tserver != null)
              ThriftUtil.returnClient(tserver);
          }
        }
      }
    }
    
    Path recoveryDir = new Path(Constants.getRecoveryDir(conf));
    
    for (String sortedWALog : sortedWALogs) {
      log.debug("Removing sorted WAL " + sortedWALog);
      try {
        Path swalog = new Path(recoveryDir, sortedWALog);
        if (trash == null || (fs.exists(swalog) && !trash.moveToTrash(swalog))) {
          fs.delete(swalog, true);
        }
      } catch (FileNotFoundException ex) {
        // ignored
      } catch (IOException ioe) {
        log.error("Unable to delete sorted walog " + sortedWALog + ": " + ioe);
      }
    }

    return 0;
  }
  
  private static Map<String,ArrayList<String>> mapServersToFiles(Map<String,String> fileToServerMap) {
    Map<String,ArrayList<String>> serverToFileMap = new HashMap<String,ArrayList<String>>();
    for (Entry<String,String> fileServer : fileToServerMap.entrySet()) {
      ArrayList<String> files = serverToFileMap.get(fileServer.getValue());
      if (files == null) {
        files = new ArrayList<String>();
        serverToFileMap.put(fileServer.getValue(), files);
      }
      files.add(fileServer.getKey());
    }
    return serverToFileMap;
  }
  
  private static int removeMetadataEntries(Map<String,String> fileToServerMap, Set<String> sortedWALogs, GCStatus status) throws IOException, KeeperException,
      InterruptedException {
    int count = 0;
    Iterator<LogEntry> iterator = MetadataTable.getLogEntries(SecurityConstants.getSystemCredentials());
    while (iterator.hasNext()) {
      for (String filename : iterator.next().logSet) {
        filename = filename.split("/", 2)[1];
        if (fileToServerMap.remove(filename) != null)
          status.currentLog.inUse++;
        
        sortedWALogs.remove(filename);

        count++;
      }
    }
    return count;
  }
  
  private int scanServers(Map<String,String> fileToServerMap) throws Exception {
    AccumuloConfiguration conf = instance.getConfiguration();
    Path walRoot = new Path(Constants.getWalDirectory(conf));
    for (FileStatus status : fs.listStatus(walRoot)) {
      String name = status.getPath().getName();
      if (status.isDir()) {
        for (FileStatus file : fs.listStatus(new Path(walRoot, name))) {
          if (isUUID(file.getPath().getName()))
            fileToServerMap.put(file.getPath().getName(), name);
          else {
            log.info("Ignoring file " + file.getPath() + " because it doesn't look like a uuid");
          }
        }
      } else if (isUUID(name)) {
        // old-style WAL are not under a directory
        fileToServerMap.put(name, "");
      } else {
        log.info("Ignoring file " + name + " because it doesn't look like a uuid");
      }
    }

    int count = 0;
    return count;
  }
  
  private Set<String> getSortedWALogs() throws IOException {
    AccumuloConfiguration conf = instance.getConfiguration();
    Path recoveryDir = new Path(Constants.getRecoveryDir(conf));
    
    Set<String> sortedWALogs = new HashSet<String>();

    if (fs.exists(recoveryDir)) {
      for (FileStatus status : fs.listStatus(recoveryDir)) {
        if (isUUID(status.getPath().getName())) {
          sortedWALogs.add(status.getPath().getName());
        } else {
          log.debug("Ignoring file " + status.getPath() + " because it doesn't look like a uuid");
        }
      }
    }
    
    return sortedWALogs;
  }

  /**
   * @param name
   * @return
   */
  static private boolean isUUID(String name) {
    try {
      UUID.fromString(name);
      return true;
    } catch (IllegalArgumentException ex) {
      return false;
    }
  }
  
}

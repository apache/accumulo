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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.cloudtrace.instrument.Span;
import org.apache.accumulo.cloudtrace.instrument.Trace;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.tabletserver.thrift.MutationLogger;
import org.apache.accumulo.core.tabletserver.thrift.MutationLogger.Iface;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.util.MetadataTable;
import org.apache.accumulo.server.util.MetadataTable.LogEntry;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;


public class GarbageCollectWriteAheadLogs {
  private static final Logger log = Logger.getLogger(GarbageCollectWriteAheadLogs.class);
  
  private final AccumuloConfiguration conf;
  private final FileSystem fs;
  
  GarbageCollectWriteAheadLogs(FileSystem fs, AccumuloConfiguration conf) {
    this.fs = fs;
    this.conf = conf;
  }

  public void collect(GCStatus status) {
    
    Span span = Trace.start("scanServers");
    try {
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
        count = removeMetadataEntries(fileToServerMap, status);
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
      
      count = removeFiles(serverToFileMap, status);
      
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
  
  private int removeFiles(Map<String,ArrayList<String>> serverToFileMap, final GCStatus status) {
    final AtomicInteger count = new AtomicInteger();
    ExecutorService threadPool = java.util.concurrent.Executors.newCachedThreadPool();
    
    for (final Entry<String,ArrayList<String>> serverFiles : serverToFileMap.entrySet()) {
      final String server = serverFiles.getKey();
      final List<String> files = serverFiles.getValue();
      threadPool.submit(new Runnable() {
        @Override
        public void run() {
          try {
            Iface logger = ThriftUtil.getClient(new MutationLogger.Client.Factory(), server, Property.LOGGER_PORT, Property.TSERV_LOGGER_TIMEOUT, conf);
            try {
              count.addAndGet(files.size());
              log.debug(String.format("removing %d files from %s", files.size(), server));
              if (files.size() > 0) {
                log.debug("deleting files on logger " + server);
                for (String file : files) {
                  log.debug("Deleting " + file);
                }
                logger.remove(null, SecurityConstants.getSystemCredentials(), files);
                synchronized (status.currentLog) {
                  status.currentLog.deleted += files.size();
                }
              }
            } finally {
              ThriftUtil.returnClient(logger);
            }
            log.info(String.format("Removed %d files from %s", files.size(), server));
            for (String file : files) {
              try {
                for (FileStatus match : fs.globStatus(new Path(ServerConstants.getRecoveryDir(), file + "*"))) {
                  fs.delete(match.getPath(), true);
                }
              } catch (IOException ex) {
                log.warn("Error deleting recovery data: ", ex);
              }
            }
          } catch (TTransportException err) {
            log.info("Ignoring communication error talking to logger " + serverFiles.getKey() + " (probably a timeout)");
          } catch (TException err) {
            log.info("Ignoring exception talking to logger " + serverFiles.getKey() + "(" + err + ")");
          }
        }
      });
      
    }
    threadPool.shutdown();
    while (!threadPool.isShutdown())
      try {
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {}
    return count.get();
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
  
  private static int removeMetadataEntries(Map<String,String> fileToServerMap, GCStatus status) throws IOException, KeeperException, InterruptedException {
    int count = 0;
    Iterator<LogEntry> iterator = MetadataTable.getLogEntries(SecurityConstants.getSystemCredentials());
    while (iterator.hasNext()) {
      for (String filename : iterator.next().logSet) {
        filename = filename.split("/", 2)[1];
        if (fileToServerMap.remove(filename) != null)
          status.currentLog.inUse++;
        count++;
      }
    }
    return count;
  }
  
  private int scanServers(Map<String,String> fileToServerMap) throws Exception {
    int count = 0;
    IZooReaderWriter zk = ZooReaderWriter.getInstance();
    String loggersDir = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZLOGGERS;
    List<String> servers = zk.getChildren(loggersDir, null);
    Collections.shuffle(servers);
    for (String server : servers) {
      String address = "no-data";
      count++;
      try {
        byte[] data = zk.getData(loggersDir + "/" + server, null);
        address = new String(data);
        Iface logger = ThriftUtil.getClient(new MutationLogger.Client.Factory(), address, Property.LOGGER_PORT, Property.TSERV_LOGGER_TIMEOUT, conf);
        for (String log : logger.getClosedLogs(null, SecurityConstants.getSystemCredentials())) {
          fileToServerMap.put(log, address);
        }
        ThriftUtil.returnClient(logger);
      } catch (TException err) {
        log.warn("Ignoring exception talking to logger " + address);
      }
      if (SimpleGarbageCollector.almostOutOfMemory()) {
        log.warn("Running out of memory collecting write-ahead log file names from loggers, continuing with a partial list");
        break;
      }
    }
    return count;
  }
  
}

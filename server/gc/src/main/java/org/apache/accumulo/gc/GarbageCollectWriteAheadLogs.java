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
package org.apache.accumulo.gc;

import java.io.FileNotFoundException;
import java.io.IOException;
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
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;

import com.google.common.net.HostAndPort;

public class GarbageCollectWriteAheadLogs {
  private static final Logger log = Logger.getLogger(GarbageCollectWriteAheadLogs.class);

  private final Instance instance;
  private final VolumeManager fs;

  private boolean useTrash;

  /**
   * Creates a new GC WAL object.
   *
   * @param instance
   *          instance to use
   * @param fs
   *          volume manager to use
   * @param useTrash
   *          true to move files to trash rather than delete them
   */
  GarbageCollectWriteAheadLogs(Instance instance, VolumeManager fs, boolean useTrash) throws IOException {
    this.instance = instance;
    this.fs = fs;
    this.useTrash = useTrash;
  }

  /**
   * Gets the instance used by this object.
   *
   * @return instance
   */
  Instance getInstance() {
    return instance;
  }

  /**
   * Gets the volume manager used by this object.
   *
   * @return volume manager
   */
  VolumeManager getVolumeManager() {
    return fs;
  }

  /**
   * Checks if the volume manager should move files to the trash rather than delete them.
   *
   * @return true if trash is used
   */
  boolean isUsingTrash() {
    return useTrash;
  }

  public void collect(GCStatus status) {

    Span span = Trace.start("scanServers");
    try {

      Map<String,Path> sortedWALogs = getSortedWALogs();

      status.currentLog.started = System.currentTimeMillis();

      Map<Path,String> fileToServerMap = new HashMap<Path,String>();
      Map<String,Path> nameToFileMap = new HashMap<String,Path>();
      int count = scanServers(fileToServerMap, nameToFileMap);
      long fileScanStop = System.currentTimeMillis();
      log.info(String.format("Fetched %d files from %d servers in %.2f seconds", fileToServerMap.size(), count,
          (fileScanStop - status.currentLog.started) / 1000.));
      status.currentLog.candidates = fileToServerMap.size();
      span.stop();

      span = Trace.start("removeMetadataEntries");
      try {
        count = removeMetadataEntries(nameToFileMap, sortedWALogs, status);
      } catch (Exception ex) {
        log.error("Unable to scan metadata table", ex);
        return;
      } finally {
        span.stop();
      }

      long logEntryScanStop = System.currentTimeMillis();
      log.info(String.format("%d log entries scanned in %.2f seconds", count, (logEntryScanStop - fileScanStop) / 1000.));

      span = Trace.start("removeFiles");
      Map<String,ArrayList<Path>> serverToFileMap = mapServersToFiles(fileToServerMap, nameToFileMap);

      count = removeFiles(nameToFileMap, serverToFileMap, sortedWALogs, status);

      long removeStop = System.currentTimeMillis();
      log.info(String.format("%d total logs removed from %d servers in %.2f seconds", count, serverToFileMap.size(), (removeStop - logEntryScanStop) / 1000.));
      status.currentLog.finished = removeStop;
      status.lastLog = status.currentLog;
      status.currentLog = new GcCycleStats();
      span.stop();

    } catch (Exception e) {
      log.error("exception occured while garbage collecting write ahead logs", e);
    } finally {
      span.stop();
    }
  }

  boolean holdsLock(HostAndPort addr) {
    try {
      String zpath = ZooUtil.getRoot(instance) + Constants.ZTSERVERS + "/" + addr.toString();
      List<String> children = ZooReaderWriter.getInstance().getChildren(zpath);
      return !(children == null || children.isEmpty());
    } catch (KeeperException.NoNodeException ex) {
      return false;
    } catch (Exception ex) {
      log.debug(ex, ex);
      return true;
    }
  }

  private int removeFiles(Map<String,Path> nameToFileMap, Map<String,ArrayList<Path>> serverToFileMap, Map<String,Path> sortedWALogs, final GCStatus status) {
    AccumuloConfiguration conf = ServerConfiguration.getSystemConfiguration(instance);
    for (Entry<String,ArrayList<Path>> entry : serverToFileMap.entrySet()) {
      if (entry.getKey().isEmpty()) {
        // old-style log entry, just remove it
        for (Path path : entry.getValue()) {
          log.debug("Removing old-style WAL " + path);
          try {
            if (!useTrash || !fs.moveToTrash(path))
              fs.deleteRecursively(path);
            status.currentLog.deleted++;
          } catch (FileNotFoundException ex) {
            // ignored
          } catch (IOException ex) {
            log.error("Unable to delete wal " + path + ": " + ex);
          }
        }
      } else {
        HostAndPort address = AddressUtil.parseAddress(entry.getKey(), false);
        if (!holdsLock(address)) {
          for (Path path : entry.getValue()) {
            log.debug("Removing WAL for offline server " + path);
            try {
              if (!useTrash || !fs.moveToTrash(path))
                fs.deleteRecursively(path);
              status.currentLog.deleted++;
            } catch (FileNotFoundException ex) {
              // ignored
            } catch (IOException ex) {
              log.error("Unable to delete wal " + path + ": " + ex);
            }
          }
          continue;
        } else {
          Client tserver = null;
          try {
            tserver = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
            tserver.removeLogs(Tracer.traceInfo(), SystemCredentials.get().toThrift(instance), paths2strings(entry.getValue()));
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

    for (Path swalog : sortedWALogs.values()) {
      log.debug("Removing sorted WAL " + swalog);
      try {
        if (!useTrash || !fs.moveToTrash(swalog)) {
          fs.deleteRecursively(swalog);
        }
      } catch (FileNotFoundException ex) {
        // ignored
      } catch (IOException ioe) {
        try {
          if (fs.exists(swalog)) {
            log.error("Unable to delete sorted walog " + swalog + ": " + ioe);
          }
        } catch (IOException ex) {
          log.error("Unable to check for the existence of " + swalog, ex);
        }
      }
    }

    return 0;
  }

  /**
   * Converts a list of paths to their corresponding strings.
   *
   * @param paths
   *          list of paths
   * @return string forms of paths
   */
  static List<String> paths2strings(List<Path> paths) {
    List<String> result = new ArrayList<String>(paths.size());
    for (Path path : paths)
      result.add(path.toString());
    return result;
  }

  /**
   * Reverses the given mapping of file paths to servers. The returned map provides a list of file paths for each server. Any path whose name is not in the
   * mapping of file names to paths is skipped.
   *
   * @param fileToServerMap
   *          map of file paths to servers
   * @param nameToFileMap
   *          map of file names to paths
   * @return map of servers to lists of file paths
   */
  static Map<String,ArrayList<Path>> mapServersToFiles(Map<Path,String> fileToServerMap, Map<String,Path> nameToFileMap) {
    Map<String,ArrayList<Path>> result = new HashMap<String,ArrayList<Path>>();
    for (Entry<Path,String> fileServer : fileToServerMap.entrySet()) {
      if (!nameToFileMap.containsKey(fileServer.getKey().getName()))
        continue;
      ArrayList<Path> files = result.get(fileServer.getValue());
      if (files == null) {
        files = new ArrayList<Path>();
        result.put(fileServer.getValue(), files);
      }
      files.add(fileServer.getKey());
    }
    return result;
  }

  private int removeMetadataEntries(Map<String,Path> nameToFileMap, Map<String,Path> sortedWALogs, GCStatus status) throws IOException, KeeperException,
      InterruptedException {
    int count = 0;
    Iterator<LogEntry> iterator = MetadataTableUtil.getLogEntries(SystemCredentials.get());

    while (iterator.hasNext()) {
      for (String entry : iterator.next().logSet) {
        // old style WALs will have the IP:Port of their logger and new style will either be a Path either absolute or relative, in all cases
        // the last "/" will mark a UUID file name.
        String uuid = entry.substring(entry.lastIndexOf("/") + 1);
        if (!isUUID(uuid)) {
          // fully expect this to be a uuid, if its not then something is wrong and walog GC should not proceed!
          throw new IllegalArgumentException("Expected uuid, but got " + uuid + " from " + entry);
        }

        Path pathFromNN = nameToFileMap.remove(uuid);
        if (pathFromNN != null) {
          status.currentLog.inUse++;
          sortedWALogs.remove(uuid);
        }
        count++;
      }
    }
    return count;
  }

  private int scanServers(Map<Path,String> fileToServerMap, Map<String,Path> nameToFileMap) throws Exception {
    return scanServers(ServerConstants.getWalDirs(), fileToServerMap, nameToFileMap);
  }

  // TODO Remove deprecation warning suppression when Hadoop1 support is dropped
  @SuppressWarnings("deprecation")
  /**
   * Scans write-ahead log directories for logs. The maps passed in are
   * populated with scan information.
   *
   * @param walDirs write-ahead log directories
   * @param fileToServerMap map of file paths to servers
   * @param nameToFileMap map of file names to paths
   * @return number of servers located (including those with no logs present)
   */
  int scanServers(String[] walDirs, Map<Path,String> fileToServerMap, Map<String,Path> nameToFileMap) throws Exception {
    Set<String> servers = new HashSet<String>();
    for (String walDir : walDirs) {
      Path walRoot = new Path(walDir);
      FileStatus[] listing = null;
      try {
        listing = fs.listStatus(walRoot);
      } catch (FileNotFoundException e) {
        // ignore dir
      }

      if (listing == null)
        continue;
      for (FileStatus status : listing) {
        String server = status.getPath().getName();
        if (status.isDir()) {
          servers.add(server);
          for (FileStatus file : fs.listStatus(new Path(walRoot, server))) {
            if (isUUID(file.getPath().getName())) {
              fileToServerMap.put(file.getPath(), server);
              nameToFileMap.put(file.getPath().getName(), file.getPath());
            } else {
              log.info("Ignoring file " + file.getPath() + " because it doesn't look like a uuid");
            }
          }
        } else if (isUUID(server)) {
          // old-style WAL are not under a directory
          servers.add("");
          fileToServerMap.put(status.getPath(), "");
          nameToFileMap.put(server, status.getPath());
        } else {
          log.info("Ignoring file " + status.getPath() + " because it doesn't look like a uuid");
        }
      }
    }
    return servers.size();
  }

  private Map<String,Path> getSortedWALogs() throws IOException {
    return getSortedWALogs(ServerConstants.getRecoveryDirs());
  }

  /**
   * Looks for write-ahead logs in recovery directories.
   *
   * @param recoveryDirs
   *          recovery directories
   * @return map of log file names to paths
   */
  Map<String,Path> getSortedWALogs(String[] recoveryDirs) throws IOException {
    Map<String,Path> result = new HashMap<String,Path>();

    for (String dir : recoveryDirs) {
      Path recoveryDir = new Path(dir);

      if (fs.exists(recoveryDir)) {
        for (FileStatus status : fs.listStatus(recoveryDir)) {
          String name = status.getPath().getName();
          if (isUUID(name)) {
            result.put(name, status.getPath());
          } else {
            log.debug("Ignoring file " + status.getPath() + " because it doesn't look like a uuid");
          }
        }
      }
    }
    return result;
  }

  /**
   * Checks if a string is a valid UUID.
   *
   * @param name
   *          string to check
   * @return true if string is a UUID
   */
  static boolean isUUID(String name) {
    if (name == null || name.length() != 36) {
      return false;
    }
    try {
      UUID.fromString(name);
      return true;
    } catch (IllegalArgumentException ex) {
      return false;
    }
  }

}

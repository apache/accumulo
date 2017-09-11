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
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTableOfflineException;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;

public class GarbageCollectWriteAheadLogs {
  private static final Logger log = LoggerFactory.getLogger(GarbageCollectWriteAheadLogs.class);

  private final AccumuloServerContext context;
  private final VolumeManager fs;
  private final Map<HostAndPort,Long> firstSeenDead;

  private boolean useTrash;

  /**
   * Creates a new GC WAL object.
   *
   * @param context
   *          the collection server's context
   * @param fs
   *          volume manager to use
   * @param useTrash
   *          true to move files to trash rather than delete them
   * @param firstSeenDead
   *          mutable map of a host to when it was first seen dead
   */
  GarbageCollectWriteAheadLogs(AccumuloServerContext context, VolumeManager fs, boolean useTrash, Map<HostAndPort,Long> firstSeenDead) throws IOException {
    this.context = context;
    this.fs = fs;
    this.useTrash = useTrash;
    this.firstSeenDead = firstSeenDead;
  }

  /**
   * Gets the instance used by this object.
   *
   * @return instance
   */
  Instance getInstance() {
    return context.getInstance();
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

  /**
   * Removes all the WAL files that are no longer used.
   * <p>
   *
   * This method is not Threadsafe. SimpleGarbageCollector#run does not invoke collect in a concurrent manner.
   *
   * @param status
   *          GCStatus object
   */
  public void collect(GCStatus status) {

    Span span = Trace.start("scanServers");
    try {

      Map<String,Path> sortedWALogs = getSortedWALogs();

      status.currentLog.started = System.currentTimeMillis();

      Map<Path,String> fileToServerMap = new HashMap<>();
      Map<String,Path> nameToFileMap = new HashMap<>();
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

      span = Trace.start("removeReplicationEntries");
      try {
        count = removeReplicationEntries(nameToFileMap, sortedWALogs, status);
      } catch (Exception ex) {
        log.error("Unable to scan replication table", ex);
        return;
      } finally {
        span.stop();
      }

      long replicationEntryScanStop = System.currentTimeMillis();
      log.info(String.format("%d replication entries scanned in %.2f seconds", count, (replicationEntryScanStop - logEntryScanStop) / 1000.));

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
      String zpath = ZooUtil.getRoot(context.getInstance()) + Constants.ZTSERVERS + "/" + addr.toString();
      List<String> children = ZooReaderWriter.getInstance().getChildren(zpath);
      return !(children == null || children.isEmpty());
    } catch (KeeperException.NoNodeException ex) {
      return false;
    } catch (Exception ex) {
      log.debug(ex.toString(), ex);
      return true;
    }
  }

  private AccumuloConfiguration getConfig() {
    return context.getServerConfigurationFactory().getConfiguration();
  }

  /**
   * Top level method for removing WAL files.
   * <p>
   * Loops over all the gathered WAL and sortedWAL entries and calls the appropriate methods for removal
   *
   * @param nameToFileMap
   *          Map of filename to Path
   * @param serverToFileMap
   *          Map of HostAndPort string to a list of Paths
   * @param sortedWALogs
   *          Map of sorted WAL names to Path
   * @param status
   *          GCStatus object for tracking what is done
   * @return 0 always
   */
  @VisibleForTesting
  int removeFiles(Map<String,Path> nameToFileMap, Map<String,ArrayList<Path>> serverToFileMap, Map<String,Path> sortedWALogs, final GCStatus status) {
    // TODO: remove nameToFileMap from method signature, not used here I don't think
    AccumuloConfiguration conf = getConfig();
    for (Entry<String,ArrayList<Path>> entry : serverToFileMap.entrySet()) {
      if (entry.getKey().isEmpty()) {
        removeOldStyleWAL(entry, status);
      } else {
        removeWALFile(entry, conf, status);
      }
    }
    for (Path swalog : sortedWALogs.values()) {
      removeSortedWAL(swalog);
    }
    return 0;
  }

  /**
   * Removes sortedWALs.
   * <p>
   * Sorted WALs are WALs that are in the recovery directory and have already been used.
   *
   * @param swalog
   *          Path to the WAL
   */
  @VisibleForTesting
  void removeSortedWAL(Path swalog) {
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

  /**
   * A wrapper method to check if the tserver using the WAL is still alive
   * <p>
   * Delegates to the deletion to #removeWALfromDownTserver if the ZK lock is gone or #askTserverToRemoveWAL if the server is known to still be alive
   *
   * @param entry
   *          WAL information gathered
   * @param conf
   *          AccumuloConfiguration object
   * @param status
   *          GCStatus object
   */
  void removeWALFile(Entry<String,ArrayList<Path>> entry, AccumuloConfiguration conf, final GCStatus status) {
    HostAndPort address = AddressUtil.parseAddress(entry.getKey(), false);
    if (!holdsLock(address)) {
      removeWALfromDownTserver(address, conf, entry, status);
    } else {
      askTserverToRemoveWAL(address, conf, entry, status);
    }
  }

  /**
   * Asks a currently running tserver to remove it's WALs.
   * <p>
   * A tserver has more information about whether a WAL is still being used for current mutations. It is safer to ask the tserver to remove the file instead of
   * just relying on information in the metadata table.
   *
   * @param address
   *          HostAndPort of the tserver
   * @param conf
   *          AccumuloConfiguration entry
   * @param entry
   *          WAL information gathered
   * @param status
   *          GCStatus object
   */
  @VisibleForTesting
  void askTserverToRemoveWAL(HostAndPort address, AccumuloConfiguration conf, Entry<String,ArrayList<Path>> entry, final GCStatus status) {
    firstSeenDead.remove(address);
    Client tserver = null;
    try {
      tserver = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, context);
      tserver.removeLogs(Tracer.traceInfo(), context.rpcCreds(), paths2strings(entry.getValue()));
      log.debug("asked tserver to delete " + entry.getValue() + " from " + entry.getKey());
      status.currentLog.deleted += entry.getValue().size();
    } catch (TException e) {
      log.warn("Error talking to " + address + ": " + e);
    } finally {
      if (tserver != null)
        ThriftUtil.returnClient(tserver);
    }
  }

  /**
   * Get the configured wait period a server has to be dead.
   * <p>
   * The property is "gc.wal.dead.server.wait" defined in Property.GC_WAL_DEAD_SERVER_WAIT and is duration. Valid values include a unit with no space like
   * 3600s, 5m or 2h.
   *
   * @param conf
   *          AccumuloConfiguration
   * @return long that represents the millis to wait
   */
  @VisibleForTesting
  long getGCWALDeadServerWaitTime(AccumuloConfiguration conf) {
    return conf.getTimeInMillis(Property.GC_WAL_DEAD_SERVER_WAIT);
  }

  /**
   * Remove walogs associated with a tserver that no longer has a look.
   * <p>
   * There is configuration option, see #getGCWALDeadServerWaitTime, that defines how long a server must be "dead" before removing the associated write ahead
   * log files. The intent to ensure that recovery succeeds for the tablet that were host on that tserver.
   *
   * @param address
   *          HostAndPort of the tserver with no lock
   * @param conf
   *          AccumuloConfiguration to get that gc.wal.dead.server.wait info
   * @param entry
   *          The WALOG path
   * @param status
   *          GCStatus for tracking changes
   */
  @VisibleForTesting
  void removeWALfromDownTserver(HostAndPort address, AccumuloConfiguration conf, Entry<String,ArrayList<Path>> entry, final GCStatus status) {
    // tserver is down, only delete once configured time has passed
    if (timeToDelete(address, getGCWALDeadServerWaitTime(conf))) {
      for (Path path : entry.getValue()) {
        log.debug("Removing WAL for offline server " + address + " at " + path);
        try {
          if (!useTrash || !fs.moveToTrash(path)) {
            fs.deleteRecursively(path);
          }
          status.currentLog.deleted++;
        } catch (FileNotFoundException ex) {
          // ignored
        } catch (IOException ex) {
          log.error("Unable to delete wal " + path + ": " + ex);
        }
      }
      firstSeenDead.remove(address);
    } else {
      log.debug("Not removing " + entry.getValue().size() + " WAL(s) for offline server since it has not be long enough: " + address);
    }
  }

  /**
   * Removes old style WAL entries.
   * <p>
   * The format for storing WAL info in the metadata table changed at some point, maybe the 1.5 release. Once that is known for sure and we no longer support
   * upgrading from that version, this code should be removed
   *
   * @param entry
   *          Map of empty server address to List of Paths
   * @param status
   *          GCStatus object
   */
  @VisibleForTesting
  void removeOldStyleWAL(Entry<String,ArrayList<Path>> entry, final GCStatus status) {
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
  }

  /**
   * Converts a list of paths to their corresponding strings.
   *
   * @param paths
   *          list of paths
   * @return string forms of paths
   */
  static List<String> paths2strings(List<Path> paths) {
    List<String> result = new ArrayList<>(paths.size());
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
    Map<String,ArrayList<Path>> result = new HashMap<>();
    for (Entry<Path,String> fileServer : fileToServerMap.entrySet()) {
      if (!nameToFileMap.containsKey(fileServer.getKey().getName()))
        continue;
      ArrayList<Path> files = result.get(fileServer.getValue());
      if (files == null) {
        files = new ArrayList<>();
        result.put(fileServer.getValue(), files);
      }
      files.add(fileServer.getKey());
    }
    return result;
  }

  @VisibleForTesting
  int removeMetadataEntries(Map<String,Path> nameToFileMap, Map<String,Path> sortedWALogs, GCStatus status) throws IOException, KeeperException,
      InterruptedException {
    int count = 0;
    Iterator<LogEntry> iterator = MetadataTableUtil.getLogEntries(context);

    // For each WAL reference in the metadata table
    while (iterator.hasNext()) {
      // Each metadata reference has at least one WAL file
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

  protected int removeReplicationEntries(Map<String,Path> nameToFileMap, Map<String,Path> sortedWALogs, GCStatus status) throws IOException, KeeperException,
      InterruptedException {
    Connector conn;
    try {
      conn = context.getConnector();
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("Failed to get connector", e);
      throw new IllegalArgumentException(e);
    }

    int count = 0;

    Iterator<Entry<String,Path>> walIter = nameToFileMap.entrySet().iterator();

    while (walIter.hasNext()) {
      Entry<String,Path> wal = walIter.next();
      String fullPath = wal.getValue().toString();
      if (neededByReplication(conn, fullPath)) {
        log.debug("Removing WAL from candidate deletion as it is still needed for replication: {}", fullPath);
        // If we haven't already removed it, check to see if this WAL is
        // "in use" by replication (needed for replication purposes)
        status.currentLog.inUse++;

        walIter.remove();
        sortedWALogs.remove(wal.getKey());
      } else {
        log.debug("WAL not needed for replication {}", fullPath);
      }
      count++;
    }

    return count;
  }

  /**
   * Determine if the given WAL is needed for replication
   *
   * @param wal
   *          The full path (URI)
   * @return True if the WAL is still needed by replication (not a candidate for deletion)
   */
  protected boolean neededByReplication(Connector conn, String wal) {
    log.info("Checking replication table for " + wal);

    Iterable<Entry<Key,Value>> iter = getReplicationStatusForFile(conn, wal);

    // TODO Push down this filter to the tserver to only return records
    // that are not completely replicated and convert this loop into a
    // `return s.iterator.hasNext()` statement
    for (Entry<Key,Value> entry : iter) {
      try {
        Status status = Status.parseFrom(entry.getValue().get());
        log.info("Checking if {} is safe for removal with {}", wal, ProtobufUtil.toString(status));
        if (!StatusUtil.isSafeForRemoval(status)) {
          return true;
        }
      } catch (InvalidProtocolBufferException e) {
        log.error("Could not deserialize Status protobuf for " + entry.getKey(), e);
      }
    }

    return false;
  }

  protected Iterable<Entry<Key,Value>> getReplicationStatusForFile(Connector conn, String wal) {
    Scanner metaScanner;
    try {
      metaScanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }

    // Need to add in the replication section prefix
    metaScanner.setRange(Range.exact(ReplicationSection.getRowPrefix() + wal));
    // Limit the column family to be sure
    metaScanner.fetchColumnFamily(ReplicationSection.COLF);

    try {
      Scanner replScanner = ReplicationTable.getScanner(conn);

      // Scan only the Status records
      StatusSection.limit(replScanner);

      // Only look for this specific WAL
      replScanner.setRange(Range.exact(wal));

      return Iterables.concat(metaScanner, replScanner);
    } catch (ReplicationTableOfflineException e) {
      // do nothing
    }

    return metaScanner;
  }

  @VisibleForTesting
  int scanServers(Map<Path,String> fileToServerMap, Map<String,Path> nameToFileMap) throws Exception {
    return scanServers(ServerConstants.getWalDirs(), fileToServerMap, nameToFileMap);
  }

  /**
   * Scans write-ahead log directories for logs. The maps passed in are populated with scan information.
   *
   * @param walDirs
   *          write-ahead log directories
   * @param fileToServerMap
   *          map of file paths to servers
   * @param nameToFileMap
   *          map of file names to paths
   * @return number of servers located (including those with no logs present)
   */
  int scanServers(String[] walDirs, Map<Path,String> fileToServerMap, Map<String,Path> nameToFileMap) throws Exception {
    Set<String> servers = new HashSet<>();
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
        if (status.isDirectory()) {
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

  @VisibleForTesting
  Map<String,Path> getSortedWALogs() throws IOException {
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
    Map<String,Path> result = new HashMap<>();

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

  /**
   * Determine if TServer has been dead long enough to remove associated WALs.
   * <p>
   * Uses a map where the key is the address and the value is the time first seen dead. If the address is not in the map, it is added with the current system
   * nanoTime. When the passed in wait time has elapsed, this method returns true and removes the key and value from the map.
   *
   * @param address
   *          HostAndPort of dead tserver
   * @param wait
   *          long value of elapsed millis to wait
   * @return boolean whether enough time elapsed since the server was first seen as dead.
   */
  @VisibleForTesting
  protected boolean timeToDelete(HostAndPort address, long wait) {
    // check whether the tserver has been dead long enough
    Long firstSeen = firstSeenDead.get(address);
    if (firstSeen != null) {
      long elapsedTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - firstSeen);
      log.trace("Elapsed milliseconds since " + address + " first seen dead: " + elapsedTime);
      return elapsedTime > wait;
    } else {
      log.trace("Adding server to firstSeenDead map " + address);
      firstSeenDead.put(address, System.nanoTime());
      return false;
    }
  }

  /**
   * Method to clear the map used in timeToDelete.
   * <p>
   * Useful for testing.
   */
  @VisibleForTesting
  void clearFirstSeenDead() {
    firstSeenDead.clear();
  }

}

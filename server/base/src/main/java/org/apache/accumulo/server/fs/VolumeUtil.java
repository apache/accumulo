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
package org.apache.accumulo.server.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for managing absolute URIs contained in Accumulo metadata.
 */
public class VolumeUtil {

  private static final Logger log = LoggerFactory.getLogger(VolumeUtil.class);
  private static final SecureRandom rand = new SecureRandom();

  private static boolean isActiveVolume(Path dir) {

    // consider relative path as active and take no action
    if (!dir.toString().contains(":"))
      return true;

    for (String tableDir : ServerConstants.getTablesDirs()) {
      // use Path to normalize tableDir
      if (dir.toString().startsWith(new Path(tableDir).toString()))
        return true;
    }

    return false;
  }

  public static String removeTrailingSlash(String path) {
    while (path.endsWith("/"))
      path = path.substring(0, path.length() - 1);
    return path;
  }

  public static Path removeTrailingSlash(Path path) {
    if (path.toString().endsWith("/"))
      return new Path(removeTrailingSlash(path.toString()));
    return path;
  }

  public static String switchVolume(String path, FileType ft, List<Pair<Path,Path>> replacements) {
    if (replacements.size() == 0) {
      log.trace("Not switching volume because there are no replacements");
      return null;
    }

    if (!path.contains(":")) {
      // ignore relative paths
      return null;
    }

    Path p = new Path(path);

    // removing slash because new Path("hdfs://nn1").equals(new Path("hdfs://nn1/")) evaluates to false
    Path volume = removeTrailingSlash(ft.getVolume(p));

    for (Pair<Path,Path> pair : replacements) {
      Path key = removeTrailingSlash(pair.getFirst());

      if (key.equals(volume)) {
        String replacement = new Path(pair.getSecond(), ft.removeVolume(p)).toString();
        log.trace("Replacing {} with {}", path, replacement);
        return replacement;
      }
    }

    log.trace("Could not find replacement for {} at {}", ft, path);

    return null;
  }

  private static LogEntry switchVolumes(LogEntry le, List<Pair<Path,Path>> replacements) {
    String switchedPath = switchVolume(le.filename, FileType.WAL, replacements);
    int numSwitched = 0;
    if (switchedPath != null)
      numSwitched++;
    else
      switchedPath = le.filename;

    ArrayList<String> switchedLogs = new ArrayList<>();
    String switchedLog = switchVolume(le.filename, FileType.WAL, replacements);
    if (switchedLog != null) {
      switchedLogs.add(switchedLog);
      numSwitched++;
    } else {
      switchedLogs.add(le.filename);
    }

    if (numSwitched == 0) {
      log.trace("Did not switch {}", le);
      return null;
    }

    LogEntry newLogEntry = new LogEntry(le.extent, le.timestamp, le.server, switchedPath);

    log.trace("Switched {} to {}", le, newLogEntry);

    return newLogEntry;
  }

  public static class TabletFiles {
    public String dir;
    public List<LogEntry> logEntries;
    public SortedMap<FileRef,DataFileValue> datafiles;

    public TabletFiles() {
      logEntries = new ArrayList<>();
      datafiles = new TreeMap<>();
    }

    public TabletFiles(String dir, List<LogEntry> logEntries, SortedMap<FileRef,DataFileValue> datafiles) {
      this.dir = dir;
      this.logEntries = logEntries;
      this.datafiles = datafiles;
    }
  }

  public static String switchRootTableVolume(String location) throws IOException {
    String newLocation = switchVolume(location, FileType.TABLE, ServerConstants.getVolumeReplacements());
    if (newLocation != null) {
      MetadataTableUtil.setRootTabletDir(newLocation);
      log.info("Volume replaced: {} -> {}", location, newLocation);
      return new Path(newLocation).toString();
    }
    return location;
  }

  /**
   * This method does two things. First, it switches any volumes a tablet is using that are configured in instance.volumes.replacements. Second, if a tablet dir
   * is no longer configured for use it chooses a new tablet directory.
   */
  public static TabletFiles updateTabletVolumes(AccumuloServerContext context, ZooLock zooLock, VolumeManager vm, KeyExtent extent, TabletFiles tabletFiles,
      boolean replicate) throws IOException {
    List<Pair<Path,Path>> replacements = ServerConstants.getVolumeReplacements();
    log.trace("Using volume replacements: {}", replacements);

    List<LogEntry> logsToRemove = new ArrayList<>();
    List<LogEntry> logsToAdd = new ArrayList<>();

    List<FileRef> filesToRemove = new ArrayList<>();
    SortedMap<FileRef,DataFileValue> filesToAdd = new TreeMap<>();

    TabletFiles ret = new TabletFiles();

    for (LogEntry logEntry : tabletFiles.logEntries) {
      LogEntry switchedLogEntry = switchVolumes(logEntry, replacements);
      if (switchedLogEntry != null) {
        logsToRemove.add(logEntry);
        logsToAdd.add(switchedLogEntry);
        ret.logEntries.add(switchedLogEntry);
        log.debug("Replacing volume {} : {} -> {}", extent, logEntry.filename, switchedLogEntry.filename);
      } else {
        ret.logEntries.add(logEntry);
      }
    }

    if (extent.isRootTablet()) {
      ret.datafiles = tabletFiles.datafiles;
    } else {
      for (Entry<FileRef,DataFileValue> entry : tabletFiles.datafiles.entrySet()) {
        String metaPath = entry.getKey().meta().toString();
        String switchedPath = switchVolume(metaPath, FileType.TABLE, replacements);
        if (switchedPath != null) {
          filesToRemove.add(entry.getKey());
          FileRef switchedRef = new FileRef(switchedPath, new Path(switchedPath));
          filesToAdd.put(switchedRef, entry.getValue());
          ret.datafiles.put(switchedRef, entry.getValue());
          log.debug("Replacing volume {} : {} -> {}", extent, metaPath, switchedPath);
        } else {
          ret.datafiles.put(entry.getKey(), entry.getValue());
        }
      }
    }

    String tabletDir = tabletFiles.dir;
    String switchedDir = switchVolume(tabletDir, FileType.TABLE, replacements);

    if (switchedDir != null) {
      log.debug("Replacing volume {} : {} -> {}", extent, tabletDir, switchedDir);
      tabletDir = switchedDir;
    }

    if (logsToRemove.size() + filesToRemove.size() > 0 || switchedDir != null) {
      MetadataTableUtil.updateTabletVolumes(extent, logsToRemove, logsToAdd, filesToRemove, filesToAdd, switchedDir, zooLock, context);
      if (replicate) {
        Status status = StatusUtil.fileClosed();
        log.debug("Tablet directory switched, need to record old log files {} {}", logsToRemove, ProtobufUtil.toString(status));
        // Before deleting these logs, we need to mark them for replication
        for (LogEntry logEntry : logsToRemove) {
          ReplicationTableUtil.updateFiles(context, extent, logEntry.filename, status);
        }
      }
    }

    ret.dir = decommisionedTabletDir(context, zooLock, vm, extent, tabletDir);
    if (extent.isRootTablet()) {
      SortedMap<FileRef,DataFileValue> copy = ret.datafiles;
      ret.datafiles = new TreeMap<>();
      for (Entry<FileRef,DataFileValue> entry : copy.entrySet()) {
        ret.datafiles.put(new FileRef(new Path(ret.dir, entry.getKey().path().getName()).toString()), entry.getValue());
      }
    }

    // method this should return the exact strings that are in the metadata table
    return ret;
  }

  private static String decommisionedTabletDir(AccumuloServerContext context, ZooLock zooLock, VolumeManager vm, KeyExtent extent, String metaDir)
      throws IOException {
    Path dir = new Path(metaDir);
    if (isActiveVolume(dir))
      return metaDir;

    if (!dir.getParent().getParent().getName().equals(ServerConstants.TABLE_DIR)) {
      throw new IllegalArgumentException("Unexpected table dir " + dir);
    }

    VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironment(extent.getTableId());
    Path newDir = new Path(vm.choose(chooserEnv, ServerConstants.getBaseUris()) + Path.SEPARATOR + ServerConstants.TABLE_DIR + Path.SEPARATOR
        + dir.getParent().getName() + Path.SEPARATOR + dir.getName());

    log.info("Updating directory for {} from {} to {}", extent, dir, newDir);
    if (extent.isRootTablet()) {
      // the root tablet is special case, its files need to be copied if its dir is changed

      // this code needs to be idempotent

      FileSystem fs1 = vm.getVolumeByPath(dir).getFileSystem();
      FileSystem fs2 = vm.getVolumeByPath(newDir).getFileSystem();

      if (!same(fs1, dir, fs2, newDir)) {
        if (fs2.exists(newDir)) {
          Path newDirBackup = getBackupName(fs2, newDir);
          // never delete anything because were dealing with the root tablet
          // one reason this dir may exist is because this method failed previously
          log.info("renaming {} to {}", newDir, newDirBackup);
          if (!fs2.rename(newDir, newDirBackup)) {
            throw new IOException("Failed to rename " + newDir + " to " + newDirBackup);
          }
        }

        // do a lot of logging since this is the root tablet
        log.info("copying {} to {}", dir, newDir);
        if (!FileUtil.copy(fs1, dir, fs2, newDir, false, CachedConfiguration.getInstance())) {
          throw new IOException("Failed to copy " + dir + " to " + newDir);
        }

        // only set the new location in zookeeper after a successful copy
        log.info("setting root tablet location to {}", newDir);
        MetadataTableUtil.setRootTabletDir(newDir.toString());

        // rename the old dir to avoid confusion when someone looks at filesystem... its ok if we fail here and this does not happen because the location in
        // zookeeper is the authority
        Path dirBackup = getBackupName(fs1, dir);
        log.info("renaming {} to {}", dir, dirBackup);
        fs1.rename(dir, dirBackup);

      } else {
        log.info("setting root tablet location to {}", newDir);
        MetadataTableUtil.setRootTabletDir(newDir.toString());
      }

      return newDir.toString();
    } else {
      MetadataTableUtil.updateTabletDir(extent, newDir.toString(), context, zooLock);
      return newDir.toString();
    }
  }

  static boolean same(FileSystem fs1, Path dir, FileSystem fs2, Path newDir) throws FileNotFoundException, IOException {
    // its possible that a user changes config in such a way that two uris point to the same thing. Like hdfs://foo/a/b and hdfs://1.2.3.4/a/b both reference
    // the same thing because DNS resolves foo to 1.2.3.4. This method does not analyze uris to determine if equivalent, instead it inspects the contents of
    // what the uris point to.

    // this code is called infrequently and does not need to be optimized.

    if (fs1.exists(dir) && fs2.exists(newDir)) {

      if (!fs1.isDirectory(dir))
        throw new IllegalArgumentException("expected " + dir + " to be a directory");

      if (!fs2.isDirectory(newDir))
        throw new IllegalArgumentException("expected " + newDir + " to be a directory");

      HashSet<String> names1 = getFileNames(fs1.listStatus(dir));
      HashSet<String> names2 = getFileNames(fs2.listStatus(newDir));

      if (names1.equals(names2)) {
        for (String name : names1)
          if (!hash(fs1, dir, name).equals(hash(fs2, newDir, name)))
            return false;
        return true;
      }

    }
    return false;
  }

  private static HashSet<String> getFileNames(FileStatus[] filesStatuses) {
    HashSet<String> names = new HashSet<>();
    for (FileStatus fileStatus : filesStatuses)
      if (fileStatus.isDirectory())
        throw new IllegalArgumentException("expected " + fileStatus.getPath() + " to be a file");
      else
        names.add(fileStatus.getPath().getName());
    return names;
  }

  private static String hash(FileSystem fs, Path dir, String name) throws IOException {
    FSDataInputStream in = fs.open(new Path(dir, name));
    try {
      return DigestUtils.shaHex(in);
    } finally {
      in.close();
    }

  }

  private static Path getBackupName(FileSystem fs, Path path) {
    return new Path(path.getParent(), path.getName() + "_" + System.currentTimeMillis() + "_" + (rand.nextInt(Integer.MAX_VALUE) + 1) + ".bak");
  }

}

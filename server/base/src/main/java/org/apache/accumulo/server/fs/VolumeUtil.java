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

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Utility methods for managing absolute URIs contained in Accumulo metadata.
 */

public class VolumeUtil {

  private static final Logger log = Logger.getLogger(VolumeUtil.class);
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
        log.trace("Replacing " + path + " with " + replacement);
        return replacement;
      }
    }

    log.trace("Could not find replacement for " + ft + " at " + path);

    return null;
  }

  private static LogEntry switchVolumes(LogEntry le, List<Pair<Path,Path>> replacements) {
    String switchedPath = switchVolume(le.filename, FileType.WAL, replacements);
    int numSwitched = 0;
    if (switchedPath != null)
      numSwitched++;
    else
      switchedPath = le.filename;

    ArrayList<String> switchedLogs = new ArrayList<String>();
    for (String log : le.logSet) {
      String switchedLog = switchVolume(le.filename, FileType.WAL, replacements);
      if (switchedLog != null) {
        switchedLogs.add(switchedLog);
        numSwitched++;
      } else {
        switchedLogs.add(log);
      }

    }

    if (numSwitched == 0) {
      log.trace("Did not switch " + le);
      return null;
    }

    LogEntry newLogEntry = new LogEntry(le);
    newLogEntry.filename = switchedPath;
    newLogEntry.logSet = switchedLogs;

    log.trace("Switched " + le + " to " + newLogEntry);

    return newLogEntry;
  }

  public static class TabletFiles {
    public String dir;
    public List<LogEntry> logEntries;
    public SortedMap<FileRef,DataFileValue> datafiles;

    public TabletFiles() {
      logEntries = new ArrayList<LogEntry>();
      datafiles = new TreeMap<FileRef,DataFileValue>();
    }

    public TabletFiles(String dir, List<LogEntry> logEntries, SortedMap<FileRef,DataFileValue> datafiles) {
      this.dir = dir;
      this.logEntries = logEntries;
      this.datafiles = datafiles;
    }
  }

  public static Text switchRootTabletVolume(KeyExtent extent, Text location) throws IOException {
    if (extent.isRootTablet()) {
      String newLocation = switchVolume(location.toString(), FileType.TABLE, ServerConstants.getVolumeReplacements());
      if (newLocation != null) {
        MetadataTableUtil.setRootTabletDir(newLocation);
        log.info("Volume replaced " + extent + " : " + location + " -> " + newLocation);
        return new Text(new Path(newLocation).toString());
      }
    }
    return location;
  }

  /**
   * This method does two things. First, it switches any volumes a tablet is using that are configured in instance.volumes.replacements. Second, if a tablet dir
   * is no longer configured for use it chooses a new tablet directory.
   */
  public static TabletFiles updateTabletVolumes(ZooLock zooLock, VolumeManager vm, KeyExtent extent, TabletFiles tabletFiles) throws IOException {
    List<Pair<Path,Path>> replacements = ServerConstants.getVolumeReplacements();
    log.trace("Using volume replacements: " + replacements);

    List<LogEntry> logsToRemove = new ArrayList<LogEntry>();
    List<LogEntry> logsToAdd = new ArrayList<LogEntry>();

    List<FileRef> filesToRemove = new ArrayList<FileRef>();
    SortedMap<FileRef,DataFileValue> filesToAdd = new TreeMap<FileRef,DataFileValue>();

    TabletFiles ret = new TabletFiles();

    for (LogEntry logEntry : tabletFiles.logEntries) {
      LogEntry switchedLogEntry = switchVolumes(logEntry, replacements);
      if (switchedLogEntry != null) {
        logsToRemove.add(logEntry);
        logsToAdd.add(switchedLogEntry);
        ret.logEntries.add(switchedLogEntry);
        log.debug("Replacing volume " + extent + " : " + logEntry.filename + " -> " + switchedLogEntry.filename);
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
          log.debug("Replacing volume " + extent + " : " + metaPath + " -> " + switchedPath);
        } else {
          ret.datafiles.put(entry.getKey(), entry.getValue());
        }
      }
    }

    String tabletDir = tabletFiles.dir;
    String switchedDir = switchVolume(tabletDir, FileType.TABLE, replacements);

    if (switchedDir != null) {
      log.debug("Replacing volume " + extent + " : " + tabletDir + " -> " + switchedDir);
      tabletDir = switchedDir;
    }

    if (logsToRemove.size() + filesToRemove.size() > 0 || switchedDir != null)
      MetadataTableUtil.updateTabletVolumes(extent, logsToRemove, logsToAdd, filesToRemove, filesToAdd, switchedDir, zooLock, SystemCredentials.get());

    ret.dir = decommisionedTabletDir(zooLock, vm, extent, tabletDir);

    // method this should return the exact strings that are in the metadata table
    return ret;

  }

  private static String decommisionedTabletDir(ZooLock zooLock, VolumeManager vm, KeyExtent extent, String metaDir) throws IOException {
    Path dir = new Path(metaDir);
    if (isActiveVolume(dir))
      return metaDir;

    if (!dir.getParent().getParent().getName().equals(ServerConstants.TABLE_DIR)) {
      throw new IllegalArgumentException("Unexpected table dir " + dir);
    }

    Path newDir = new Path(vm.choose(ServerConstants.getBaseUris()) + Path.SEPARATOR + ServerConstants.TABLE_DIR + Path.SEPARATOR + dir.getParent().getName()
        + Path.SEPARATOR + dir.getName());

    log.info("Updating directory for " + extent + " from " + dir + " to " + newDir);
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
          log.info("renaming " + newDir + " to " + newDirBackup);
          if (!fs2.rename(newDir, newDirBackup)) {
            throw new IOException("Failed to rename " + newDir + " to " + newDirBackup);
          }
        }

        // do a lot of logging since this is the root tablet
        log.info("copying " + dir + " to " + newDir);
        if (!FileUtil.copy(fs1, dir, fs2, newDir, false, CachedConfiguration.getInstance())) {
          throw new IOException("Failed to copy " + dir + " to " + newDir);
        }

        // only set the new location in zookeeper after a successful copy
        log.info("setting root tablet location to " + newDir);
        MetadataTableUtil.setRootTabletDir(newDir.toString());

        // rename the old dir to avoid confusion when someone looks at filesystem... its ok if we fail here and this does not happen because the location in
        // zookeeper is the authority
        Path dirBackup = getBackupName(fs1, dir);
        log.info("renaming " + dir + " to " + dirBackup);
        fs1.rename(dir, dirBackup);

      } else {
        log.info("setting root tablet location to " + newDir);
        MetadataTableUtil.setRootTabletDir(newDir.toString());
      }

      return newDir.toString();
    } else {
      MetadataTableUtil.updateTabletDir(extent, newDir.toString(), SystemCredentials.get(), zooLock);
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

  @SuppressWarnings("deprecation")
  private static HashSet<String> getFileNames(FileStatus[] filesStatuses) {
    HashSet<String> names = new HashSet<String>();
    for (FileStatus fileStatus : filesStatuses)
      if (fileStatus.isDir())
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

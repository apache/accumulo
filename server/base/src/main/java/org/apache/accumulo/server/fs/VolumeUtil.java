/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.fs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for managing absolute URIs contained in Accumulo metadata.
 */
public class VolumeUtil {

  private static final Logger log = LoggerFactory.getLogger(VolumeUtil.class);

  public static String removeTrailingSlash(String path) {
    while (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }

  public static Path removeTrailingSlash(Path path) {
    String pathStr = path.toString();
    if (pathStr.endsWith("/")) {
      return new Path(removeTrailingSlash(pathStr));
    }
    return path;
  }

  public static Path switchVolume(String path, FileType ft, List<Pair<Path,Path>> replacements) {
    if (replacements.isEmpty()) {
      log.trace("Not switching volume because there are no replacements");
      return null;
    }
    Path p = new Path(path);

    // removing slash because new Path("hdfs://nn1").equals(new Path("hdfs://nn1/")) evaluates to
    // false
    Path volume = removeTrailingSlash(ft.getVolume(p));

    for (Pair<Path,Path> pair : replacements) {
      Path key = removeTrailingSlash(pair.getFirst());

      if (key.equals(volume)) {
        Path replacement = new Path(pair.getSecond(), ft.removeVolume(p));
        log.trace("Replacing {} with {}", path, replacement);
        return replacement;
      }
    }

    log.trace("Could not find replacement for {} at {}", ft, path);

    return null;
  }

  private static LogEntry switchVolumes(LogEntry le, List<Pair<Path,Path>> replacements) {
    Path switchedPath = switchVolume(le.filename, FileType.WAL, replacements);
    String switchedString;
    int numSwitched = 0;
    if (switchedPath != null) {
      switchedString = switchedPath.toString();
      numSwitched++;
    } else {
      switchedString = le.filename;
    }

    if (numSwitched == 0) {
      log.trace("Did not switch {}", le);
      return null;
    }

    LogEntry newLogEntry = le.switchFile(switchedString);

    log.trace("Switched {} to {}", le, newLogEntry);

    return newLogEntry;
  }

  public static class TabletFiles {
    public String dirName;
    public List<LogEntry> logEntries;
    public SortedMap<StoredTabletFile,DataFileValue> datafiles;

    public TabletFiles() {
      logEntries = new ArrayList<>();
      datafiles = new TreeMap<>();
    }

    public TabletFiles(String dirName, List<LogEntry> logEntries,
        SortedMap<StoredTabletFile,DataFileValue> datafiles) {
      this.dirName = dirName;
      this.logEntries = logEntries;
      this.datafiles = datafiles;
    }
  }

  /**
   * This method does two things. First, it switches any volumes a tablet is using that are
   * configured in instance.volumes.replacements. Second, if a tablet dir is no longer configured
   * for use it chooses a new tablet directory.
   */
  public static TabletFiles updateTabletVolumes(ServerContext context, ServiceLock zooLock,
      KeyExtent extent, TabletFiles tabletFiles) {
    List<Pair<Path,Path>> replacements = context.getVolumeReplacements();
    if (replacements.isEmpty()) {
      return tabletFiles;
    }
    log.trace("Using volume replacements: {}", replacements);

    List<LogEntry> logsToRemove = new ArrayList<>();
    List<LogEntry> logsToAdd = new ArrayList<>();

    List<StoredTabletFile> filesToRemove = new ArrayList<>();
    SortedMap<TabletFile,DataFileValue> filesToAdd = new TreeMap<>();

    TabletFiles ret = new TabletFiles();

    for (LogEntry logEntry : tabletFiles.logEntries) {
      LogEntry switchedLogEntry = switchVolumes(logEntry, replacements);
      if (switchedLogEntry != null) {
        logsToRemove.add(logEntry);
        logsToAdd.add(switchedLogEntry);
        ret.logEntries.add(switchedLogEntry);
        log.debug("Replacing volume {} : {} -> {}", extent, logEntry.filename,
            switchedLogEntry.filename);
      } else {
        ret.logEntries.add(logEntry);
      }
    }

    for (Entry<StoredTabletFile,DataFileValue> entry : tabletFiles.datafiles.entrySet()) {
      String metaPath = entry.getKey().getMetaUpdateDelete();
      Path switchedPath = switchVolume(metaPath, FileType.TABLE, replacements);
      if (switchedPath != null) {
        filesToRemove.add(entry.getKey());
        TabletFile switchedFile = new TabletFile(switchedPath);
        filesToAdd.put(switchedFile, entry.getValue());
        ret.datafiles.put(switchedFile.insert(), entry.getValue());
        log.debug("Replacing volume {} : {} -> {}", extent, metaPath, switchedPath);
      } else {
        ret.datafiles.put(entry.getKey(), entry.getValue());
      }
    }

    if (logsToRemove.size() + filesToRemove.size() > 0) {
      MetadataTableUtil.updateTabletVolumes(extent, logsToRemove, logsToAdd, filesToRemove,
          filesToAdd, zooLock, context);
    }

    // method this should return the exact strings that are in the metadata table
    ret.dirName = tabletFiles.dirName;
    return ret;
  }
}

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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.commons.lang3.mutable.MutableBoolean;
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
    String pathStr = requireNonNull(path).toString();
    if (pathStr.endsWith("/")) {
      return new Path(removeTrailingSlash(pathStr));
    }
    return path;
  }

  public static Path switchVolume(Path path, FileType ft, Map<Path,Path> replacements) {
    Path replacement = null;
    if (!replacements.isEmpty()) {
      // removing trailing slash for exact match comparison on the volume itself
      Path volume = removeTrailingSlash(ft.getVolume(requireNonNull(path)));
      replacement = replacements.entrySet().stream()
          .filter(entry -> removeTrailingSlash(entry.getKey()).equals(volume))
          .map(entry -> new Path(entry.getValue(), requireNonNull(ft.removeVolume(path))))
          .findFirst().orElse(null);
    }
    if (replacement != null) {
      log.trace("Replacing {} with {} for {}", path, replacement, ft);
      return replacement;
    }
    log.trace("No replacement available for {} at {}", ft, path);
    return null;
  }

  public static LogEntry switchVolume(LogEntry le, Map<Path,Path> replacements) {
    Path switchedPath = switchVolume(new Path(le.getPath()), FileType.WAL, replacements);
    return switchedPath == null ? null : LogEntry.fromPath(switchedPath.toString());
  }

  public static class TabletFiles {
    public String dirName;
    public final List<LogEntry> logEntries;
    public final SortedMap<StoredTabletFile,DataFileValue> datafiles;

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

  public static boolean needsVolumeReplacement(final Map<Path,Path> replacements,
      final TabletMetadata tm) {
    if (replacements.isEmpty()) {
      return false;
    }

    MutableBoolean needsReplacement = new MutableBoolean(false);

    Consumer<LogEntry> consumer = le -> needsReplacement.setTrue();

    volumeReplacementEvaluation(replacements, tm, consumer, consumer,
        f -> needsReplacement.setTrue(), (f, dfv) -> needsReplacement.setTrue());

    return needsReplacement.booleanValue();
  }

  public static class VolumeReplacements {
    public final TabletMetadata tabletMeta;
    public final List<LogEntry> logsToRemove = new ArrayList<>();
    public final List<LogEntry> logsToAdd = new ArrayList<>();
    public final List<StoredTabletFile> filesToRemove = new ArrayList<>();
    public final Map<ReferencedTabletFile,DataFileValue> filesToAdd = new HashMap<>();

    public VolumeReplacements(TabletMetadata tabletMeta) {
      this.tabletMeta = tabletMeta;
    }
  }

  public static VolumeReplacements computeVolumeReplacements(final Map<Path,Path> replacements,
      final TabletMetadata tm) {
    var vr = new VolumeReplacements(tm);
    volumeReplacementEvaluation(replacements, tm, vr.logsToRemove::add, vr.logsToAdd::add,
        vr.filesToRemove::add, vr.filesToAdd::put);
    return vr;
  }

  public static void volumeReplacementEvaluation(final Map<Path,Path> replacements,
      final TabletMetadata tm, final Consumer<LogEntry> logsToRemove,
      final Consumer<LogEntry> logsToAdd, final Consumer<StoredTabletFile> filesToRemove,
      final BiConsumer<ReferencedTabletFile,DataFileValue> filesToAdd) {
    if (replacements.isEmpty() || (tm.getFilesMap().isEmpty() && tm.getLogs().isEmpty())) {
      return;
    }

    log.trace("Using volume replacements: {}", replacements);
    for (LogEntry logEntry : tm.getLogs()) {
      log.trace("Evaluating walog {} for replacement.", logEntry);
      LogEntry switchedLogEntry = switchVolume(logEntry, replacements);
      if (switchedLogEntry != null) {
        logsToRemove.accept(logEntry);
        logsToAdd.accept(switchedLogEntry);
        log.trace("Replacing volume {} : {} -> {}", tm.getExtent(), logEntry.getPath(),
            switchedLogEntry.getPath());
      }
    }

    for (Entry<StoredTabletFile,DataFileValue> entry : tm.getFilesMap().entrySet()) {
      log.trace("Evaluating file {} for replacement.", entry.getKey().getPath());
      String metaPath = entry.getKey().getMetadata();
      Path switchedPath = switchVolume(entry.getKey().getPath(), FileType.TABLE, replacements);
      if (switchedPath != null) {
        filesToRemove.accept(entry.getKey());
        ReferencedTabletFile switchedFile =
            new ReferencedTabletFile(switchedPath, entry.getKey().getRange());
        filesToAdd.accept(switchedFile, entry.getValue());
        log.trace("Replacing volume {} : {} -> {}", tm.getExtent(), metaPath, switchedPath);
      }
    }
  }
}

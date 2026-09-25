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
package org.apache.accumulo.tserver.log;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.server.manager.recovery.RecoveryPath;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Write ahead logs have two paths in DFS. There is the path of the original unsorted walog and the
 * path of the sorted walog. The purpose of this class is to convert the unsorted wal path to a
 * sorted wal path and validate the sorted dir exists and is finished.
 */
public class ResolvedSortedLog {

  private final SortedSet<Path> children;
  private final LogEntry origin;
  private final Path sortedLogDir;

  private ResolvedSortedLog(LogEntry origin, Path sortedLogDir, SortedSet<Path> children) {
    this.origin = origin;
    this.sortedLogDir = sortedLogDir;
    this.children = Collections.unmodifiableSortedSet(children);
  }

  /**
   * @return the unsorted walog path from which this was created.
   */
  public LogEntry getOrigin() {
    return origin;
  }

  /**
   * @return the path of the directory in which sorted logs are stored
   */
  public Path getDir() {
    return sortedLogDir;
  }

  /**
   * @return When an unsorted walog is sorted the sorted data is stored in one or more rfiles, this
   *         returns the paths of those rfiles.
   */
  public SortedSet<Path> getChildren() {
    return children;
  }

  @Override
  public String toString() {
    return sortedLogDir.toString();
  }

  /**
   * For a given path of an unsorted walog check to see if the corresponding sorted log dir exists
   * and is finished. If it is return an immutable object containing information about the sorted
   * walogs.
   */
  public static ResolvedSortedLog resolve(LogEntry logEntry, VolumeManager fs) throws IOException {

    // convert the path of an unsorted log to the expected path for the corresponding sorted log
    // dir
    Path sortedLogPath = RecoveryPath.getRecoveryPath(new Path(logEntry.filename));

    boolean foundFinish = false;
    // Path::getName compares the last component of each Path value. In this case, the last
    // component should
    // always have the format 'part-r-XXXXX.rf', where XXXXX are one-up values.
    SortedSet<Path> logFiles = new TreeSet<>(Comparator.comparing(Path::getName));
    for (FileStatus child : fs.listStatus(sortedLogPath)) {
      if (child.getPath().getName().startsWith("_")) {
        continue;
      }
      if (SortedLogState.isFinished(child.getPath().getName())) {
        foundFinish = true;
        continue;
      }
      if (SortedLogState.FAILED.getMarker().equals(child.getPath().getName())) {
        continue;
      }
      FileSystem ns = fs.getFileSystemByPath(child.getPath());
      Path fullLogPath = ns.makeQualified(child.getPath());
      logFiles.add(fullLogPath);
    }
    if (!foundFinish) {
      throw new IOException("Sort '" + SortedLogState.FINISHED.getMarker() + "' flag not found in "
          + sortedLogPath + " for walog " + logEntry.filename);
    }

    return new ResolvedSortedLog(logEntry, sortedLogPath, logFiles);
  }

  /**
   * Create a ResolvedSortedLog directly from a sorted log directory path. This is useful for
   * diagnostic tools that operate directly on sorted recovery logs without going through the normal
   * recovery flow with LogEntry objects.
   */
  public static ResolvedSortedLog fromSortedLogDir(Path sortedLogDir, VolumeManager fs)
      throws IOException {
    boolean foundFinish = false;
    SortedSet<Path> logFiles = new TreeSet<>(Comparator.comparing(Path::getName));
    for (FileStatus child : fs.listStatus(sortedLogDir)) {
      if (child.getPath().getName().startsWith("_")) {
        continue;
      }
      if (SortedLogState.isFinished(child.getPath().getName())) {
        foundFinish = true;
        continue;
      }
      if (SortedLogState.FAILED.getMarker().equals(child.getPath().getName())) {
        continue;
      }
      FileSystem ns = fs.getFileSystemByPath(child.getPath());
      Path fullLogPath = ns.makeQualified(child.getPath());
      logFiles.add(fullLogPath);
    }
    if (!foundFinish) {
      throw new IOException(
          "Sort '" + SortedLogState.FINISHED.getMarker() + "' flag not found in " + sortedLogDir);
    }

    // Create a dummy LogEntry for the origin (used only for diagnostics)
    LogEntry dummyOrigin = new LogEntry(null, 0, sortedLogDir.toString());
    return new ResolvedSortedLog(dummyOrigin, sortedLogDir, logFiles);
  }
}

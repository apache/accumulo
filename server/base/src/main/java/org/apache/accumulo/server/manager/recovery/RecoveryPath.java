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
package org.apache.accumulo.server.manager.recovery;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.hadoop.fs.Path;

import com.google.common.hash.Hashing;

public class RecoveryPath {

  /**
   * Given a legacy path-based WAL entry, transform it to a recovery path.
   */
  public static Path getRecoveryPath(LogEntry logEntry) {
    if (!logEntry.isLegacyPathReference()) {
      throw new IllegalArgumentException(
          "Recovery volumes are required for an opaque WAL reference");
    }
    return getLegacyRecoveryPath(logEntry);
  }

  /**
   * Derive the recovery path. New opaque references use rendezvous hashing to distribute recovery
   * logs across the configured recovery directories. Legacy path-based references continue using
   * the recovery directory associated with their original WAL path.
   */
  public static Path getRecoveryPath(LogEntry logEntry, Collection<String> recoveryDirectories) {
    if (logEntry.isLegacyPathReference()) {
      return getLegacyRecoveryPath(logEntry);
    }
    Path recoveryDirectory = chooseRecoveryVolume(logEntry.getUniqueID(), recoveryDirectories);
    return new Path(recoveryDirectory, logEntry.getUniqueID().toString());
  }

  private static Path getLegacyRecoveryPath(LogEntry logEntry) {
    Path walPath = logEntry.getPath();
    if (walPath.toUri().getScheme() == null || !walPath.isAbsolute()
        || !walPath.getName().equals(logEntry.getUniqueID().toString())) {
      throw new IllegalArgumentException("Bad WAL path " + walPath);
    }

    Path parent = walPath.getParent();
    if (parent == null) {
      throw new IllegalArgumentException("Bad WAL path " + walPath);
    }

    Path walDir;
    if (parent.getName().equals(FileType.WAL.getDirectory())) {
      // New paths are <volume>/wal/<uuid>.
      walDir = parent;
    } else {
      // Legacy paths are <volume>/wal/<tserver>/<uuid>. The tserver component is not needed
      // to determine the recovery path.
      walDir = parent.getParent();
      if (walDir == null || !walDir.getName().equals(FileType.WAL.getDirectory())) {
        throw new IllegalArgumentException("Bad WAL path " + walPath);
      }
    }

    Path volume = walDir.getParent();
    if (volume == null) {
      throw new IllegalArgumentException("Bad WAL path " + walPath);
    }
    return new Path(new Path(volume, FileType.RECOVERY.getDirectory()),
        logEntry.getUniqueID().toString());
  }

  /**
   * Selects the recovery directory with the highest stable rendezvous hash score for this WAL.
   *
   * <p>
   * Sorting the normalized directory strings makes the choice independent of collection iteration
   * order. The UUID makes assignments appear random and distribute WALs across configured volumes.
   */
  public static Path chooseRecoveryVolume(UUID walId, Collection<String> recoveryDirectories) {
    Set<String> uniqueDirectories = new HashSet<>();
    for (String directory : recoveryDirectories) {
      uniqueDirectories.add(new Path(directory).toString());
    }
    if (uniqueDirectories.isEmpty()) {
      throw new IllegalArgumentException("No recovery directories are configured");
    }

    List<String> directories = new ArrayList<>(uniqueDirectories);
    directories.sort(Comparator.naturalOrder());

    // TODO: Replace rendezvous hashing with a configurable volume chooser for WAL sorting.
    String selectedDirectory = null;
    long selectedScore = 0;
    for (String directory : directories) {
      long score =
          Hashing.murmur3_128().hashString(walId.toString() + "\u0000" + directory, UTF_8).asLong();
      if (selectedDirectory == null || Long.compareUnsigned(score, selectedScore) > 0) {
        selectedDirectory = directory;
        selectedScore = score;
      }
    }
    return new Path(selectedDirectory);
  }

}

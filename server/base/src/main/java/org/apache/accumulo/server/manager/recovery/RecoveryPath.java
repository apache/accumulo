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

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.hadoop.fs.Path;

public class RecoveryPath {
  private static int getPathStart(String walPath) {
    final String schemeTest = "://";

    if (walPath == null || walPath.isEmpty()) {
      throw new IllegalArgumentException("Bad path " + walPath);
    }

    // Test for scheme
    int schemeEnd = walPath.indexOf(schemeTest);
    if (schemeEnd < 0) {
      throw new IllegalArgumentException("Bad path " + walPath + " No scheme");
    }

    // Find the start of the path
    int pathStart = walPath.indexOf("/", schemeEnd + schemeTest.length());
    if (pathStart < 0) {
      // Empty path after authority
      throw new IllegalArgumentException("Bad path " + walPath + " No content");
    }
    return pathStart;
  }

  public static String transformToRecoveryPath(String walPath) {
    int pathStart = getPathStart(walPath);
    String pathPortion = walPath.substring(pathStart);

    // This replaces the need for Path.getParent calls
    String[] segments = pathPortion.split("/");
    // Remove any spaces
    List<String> parts = new ArrayList<>();
    for (String s : segments) {
      if (!s.isEmpty()) {
        parts.add(s);
      }
    }

    // checks for minimum correct depth
    if (parts.size() < 3) {
      throw new IllegalArgumentException("Bad path due to length" + walPath);
    }

    int validWalLength = 2;

    String uuid = parts.get(parts.size() - 1);
    String serverComponent = parts.get(parts.size() - validWalLength);

    // Support recovering 1.4 WALs that don't have a server component
    if (!serverComponent.equals(FileType.WAL.getDirectory())) {
      validWalLength = 3;
    }
    String walDir = parts.get(parts.size() - validWalLength);

    if (!walDir.equals(FileType.WAL.getDirectory())) {
      throw new IllegalArgumentException(
          "Bad path " + walPath + " (missing wal directory component)");
    }

    String authority = walPath.substring(0, pathStart);
    // Handle file scheme like Hadoop Path and drop the slashes
    // This converts file:///<path> to file:/<path>
    if (authority.equals("file://")) {
      authority = "file:";
    }

    List<String> baseParts = parts.subList(0, parts.size() - validWalLength);
    StringBuilder recoveryPath = new StringBuilder(authority);
    for (String part : baseParts) {
      recoveryPath.append("/").append(part);
    }
    recoveryPath.append("/").append(FileType.RECOVERY.getDirectory());
    recoveryPath.append("/").append(uuid);

    return recoveryPath.toString();
  }

  // given a wal path, transform it to a recovery path
  public static Path getRecoveryPath(Path walPath) {
    if (walPath.depth() >= 3 && walPath.toUri().getScheme() != null) {
      // it's a fully qualified path
      String uuid = walPath.getName();
      // drop uuid
      walPath = walPath.getParent();
      // recovered 1.4 WALs won't have a server component
      if (!walPath.getName().equals(FileType.WAL.getDirectory())) {
        // drop server
        walPath = walPath.getParent();
      }

      if (!walPath.getName().equals(FileType.WAL.getDirectory())) {
        throw new IllegalArgumentException(
            "Bad path " + walPath + "missing wal directory component");
      }

      // drop wal
      walPath = walPath.getParent();

      walPath = new Path(walPath, FileType.RECOVERY.getDirectory());
      walPath = new Path(walPath, uuid);

      return walPath;
    }

    throw new IllegalArgumentException("Bad path " + walPath);

  }

}

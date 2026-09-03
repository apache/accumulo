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

  static String transformToRecoveryPath(String walPath) {
    int pathStart = getPathStart(walPath);

    // This replaces the need for Path.getParent calls
    String[] segments = walPath.substring(pathStart).split("/");
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

    String uuid = parts.get(parts.size() - 1);
    String serverComponent = parts.get(parts.size() - 2);
    String walDir = parts.get(parts.size() - 3);

    if (serverComponent.equals(FileType.WAL.getDirectory())) {
      throw new IllegalArgumentException("Bad path " + walPath + " (missing server component)");
    }

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

    List<String> baseParts = parts.subList(0, parts.size() - 3);
    StringBuilder recoveryPath = new StringBuilder(authority);
    for (String part : baseParts) {
      recoveryPath.append("/").append(part);
    }
    recoveryPath.append("/").append(FileType.RECOVERY.getDirectory());
    recoveryPath.append("/").append(uuid);

    return recoveryPath.toString();
  }

  public static String getRecoveryPath(String walPath) {
    return transformToRecoveryPath(walPath);
  }

}

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

import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.hadoop.fs.Path;

public class RecoveryPath {

  // given a wal path, transform it to a recovery path
  public static Path getRecoveryPath(Path walPath) {
    if (walPath.depth() >= 3 && walPath.toUri().getScheme() != null) {
      // its a fully qualified path
      String uuid = walPath.getName();
      // drop uuid
      walPath = walPath.getParent();

      // expect and drop the server component
      if (walPath.getName().equals(FileType.WAL.getDirectory())) {
        throw new IllegalArgumentException("Bath path " + walPath + " (missing server component)");
      }
      walPath = walPath.getParent();

      // expect and drop the wal component
      if (!walPath.getName().equals(FileType.WAL.getDirectory())) {
        throw new IllegalArgumentException(
            "Bad path " + walPath + " (missing wal directory component)");
      }
      walPath = walPath.getParent();

      // create new path in recovery directory that is a sibling to the wal directory (same volume),
      // without the server component
      walPath = new Path(walPath, FileType.RECOVERY.getDirectory());
      walPath = new Path(walPath, uuid);

      return walPath;
    }

    throw new IllegalArgumentException("Bad path " + walPath);
  }

}

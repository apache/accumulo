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
package org.apache.accumulo.server.master.recovery;

import java.io.IOException;

import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.hadoop.fs.Path;

/**
 *
 */
public class RecoveryPath {

  // given a wal path, transform it to a recovery path
  public static Path getRecoveryPath(VolumeManager fs, Path walPath) throws IOException {
    if (walPath.depth() >= 3 && walPath.toUri().getScheme() != null) {
      // its a fully qualified path
      String uuid = walPath.getName();
      // drop uuid
      walPath = walPath.getParent();
      // recovered 1.4 WALs won't have a server component
      if (!walPath.getName().equals(FileType.WAL.getDirectory())) {
        // drop server
        walPath = walPath.getParent();
      }

      if (!walPath.getName().equals(FileType.WAL.getDirectory()))
        throw new IllegalArgumentException("Bad path " + walPath);

      // drop wal
      walPath = walPath.getParent();

      walPath = new Path(walPath, FileType.RECOVERY.getDirectory());
      walPath = new Path(walPath, uuid);

      return walPath;
    }

    throw new IllegalArgumentException("Bad path " + walPath);

  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.metadata.schema;

import org.apache.hadoop.fs.Path;

public class TabletFileUtil {

  public static Path getVolumeFromFullPath(Path path, String dir) {
    String pathString = path.toString();

    int eopi = endOfVolumeIndex(pathString, dir);
    if (eopi != -1)
      return new Path(pathString.substring(0, eopi + 1));

    return null;
  }

  public static Path removeVolumeFromFullPath(Path path, String dir) {
    String pathString = path.toString();

    int eopi = endOfVolumeIndex(pathString, dir);
    if (eopi != -1)
      return new Path(pathString.substring(eopi + 1));

    return null;
  }

  private static int endOfVolumeIndex(String path, String dir) {
    // Strip off the suffix that starts with the FileType (e.g. tables, wal, etc)
    int dirIndex = path.indexOf('/' + dir);
    if (dirIndex != -1) {
      return dirIndex;
    }

    if (path.contains(":"))
      throw new IllegalArgumentException(path + " is absolute, but does not contain " + dir);
    return -1;
  }
}

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
package org.apache.accumulo.core.metadata;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UnassignedTabletFile extends AbstractTabletFile<UnassignedTabletFile> {

  public UnassignedTabletFile(FileSystem fs, Path path) {
    super(Objects.requireNonNull(fs).makeQualified(Objects.requireNonNull(path)));
  }

  @Override
  public int compareTo(UnassignedTabletFile o) {
    if (equals(o)) {
      return 0;
    } else {
      return path.compareTo(o.path);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof UnassignedTabletFile) {
      UnassignedTabletFile that = (UnassignedTabletFile) obj;
      return path.equals(that.path);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return path.hashCode();
  }

  @Override
  public String toString() {
    return path.toString();
  }

  public static UnassignedTabletFile of(FileSystem fs, File file) {
    return new UnassignedTabletFile(fs, new Path(Objects.requireNonNull(file).toString()));
  }

  public static UnassignedTabletFile of(FileSystem fs, Path path) {
    return new UnassignedTabletFile(fs, path);
  }

  public static UnassignedTabletFile of(Configuration conf, Path path) throws IOException {
    return new UnassignedTabletFile(Objects.requireNonNull(path).getFileSystem(conf), path);
  }

}

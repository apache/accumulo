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

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A file that is not intended to be added to a tablet as a reference, within the scope of the code
 * using this class, but needs to be passed to code that processes tablet files. These files could
 * be temp files or files directly created by a user for bulk import. The file may ultimately be
 * added to a tablet later as a new file reference, but within a different scope (process, thread,
 * code block, method, etc.) that uses a different class to represent the file in that scope.
 *
 * Unlike {@link TabletFile}, this class does not perform any validation or normalization on the
 * provided path.
 *
 * @since 3.0.0
 */
public class UnreferencedTabletFile extends AbstractTabletFile<UnreferencedTabletFile> {

  public UnreferencedTabletFile(FileSystem fs, Path path) {
    this(fs, path, new Range());
  }

  public UnreferencedTabletFile(FileSystem fs, Path path, Range range) {
    super(Objects.requireNonNull(fs).makeQualified(Objects.requireNonNull(path)), range);
  }

  @Override
  public int compareTo(UnreferencedTabletFile o) {
    if (equals(o)) {
      return 0;
    } else {
      return path.compareTo(o.path);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof UnreferencedTabletFile) {
      UnreferencedTabletFile that = (UnreferencedTabletFile) obj;
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

  public static UnreferencedTabletFile of(FileSystem fs, File file) {
    return new UnreferencedTabletFile(fs, new Path(Objects.requireNonNull(file).toString()));
  }

  public static UnreferencedTabletFile ofRanged(FileSystem fs, File file, Range range) {
    return new UnreferencedTabletFile(fs, new Path(Objects.requireNonNull(file).toString()), range);
  }

  public static UnreferencedTabletFile of(FileSystem fs, Path path) {
    return new UnreferencedTabletFile(fs, path);
  }

  public static UnreferencedTabletFile ofRanged(FileSystem fs, Path path, Range range) {
    return new UnreferencedTabletFile(fs, path, range);
  }

  public static UnreferencedTabletFile of(Configuration conf, Path path) throws IOException {
    return new UnreferencedTabletFile(Objects.requireNonNull(path).getFileSystem(conf), path);
  }

}

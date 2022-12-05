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
package org.apache.accumulo.core.volume;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Encapsulates a {@link FileSystem} and a base {@link Path} within that filesystem. This also avoid
 * the necessity to pass around a Configuration.
 */
public interface Volume {

  /**
   * A {@link FileSystem} that Accumulo will use
   */
  FileSystem getFileSystem();

  /**
   * The base path which Accumulo will use within the given {@link FileSystem}
   */
  String getBasePath();

  /**
   * Convert the given child path into a Path that is relative to the base path for this Volume. The
   * supplied path should not include any scheme (such as <code>file:</code> or <code>hdfs:</code>),
   * and should not contain any relative path "breakout" patterns, such as <code>../</code>. If the
   * path begins with a single slash, it will be preserved while prefixing this volume. If it does
   * not begin with a single slash, one will be inserted.
   *
   * @param pathString The suffix to use
   * @return A Path for this Volume with the provided suffix
   */
  Path prefixChild(String pathString);

  /**
   * Determine if the Path is contained in Volume. A Path is considered contained if refers to a
   * location within the base path for this Volume on the same FileSystem. It can be located at the
   * base path, or within any sub-directory. Unqualified paths (those without a file system scheme)
   * will resolve to using the configured Hadoop default file system before comparison. Paths are
   * not considered "contained" within this Volume if they have any relative path "breakout"
   * patterns, such as <code>../</code>.
   */
  boolean containsPath(Path path);
}

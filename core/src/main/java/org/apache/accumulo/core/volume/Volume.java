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
package org.apache.accumulo.core.volume;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Encapsulates a {@link FileSystem} and a base {@link Path} within that filesystem. This also avoid the necessity to pass around a Configuration.
 */
public interface Volume {

  /**
   * A {@link FileSystem} that Accumulo will use
   */
  public FileSystem getFileSystem();

  /**
   * The base path which Accumulo will use within the given {@link FileSystem}
   */
  public String getBasePath();

  /**
   * Convert the given Path into a Path that is relative to the base path for this Volume
   *
   * @param p
   *          The suffix to use
   * @return A Path for this Volume with the provided suffix
   */
  public Path prefixChild(Path p);

  /**
   * Convert the given child path into a Path that is relative to the base path for this Volume
   *
   * @param p
   *          The suffix to use
   * @return A Path for this Volume with the provided suffix
   */
  public Path prefixChild(String p);

  /**
   * Determine if the Path is valid on this Volume. A Path is valid if it is contained in the Volume's FileSystem and is rooted beneath the basePath
   */
  public boolean isValidPath(Path p);
}

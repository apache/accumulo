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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic Volume implementation that contains a FileSystem and a base path that should be used within
 * that filesystem.
 */
public class VolumeImpl implements Volume {

  private static final Logger log = LoggerFactory.getLogger(VolumeImpl.class);

  private final FileSystem fs;
  private final String basePath;
  private final Configuration hadoopConf;

  public VolumeImpl(Path path, Configuration hadoopConf) throws IOException {
    this.fs = requireNonNull(path).getFileSystem(requireNonNull(hadoopConf));
    this.basePath = stripTrailingSlashes(path.toUri().getPath());
    this.hadoopConf = hadoopConf;
  }

  public VolumeImpl(FileSystem fs, String basePath) {
    this.fs = requireNonNull(fs);
    this.basePath = stripTrailingSlashes(requireNonNull(basePath));
    this.hadoopConf = fs.getConf();
  }

  // remove any trailing whitespace or slashes
  private static String stripTrailingSlashes(String path) {
    return path.strip().replaceAll("/*$", "");
  }

  @Override
  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public String getBasePath() {
    return basePath;
  }

  @Override
  public boolean containsPath(Path path) {
    FileSystem otherFS;
    try {
      otherFS = requireNonNull(path).getFileSystem(hadoopConf);
    } catch (IOException e) {
      log.warn("Could not determine filesystem from path: {}", path, e);
      return false;
    }
    return equivalentFileSystems(otherFS) && isAncestorPathOf(path);
  }

  // same if the only difference is trailing slashes
  boolean equivalentFileSystems(FileSystem otherFS) {
    return stripTrailingSlashes(fs.getUri().toString())
        .equals(stripTrailingSlashes(otherFS.getUri().toString()));
  }

  // is ancestor if the path portion without the filesystem scheme
  // is a subdirectory of this volume's basePath
  boolean isAncestorPathOf(Path other) {
    String otherPath = other.toUri().getPath().strip();
    if (otherPath.startsWith(basePath)) {
      String otherRemainingPath = otherPath.substring(basePath.length());
      return otherRemainingPath.isEmpty()
          || (otherRemainingPath.startsWith("/") && !otherRemainingPath.contains(".."));
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getFileSystem()) + Objects.hashCode(getBasePath());
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof VolumeImpl) {
      VolumeImpl other = (VolumeImpl) o;
      return getFileSystem().equals(other.getFileSystem())
          && getBasePath().equals(other.getBasePath());
    }

    return false;
  }

  @Override
  public String toString() {
    return fs.makeQualified(new Path(basePath)).toString();
  }

  @Override
  public Path prefixChild(String pathString) {
    String p = requireNonNull(pathString).strip();
    p = p.startsWith("/") ? p.substring(1) : p;
    String reason;
    if (basePath.isBlank()) {
      log.error("Basepath is empty. Make sure instance.volumes is set to a correct path");
      throw new IllegalArgumentException(
          "Accumulo cannot be initialized because basepath is empty. "
              + "This probably means instance.volumes is an incorrect value");
    }

    if (p.isBlank()) {
      return fs.makeQualified(new Path(basePath));
    } else if (p.startsWith("/")) {
      // check for starting with '//'
      reason = "absolute path";
    } else if (pathString.contains(":")) {
      reason = "qualified path";
    } else if (pathString.contains("..")) {
      reason = "path contains '..'";
    } else {
      return fs.makeQualified(new Path(basePath, p));
    }
    throw new IllegalArgumentException(
        String.format("Cannot prefix %s (%s) with volume %s", pathString, reason, this));
  }

}

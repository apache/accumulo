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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Objects;

import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic Volume implementation that contains a FileSystem and a base path that should be used within that filesystem.
 */
public class VolumeImpl implements Volume {
  private static final Logger log = LoggerFactory.getLogger(VolumeImpl.class);

  protected final FileSystem fs;
  protected final String basePath;

  public VolumeImpl(Path path, Configuration conf) throws IOException {
    requireNonNull(path);
    requireNonNull(conf);

    this.fs = path.getFileSystem(conf);
    this.basePath = path.toUri().getPath();
  }

  public VolumeImpl(FileSystem fs, String basePath) {
    requireNonNull(fs);
    requireNonNull(basePath);

    this.fs = fs;
    this.basePath = basePath;
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
  public Path prefixChild(Path p) {
    return fs.makeQualified(new Path(basePath, p));
  }

  @Override
  public boolean isValidPath(Path p) {
    requireNonNull(p);

    FileSystem other;
    try {
      other = p.getFileSystem(CachedConfiguration.getInstance());
    } catch (IOException e) {
      log.warn("Could not determine filesystem from path: {}", p, e);
      return false;
    }

    if (equivalentFileSystems(other)) {
      return equivalentPaths(p);
    }

    return false;
  }

  /**
   * Test whether the provided {@link FileSystem} object reference the same actual filesystem as the member <code>fs</code>.
   *
   * @param other
   *          The filesystem to compare
   */
  boolean equivalentFileSystems(FileSystem other) {
    return fs.getUri().equals(other.getUri());
  }

  /**
   * Tests if the provided {@link Path} is rooted inside this volume, contained within <code>basePath</code>.
   *
   * @param other
   *          The path to compare
   */
  boolean equivalentPaths(Path other) {
    return other.toUri().getPath().startsWith(basePath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getFileSystem()) + Objects.hashCode(getBasePath());
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof VolumeImpl) {
      VolumeImpl other = (VolumeImpl) o;
      return getFileSystem().equals(other.getFileSystem()) && getBasePath().equals(other.getBasePath());
    }

    return false;
  }

  @Override
  public String toString() {
    return getFileSystem() + " " + basePath;
  }

  @Override
  public Path prefixChild(String p) {
    return prefixChild(new Path(basePath, p));
  }

}

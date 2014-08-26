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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Basic Volume implementation that contains a FileSystem and a base path
 * that should be used within that filesystem.
 */
public class VolumeImpl implements Volume {
  private static final Logger log = Logger.getLogger(VolumeImpl.class);

  protected final FileSystem fs;
  protected final String basePath;

  public VolumeImpl(Path path, Configuration conf) throws IOException {
    checkNotNull(path);
    checkNotNull(conf);

    this.fs = path.getFileSystem(conf);
    this.basePath = path.toUri().getPath();
  }

  public VolumeImpl(FileSystem fs, String basePath) {
    checkNotNull(fs);
    checkNotNull(basePath);

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
    checkNotNull(p);

    try {
      if (fs.equals(p.getFileSystem(CachedConfiguration.getInstance()))) {
        return p.toUri().getPath().startsWith(basePath);
      }
    } catch (IOException e) {
      log.warn("Could not determine filesystem from path: " + p);
    }

    return false;
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

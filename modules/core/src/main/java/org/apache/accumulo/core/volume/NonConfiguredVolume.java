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

import org.apache.accumulo.core.conf.Property;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Volume implementation which represents a Volume for which we have a FileSystem but no base path because it is not configured via
 * {@link Property#INSTANCE_VOLUMES}
 *
 * This is useful to handle volumes that have been removed from accumulo-site.xml but references to these volumes have not been updated. This Volume should
 * never be used to create new files, only to read existing files.
 */
public class NonConfiguredVolume implements Volume {
  private FileSystem fs;

  public NonConfiguredVolume(FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public String getBasePath() {
    throw new UnsupportedOperationException("No base path known because this Volume isn't configured in accumulo-site.xml");
  }

  @Override
  public Path prefixChild(Path p) {
    throw new UnsupportedOperationException("Cannot prefix path because this Volume isn't configured in accumulo-site.xml");
  }

  @Override
  public Path prefixChild(String p) {
    throw new UnsupportedOperationException("Cannot prefix path because this Volume isn't configured in accumulo-site.xml");
  }

  @Override
  public boolean isValidPath(Path p) {
    throw new UnsupportedOperationException("Cannot determine if path is valid because this Volume isn't configured in accumulo-site.xml");
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof NonConfiguredVolume) {
      NonConfiguredVolume other = (NonConfiguredVolume) o;
      return this.fs.equals(other.getFileSystem());
    }
    return false;
  }

  @Override
  public String toString() {
    return "NonConfiguredVolume: " + this.fs.toString();
  }

  @Override
  public int hashCode() {
    return NonConfiguredVolume.class.hashCode() ^ this.fs.hashCode();
  }
}

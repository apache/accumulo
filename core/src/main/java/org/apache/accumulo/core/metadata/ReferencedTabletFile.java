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

import java.util.Comparator;
import java.util.Objects;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Object representing a tablet file that may exist in the metadata table. This class is used for
 * reading and opening tablet files. It is also used when inserting new tablet files. When a new
 * file is inserted, the {@link #insert()} method is called and returns a {@link StoredTabletFile}
 * For situations where a tablet file needs to be updated or deleted in the metadata, a
 * {@link StoredTabletFile} is required.
 * <p>
 * As of 2.1, Tablet file paths should now be only absolute URIs with the removal of relative paths
 * in Upgrader9to10.upgradeRelativePaths()
 */
@SuppressFBWarnings(value = "CT_CONSTRUCTOR_THROW",
    justification = "Constructor validation is required for proper initialization")
public class ReferencedTabletFile extends AbstractTabletFile<ReferencedTabletFile> {

  private static final Logger log = LoggerFactory.getLogger(ReferencedTabletFile.class);

  private static final Comparator<ReferencedTabletFile> comparator =
      Comparator.comparing(ReferencedTabletFile::getNormalizedPathStr)
          .thenComparing(ReferencedTabletFile::getRange);

  public ReferencedTabletFile(Path metaPath) {
    this(metaPath, new Range());
  }

  /**
   * Construct new tablet file using a Path. Used in the case where we had to use Path object to
   * qualify an absolute path or create a new file.
   */
  public ReferencedTabletFile(Path metaPath, Range range) {
    super(validatePath(metaPath), range);
    log.trace("Creating tablet file reference from {}", metaPath);
  }

  private static Path validatePath(Path metaPath) {
    Path path = ValidationUtil.validate(Objects.requireNonNull(metaPath));
    Preconditions.checkArgument(!path.getName().isEmpty(), "Path must identify a file: %s", path);
    return path;
  }

  @Override
  public String getFileName() {
    return path.getName();
  }

  /**
   * Return a string for opening and reading the tablet file. Doesn't have to be exact string in
   * metadata.
   */
  public String getNormalizedPathStr() {
    return path.toString();
  }

  /**
   * New file was written to metadata so return a StoredTabletFile
   */
  public StoredTabletFile insert() {
    return StoredTabletFile.of(getPath(), getRange());
  }

  @Override
  public int compareTo(ReferencedTabletFile o) {
    return comparator.compare(this, o);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ReferencedTabletFile that) {
      return path.equals(that.path) && range.equals(that.range);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, range);
  }

  @Override
  public String toString() {
    return path.toString();
  }

  public static ReferencedTabletFile of(final Path path) {
    return new ReferencedTabletFile(path);
  }

  public static ReferencedTabletFile of(final Path path, Range range) {
    return new ReferencedTabletFile(path, range);
  }

}

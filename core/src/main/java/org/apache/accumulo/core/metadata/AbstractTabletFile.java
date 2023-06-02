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

import java.util.Objects;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.fs.Path;

/**
 * A base class used to represent file references that are handled by code that processes tablet
 * files.
 *
 * @since 3.0.0
 */
public abstract class AbstractTabletFile<T extends AbstractTabletFile<T>> implements Comparable<T> {

  private final String fileName; // C0004.rf
  protected final Path path;
  protected final Range range;

  protected AbstractTabletFile(Path path, Range range) {
    this.path = Objects.requireNonNull(path);
    this.fileName = path.getName();
    this.range = Objects.requireNonNull(range);
    ValidationUtil.validateFileName(fileName);
  }

  /**
   * @return The file name of the TabletFile
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * @return The path of the TabletFile
   */
  public Path getPath() {
    return path;
  }

  /**
   * @return The range of the TabletFile
   *
   */
  public Range getRange() {
    return range;
  }

  /**
   * @return True if this file is fenced by a range
   *
   */
  public boolean hasRange() {
    return !range.isInfiniteStartKey() || !range.isInfiniteStopKey();
  }
}

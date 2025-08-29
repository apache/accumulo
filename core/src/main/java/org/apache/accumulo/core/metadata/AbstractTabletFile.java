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

import static org.apache.accumulo.core.util.RowRangeUtil.requireKeyExtentDataRange;
import static org.apache.accumulo.core.util.RowRangeUtil.stripZeroTail;

import java.util.Objects;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.fs.Path;

/**
 * A base class used to represent file references that are handled by code that processes tablet
 * files.
 *
 * @since 3.0.0
 */
public abstract class AbstractTabletFile<T extends AbstractTabletFile<T>>
    implements TabletFile, Comparable<T> {

  protected final Path path;
  protected final Range range;

  protected AbstractTabletFile(Path path, Range range) {
    this.path = Objects.requireNonNull(path);
    this.range = requireKeyExtentDataRange(range);
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public Range getRange() {
    return range;
  }

  @Override
  public boolean hasRange() {
    return !range.isInfiniteStartKey() || !range.isInfiniteStopKey();
  }

  @Override
  public String toMinimalString() {
    if (hasRange()) {
      String startRow = range.isInfiniteStartKey() ? "-inf"
          : stripZeroTail(range.getStartKey().getRowData()).toString();
      String endRow = range.isInfiniteStopKey() ? "+inf"
          : stripZeroTail(range.getEndKey().getRowData()).toString();
      return getFileName() + " (" + startRow + "," + endRow + "]";
    } else {
      return getFileName();
    }
  }

}

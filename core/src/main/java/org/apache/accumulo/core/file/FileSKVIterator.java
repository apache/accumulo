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
package org.apache.accumulo.core.file;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Objects;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.util.RowRangeUtil;
import org.apache.hadoop.io.Text;

public interface FileSKVIterator extends InterruptibleIterator, AutoCloseable {

  /**
   * Captures the row range that a file covers. This class can not represent infinite ranges.
   */
  class FileRange {
    public final Range rowRange;
    public final boolean empty;

    public static final FileRange EMPTY = new FileRange();

    public FileRange(Text startRow, Text endrow) {
      this.rowRange = new Range(Objects.requireNonNull(startRow), Objects.requireNonNull(endrow));
      this.empty = false;
    }

    private FileRange() {
      this.rowRange = null;
      this.empty = true;
    }

    private FileRange(Range range) {
      this.rowRange = RowRangeUtil.requireRowRange(Objects.requireNonNull(range));
      Objects.requireNonNull(rowRange.getStartKey());
      Objects.requireNonNull(rowRange.getEndKey());
      this.empty = false;
    }

    /**
     * Computes the union of the two ranges. The returned range should contain both ranges.
     */
    public FileRange union(FileRange other) {
      if (empty) {
        return other;
      }

      if (other.empty) {
        return this;
      }

      var minKey = rowRange.getStartKey().compareTo(other.rowRange.getStartKey()) < 0
          ? rowRange.getStartKey() : other.rowRange.getStartKey();
      var maxKey = rowRange.getEndKey().compareTo(other.rowRange.getEndKey()) > 0
          ? rowRange.getEndKey() : other.rowRange.getEndKey();

      return new FileRange(new Range(minKey, true, maxKey, false));
    }

    /*
     * Computes the intersection of the two ranges. If there is no intersection returns an empty
     * range.
     */
    public FileRange intersect(Range other) {
      RowRangeUtil.requireRowRange(Objects.requireNonNull(other));
      if (empty) {
        return this;
      }

      if (other.isInfiniteStartKey() && other.isInfiniteStopKey()) {
        return this;
      }

      var clipped = rowRange.clip(other, true);
      if (clipped == null) {
        return EMPTY;
      } else {
        return new FileRange(clipped);
      }
    }

    @Override
    public String toString() {
      return "{empty:" + empty + ",rowRange:" + rowRange + "}";
    }
  }

  FileRange getFileRange();

  DataInputStream getMetaStore(String name) throws IOException, NoSuchMetaStoreException;

  /**
   * Returns an estimate of the number of entries that overlap the given extent. This is an estimate
   * because the extent may or may not entirely overlap with each of the index entries included in
   * the count. Will never underestimate but may overestimate.
   *
   * @param extent the key extent
   * @return the estimate
   */
  long estimateOverlappingEntries(KeyExtent extent) throws IOException;

  FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig);

  void closeDeepCopies() throws IOException;

  void setCacheProvider(CacheProvider cacheProvider);

  @Override
  void close() throws IOException;
}

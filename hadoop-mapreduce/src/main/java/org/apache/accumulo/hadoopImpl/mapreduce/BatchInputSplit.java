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
package org.apache.accumulo.hadoopImpl.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;

/**
 * The Class BatchInputSplit. Encapsulates a set of Accumulo ranges on a single tablet for use in
 * Map Reduce jobs. Can contain several Ranges per split.
 */
public class BatchInputSplit extends RangeInputSplit {
  private Collection<Range> ranges;
  private float[] rangeProgress = null;

  public BatchInputSplit() {
    ranges = Collections.emptyList();
  }

  public BatchInputSplit(BatchInputSplit split) throws IOException {
    super(split);
    this.setRanges(split.getRanges());
  }

  public BatchInputSplit(String table, TableId tableId, Collection<Range> ranges,
      String[] locations) {
    super(table, tableId.canonical(), new Range(), locations);
    this.ranges = ranges;
  }

  /**
   * Save progress on each call to this function, implied by value of currentKey, and return average
   * ranges in the split
   */
  @Override
  public float getProgress(Key currentKey) {
    if (rangeProgress == null) {
      rangeProgress = new float[ranges.size()];
    }

    float total = 0; // progress per range could be on different scales, this number is "fuzzy"

    if (currentKey == null) {
      for (float progress : rangeProgress) {
        total += progress;
      }
    } else {
      int i = 0;
      for (Range range : ranges) {
        if (range.contains(currentKey)) {
          // find the current range and report as if that is the single range
          if (range.getStartKey() != null && range.getEndKey() != null) {
            if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW) != 0) {
              // just look at the row progress
              rangeProgress[i] = SplitUtils.getProgress(range.getStartKey().getRowData(),
                  range.getEndKey().getRowData(), currentKey.getRowData());
            } else if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM)
                != 0) {
              // just look at the column family progress
              rangeProgress[i] = SplitUtils.getProgress(range.getStartKey().getColumnFamilyData(),
                  range.getEndKey().getColumnFamilyData(), currentKey.getColumnFamilyData());
            } else if (range.getStartKey().compareTo(range.getEndKey(),
                PartialKey.ROW_COLFAM_COLQUAL) != 0) {
              // just look at the column qualifier progress
              rangeProgress[i] = SplitUtils.getProgress(
                  range.getStartKey().getColumnQualifierData(),
                  range.getEndKey().getColumnQualifierData(), currentKey.getColumnQualifierData());
            }
          }
          total += rangeProgress[i];
        }
        i++;
      }
    }

    return total / ranges.size();
  }

  /**
   * This implementation of length is only an estimate, it does not provide exact values. Do not
   * have your code rely on this return value.
   */
  @Override
  public long getLength() throws IOException {
    long sum = 0;
    for (Range range : ranges) {
      sum += SplitUtils.getRangeLength(range);
    }
    return sum;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    int numRanges = in.readInt();
    ranges = new ArrayList<>(numRanges);
    for (int i = 0; i < numRanges; ++i) {
      Range r = new Range();
      r.readFields(in);
      ranges.add(r);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(ranges.size());
    for (Range r : ranges) {
      r.write(out);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(256);
    sb.append("BatchInputSplit:");
    sb.append(" Ranges: ").append(Arrays.asList(ranges));
    sb.append(super.toString());
    return sb.toString();
  }

  public void setRanges(Collection<Range> ranges) {
    this.ranges = ranges;
  }

  public Collection<Range> getRanges() {
    return ranges;
  }

  @Override
  public Range getRange() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRange(Range range) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Boolean isIsolatedScan() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setIsolatedScan(Boolean isolatedScan) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Boolean isOffline() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setOffline(Boolean offline) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Boolean usesLocalIterators() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setUsesLocalIterators(Boolean localIterators) {
    throw new UnsupportedOperationException();
  }
}

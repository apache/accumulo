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
package org.apache.accumulo.core.client.mapreduce;

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

/**
 * The Class BatchInputSplit. Encapsulates a set of Accumulo ranges on a single tablet for use in Map Reduce jobs.
 * Can contain several Ranges per split.
 */
public class BatchInputSplit extends AccumuloInputSplit {
  private Collection<Range> ranges;
  private int scanThreads;

  public BatchInputSplit() {
    ranges = Collections.emptyList();
  }

  public BatchInputSplit(BatchInputSplit split) throws IOException {
    super(split);
    this.setRanges(split.getRanges());
  }

  protected BatchInputSplit(String table, String tableId, Collection<Range> ranges, String[] locations) {
    super(table, tableId, locations);
    this.ranges = ranges;
  }

  public float getProgress(Key currentKey) {
    if (currentKey == null)
      return 0f;
    for (Range range : ranges) {
      if (range.contains(currentKey)) {
        // find the current range and report as if that is the single range
        if (range.getStartKey() != null && range.getEndKey() != null) {
          if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW) != 0) {
            // just look at the row progress
            return getProgress(range.getStartKey().getRowData(), range.getEndKey().getRowData(), currentKey.getRowData());
          } else if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM) != 0) {
            // just look at the column family progress
            return getProgress(range.getStartKey().getColumnFamilyData(), range.getEndKey().getColumnFamilyData(), currentKey.getColumnFamilyData());
          } else if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM_COLQUAL) != 0) {
            // just look at the column qualifier progress
            return getProgress(range.getStartKey().getColumnQualifierData(), range.getEndKey().getColumnQualifierData(), currentKey.getColumnQualifierData());
          }
        }
      }
    }
    // if we can't figure it out, then claim no progress
    return 0f;
  }

  /**
   * This implementation of length is only an estimate, it does not provide exact values. Do not have your code rely on this return value.
   */
  @Override
  public long getLength() throws IOException {
    long sum = 0;
    for (Range range : ranges)
      sum += getRangeLength(range);
    return sum;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    int numRanges = in.readInt();
    ranges = new ArrayList<Range>(numRanges);
    for (int i = 0; i < numRanges; ++i){
      Range r = new Range();
      r.readFields(in);
      ranges.add(r);
    }

    scanThreads = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(ranges.size());
    for (Range r: ranges)
      r.write(out);

    out.writeInt(scanThreads);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(256);
    sb.append("BatchInputSplit:");
    sb.append(" Ranges: ").append(Arrays.asList(ranges));
    sb.append(" Location: ").append(Arrays.asList(locations));
    sb.append(" Table: ").append(tableName);
    sb.append(" TableID: ").append(tableId);
    sb.append(" InstanceName: ").append(instanceName);
    sb.append(" zooKeepers: ").append(zooKeepers);
    sb.append(" principal: ").append(principal);
    sb.append(" tokenSource: ").append(tokenSource);
    sb.append(" authenticationToken: ").append(token);
    sb.append(" authenticationTokenFile: ").append(tokenFile);
    sb.append(" Authorizations: ").append(auths);
    sb.append(" fetchColumns: ").append(fetchedColumns);
    sb.append(" iterators: ").append(iterators);
    sb.append(" scanThreads:").append(scanThreads);
    sb.append(" logLevel: ").append(level);
    return sb.toString();
  }

  public void setRanges(Collection<Range> ranges) {
    this.ranges = ranges;
  }

  public Collection<Range> getRanges() {
    return ranges;
  }

  public int getScanThreads() {
     return scanThreads;
  }

  public void setScanThreads(int count) {
     scanThreads = count;
  }
}
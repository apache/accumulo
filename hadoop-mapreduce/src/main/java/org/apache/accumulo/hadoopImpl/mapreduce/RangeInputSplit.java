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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.InputConfigurator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * The Class RangeInputSplit. Encapsulates an Accumulo range for use in Map Reduce jobs.
 */
public class RangeInputSplit extends InputSplit implements Writable {
  private Range range;
  private String[] locations;
  private String tableId, tableName;
  private Boolean offline, isolatedScan, localIterators;
  private Set<IteratorSetting.Column> fetchedColumns;
  private List<IteratorSetting> iterators;
  private SamplerConfiguration samplerConfig;
  private Map<String,String> executionHints;

  public RangeInputSplit() {
    range = new Range();
    locations = new String[0];
    tableName = "";
    tableId = "";
  }

  public RangeInputSplit(RangeInputSplit split) throws IOException {
    this.range = split.getRange();
    this.setLocations(split.getLocations());
    this.setTableName(split.getTableName());
    this.setTableId(split.getTableId());
  }

  public RangeInputSplit(String table, String tableId, Range range, String[] locations) {
    this.range = range;
    setLocations(locations);
    this.tableName = table;
    this.tableId = tableId;
  }

  public Range getRange() {
    return range;
  }

  public static float getProgress(ByteSequence start, ByteSequence end, ByteSequence position) {
    return SplitUtils.getProgress(start, end, position);
  }

  public float getProgress(Key currentKey) {
    if (currentKey == null) {
      return 0f;
    }
    if (range.contains(currentKey)) {
      if (range.getStartKey() != null && range.getEndKey() != null) {
        if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW) != 0) {
          // just look at the row progress
          return getProgress(range.getStartKey().getRowData(), range.getEndKey().getRowData(),
              currentKey.getRowData());
        } else if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM) != 0) {
          // just look at the column family progress
          return getProgress(range.getStartKey().getColumnFamilyData(),
              range.getEndKey().getColumnFamilyData(), currentKey.getColumnFamilyData());
        } else if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM_COLQUAL)
            != 0) {
          // just look at the column qualifier progress
          return getProgress(range.getStartKey().getColumnQualifierData(),
              range.getEndKey().getColumnQualifierData(), currentKey.getColumnQualifierData());
        }
      }
    }
    // if we can't figure it out, then claim no progress
    return 0f;
  }

  /**
   * This implementation of length is only an estimate, it does not provide exact values. Do not
   * have your code rely on this return value.
   */
  @Override
  public long getLength() throws IOException {
    return SplitUtils.getRangeLength(range);
  }

  @Override
  public String[] getLocations() {
    return Arrays.copyOf(locations, locations.length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    range.readFields(in);
    tableName = in.readUTF();
    tableId = in.readUTF();
    int numLocs = in.readInt();
    locations = new String[numLocs];
    for (int i = 0; i < numLocs; ++i) {
      locations[i] = in.readUTF();
    }

    if (in.readBoolean()) {
      isolatedScan = in.readBoolean();
    }

    if (in.readBoolean()) {
      offline = in.readBoolean();
    }

    if (in.readBoolean()) {
      localIterators = in.readBoolean();
    }

    if (in.readBoolean()) {
      int numColumns = in.readInt();
      List<String> columns = new ArrayList<>(numColumns);
      for (int i = 0; i < numColumns; i++) {
        columns.add(in.readUTF());
      }

      fetchedColumns = InputConfigurator.deserializeFetchedColumns(columns);
    }

    if (in.readBoolean()) {
      int numIterators = in.readInt();
      iterators = new ArrayList<>(numIterators);
      for (int i = 0; i < numIterators; i++) {
        iterators.add(new IteratorSetting(in));
      }
    }

    if (in.readBoolean()) {
      samplerConfig = new SamplerConfigurationImpl(in).toSamplerConfiguration();
    }

    executionHints = new HashMap<>();
    int numHints = in.readInt();
    for (int i = 0; i < numHints; i++) {
      String k = in.readUTF();
      String v = in.readUTF();
      executionHints.put(k, v);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    range.write(out);
    out.writeUTF(tableName);
    out.writeUTF(tableId);
    out.writeInt(locations.length);
    for (String location : locations) {
      out.writeUTF(location);
    }

    out.writeBoolean(isolatedScan != null);
    if (isolatedScan != null) {
      out.writeBoolean(isolatedScan);
    }

    out.writeBoolean(offline != null);
    if (offline != null) {
      out.writeBoolean(offline);
    }

    out.writeBoolean(localIterators != null);
    if (localIterators != null) {
      out.writeBoolean(localIterators);
    }

    out.writeBoolean(fetchedColumns != null);
    if (fetchedColumns != null) {
      String[] cols = InputConfigurator.serializeColumns(fetchedColumns);
      out.writeInt(cols.length);
      for (String col : cols) {
        out.writeUTF(col);
      }
    }

    out.writeBoolean(iterators != null);
    if (iterators != null) {
      out.writeInt(iterators.size());
      for (IteratorSetting iterator : iterators) {
        iterator.write(out);
      }
    }

    out.writeBoolean(samplerConfig != null);
    if (samplerConfig != null) {
      new SamplerConfigurationImpl(samplerConfig).write(out);
    }

    if (executionHints == null || executionHints.isEmpty()) {
      out.writeInt(0);
    } else {
      out.writeInt(executionHints.size());
      for (Entry<String,String> entry : executionHints.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeUTF(entry.getValue());
      }
    }
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String table) {
    this.tableName = table;
  }

  public void setTableId(String tableId) {
    this.tableId = tableId;
  }

  public String getTableId() {
    return tableId;
  }

  public Boolean isOffline() {
    return offline;
  }

  public void setOffline(Boolean offline) {
    this.offline = offline;
  }

  public void setLocations(String[] locations) {
    this.locations = Arrays.copyOf(locations, locations.length);
  }

  public Boolean isIsolatedScan() {
    return isolatedScan;
  }

  public void setIsolatedScan(Boolean isolatedScan) {
    this.isolatedScan = isolatedScan;
  }

  public void setRange(Range range) {
    this.range = range;
  }

  public Boolean usesLocalIterators() {
    return localIterators;
  }

  public void setUsesLocalIterators(Boolean localIterators) {
    this.localIterators = localIterators;
  }

  public Set<IteratorSetting.Column> getFetchedColumns() {
    return fetchedColumns;
  }

  public void setFetchedColumns(Collection<IteratorSetting.Column> fetchedColumns) {
    this.fetchedColumns = new HashSet<>();
    for (IteratorSetting.Column columns : fetchedColumns) {
      this.fetchedColumns.add(columns);
    }
  }

  public void setFetchedColumns(Set<IteratorSetting.Column> fetchedColumns) {
    this.fetchedColumns = fetchedColumns;
  }

  public List<IteratorSetting> getIterators() {
    return iterators;
  }

  public void setIterators(List<IteratorSetting> iterators) {
    this.iterators = iterators;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(256);
    sb.append("Range: ").append(range);
    sb.append(" Locations: ").append(Arrays.asList(locations));
    sb.append(" Table: ").append(tableName);
    sb.append(" TableID: ").append(tableId);
    sb.append(" offlineScan: ").append(offline);
    sb.append(" isolatedScan: ").append(isolatedScan);
    sb.append(" localIterators: ").append(localIterators);
    sb.append(" fetchColumns: ").append(fetchedColumns);
    sb.append(" iterators: ").append(iterators);
    sb.append(" samplerConfig: ").append(samplerConfig);
    sb.append(" executionHints: ").append(executionHints);
    return sb.toString();
  }

  public void setSamplerConfiguration(SamplerConfiguration samplerConfiguration) {
    this.samplerConfig = samplerConfiguration;
  }

  public SamplerConfiguration getSamplerConfiguration() {
    return samplerConfig;
  }

  public void setExecutionHints(Map<String,String> executionHints) {
    this.executionHints = executionHints;
  }

  public Map<String,String> getExecutionHints() {
    return executionHints;
  }
}

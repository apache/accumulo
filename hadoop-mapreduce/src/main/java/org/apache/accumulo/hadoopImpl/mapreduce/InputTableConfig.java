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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * This class to holds a batch scan configuration for a table. It contains all the properties needed
 * to specify how rows should be returned from the table.
 */
public class InputTableConfig implements Writable {

  // store iterators by name to prevent duplicates for addIterator
  private Map<String,IteratorSetting> iterators = Collections.emptyMap();
  private List<Range> ranges = Collections.emptyList();
  private Collection<IteratorSetting.Column> columns = Collections.emptyList();

  private Optional<Authorizations> scanAuths = Optional.empty();
  private Optional<String> context = Optional.empty();

  private boolean autoAdjustRanges = true;
  private boolean useLocalIterators = false;
  private boolean useIsolatedScanners = false;
  private boolean offlineScan = false;
  private boolean batchScan = false;
  private SamplerConfiguration samplerConfig = null;
  private Map<String,String> executionHints = Collections.emptyMap();
  private ConsistencyLevel consistencyLevel = null;

  public InputTableConfig() {}

  /**
   * Creates a batch scan config object out of a previously serialized batch scan config object.
   *
   * @param input the data input of the serialized batch scan config
   */
  public InputTableConfig(DataInput input) throws IOException {
    readFields(input);
  }

  /**
   * Sets the input ranges to scan for all tables associated with this job. This will be added to
   * any per-table ranges that have been set using
   *
   * @param ranges the ranges that will be mapped over
   * @since 1.6.0
   */
  public InputTableConfig setRanges(List<Range> ranges) {
    this.ranges = ranges;
    return this;
  }

  /**
   * Returns the ranges to be queried in the configuration
   */
  public List<Range> getRanges() {
    return ranges;
  }

  /**
   * Restricts the columns that will be mapped over for this job for the default input table.
   *
   * @param columns a pair of {@link Text} objects corresponding to column family and column
   *        qualifier. If the column qualifier is null, the entire column family is selected. An
   *        empty set is the default and is equivalent to scanning the all columns.
   * @since 1.6.0
   */
  public InputTableConfig fetchColumns(Collection<IteratorSetting.Column> columns) {
    this.columns = columns;
    return this;
  }

  /**
   * Returns the columns to be fetched for this configuration
   */
  public Collection<IteratorSetting.Column> getFetchedColumns() {
    return columns != null ? columns : new HashSet<>();
  }

  public void addIterator(IteratorSetting cfg) {
    if (this.iterators.isEmpty()) {
      this.iterators = new LinkedHashMap<>();
    }
    this.iterators.put(cfg.getName(), cfg);
  }

  /**
   * Returns the iterators to be set on this configuration
   */
  public List<IteratorSetting> getIterators() {
    return new LinkedList<>(iterators.values());
  }

  /**
   * Controls the automatic adjustment of ranges for this job. This feature merges overlapping
   * ranges, then splits them to align with tablet boundaries. Disabling this feature will cause
   * exactly one Map task to be created for each specified range. The default setting is enabled. *
   *
   * <p>
   * By default, this feature is <b>enabled</b>.
   *
   * @param autoAdjustRanges the feature is enabled if true, disabled otherwise
   * @see #setRanges(java.util.List)
   * @since 1.6.0
   */
  public InputTableConfig setAutoAdjustRanges(boolean autoAdjustRanges) {
    this.autoAdjustRanges = autoAdjustRanges;
    return this;
  }

  /**
   * Determines whether a configuration has auto-adjust ranges enabled.
   *
   * @return false if the feature is disabled, true otherwise
   * @since 1.6.0
   * @see #setAutoAdjustRanges(boolean)
   */
  public boolean shouldAutoAdjustRanges() {
    return autoAdjustRanges;
  }

  /**
   * Controls the use of the {@link org.apache.accumulo.core.client.ClientSideIteratorScanner} in
   * this job. Enabling this feature will cause the iterator stack to be constructed within the Map
   * task, rather than within the Accumulo TServer. To use this feature, all classes needed for
   * those iterators must be available on the classpath for the task.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param useLocalIterators the feature is enabled if true, disabled otherwise
   * @since 1.6.0
   */
  public InputTableConfig setUseLocalIterators(boolean useLocalIterators) {
    this.useLocalIterators = useLocalIterators;
    return this;
  }

  /**
   * Determines whether a configuration uses local iterators.
   *
   * @return true if the feature is enabled, false otherwise
   * @since 1.6.0
   * @see #setUseLocalIterators(boolean)
   */
  public boolean shouldUseLocalIterators() {
    return useLocalIterators;
  }

  /**
   * Enable reading offline tables. By default, this feature is disabled and only online tables are
   * scanned. This will make the map reduce job directly read the table's files. If the table is not
   * offline, then the job will fail. If the table comes online during the map reduce job, it is
   * likely that the job will fail.
   *
   * <p>
   * To use this option, the map reduce user will need access to read the Accumulo directory in
   * HDFS.
   *
   * <p>
   * Reading the offline table will create the scan time iterator stack in the map process. So any
   * iterators that are configured for the table will need to be on the mapper's classpath. The
   * accumulo.properties may need to be on the mapper's classpath if HDFS or the Accumulo directory
   * in HDFS are non-standard.
   *
   * <p>
   * One way to use this feature is to clone a table, take the clone offline, and use the clone as
   * the input table for a map reduce job. If you plan to map reduce over the data many times, it
   * may be better to the compact the table, clone it, take it offline, and use the clone for all
   * map reduce jobs. The reason to do this is that compaction will reduce each tablet in the table
   * to one file, and it is faster to read from one file.
   *
   * <p>
   * There are two possible advantages to reading a tables file directly out of HDFS. First, you may
   * see better read performance. Second, it will support speculative execution better. When reading
   * an online table speculative execution can put more load on an already slow tablet server.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param offlineScan the feature is enabled if true, disabled otherwise
   */
  public InputTableConfig setOfflineScan(boolean offlineScan) {
    this.offlineScan = offlineScan;
    return this;
  }

  /**
   * Determines whether a configuration has the offline table scan feature enabled.
   *
   * @return true if the feature is enabled, false otherwise
   * @see #setOfflineScan(boolean)
   */
  public boolean isOfflineScan() {
    return offlineScan;
  }

  /**
   * Controls the use of the {@link org.apache.accumulo.core.client.IsolatedScanner} in this job.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param useIsolatedScanners the feature is enabled if true, disabled otherwise
   */
  public InputTableConfig setUseIsolatedScanners(boolean useIsolatedScanners) {
    this.useIsolatedScanners = useIsolatedScanners;
    return this;
  }

  /**
   * Determines whether a configuration has isolation enabled.
   *
   * @return true if the feature is enabled, false otherwise
   * @see #setUseIsolatedScanners(boolean)
   */
  public boolean shouldUseIsolatedScanners() {
    return useIsolatedScanners;
  }

  public void setUseBatchScan(boolean value) {
    this.batchScan = value;
  }

  public boolean shouldBatchScan() {
    return batchScan;
  }

  /**
   * Set the sampler configuration to use when reading from the data.
   */
  public void setSamplerConfiguration(SamplerConfiguration samplerConfiguration) {
    this.samplerConfig = samplerConfiguration;
  }

  public SamplerConfiguration getSamplerConfiguration() {
    return samplerConfig;
  }

  /**
   * The execution hints to set on created scanners. See {@link ScannerBase#setExecutionHints(Map)}
   */
  public void setExecutionHints(Map<String,String> executionHints) {
    this.executionHints = executionHints;
  }

  public Map<String,String> getExecutionHints() {
    return executionHints;
  }

  public Optional<Authorizations> getScanAuths() {
    return scanAuths;
  }

  public InputTableConfig setScanAuths(Authorizations scanAuths) {
    this.scanAuths = Optional.of(scanAuths);
    return this;
  }

  public Optional<String> getContext() {
    return context;
  }

  public void setContext(String context) {
    this.context = Optional.of(context);
  }

  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    if (iterators != null) {
      dataOutput.writeInt(iterators.size());
      for (IteratorSetting setting : getIterators()) {
        setting.write(dataOutput);
      }
    } else {
      dataOutput.writeInt(0);
    }
    if (ranges != null) {
      dataOutput.writeInt(ranges.size());
      for (Range range : ranges) {
        range.write(dataOutput);
      }
    } else {
      dataOutput.writeInt(0);
    }
    if (columns != null) {
      dataOutput.writeInt(columns.size());
      for (Pair<Text,Text> column : columns) {
        if (column.getSecond() == null) {
          dataOutput.writeInt(1);
          column.getFirst().write(dataOutput);
        } else {
          dataOutput.writeInt(2);
          column.getFirst().write(dataOutput);
          column.getSecond().write(dataOutput);
        }
      }
    } else {
      dataOutput.writeInt(0);
    }
    dataOutput.writeBoolean(autoAdjustRanges);
    dataOutput.writeBoolean(useLocalIterators);
    dataOutput.writeBoolean(useIsolatedScanners);
    dataOutput.writeBoolean(offlineScan);
    if (samplerConfig == null) {
      dataOutput.writeBoolean(false);
    } else {
      dataOutput.writeBoolean(true);
      new SamplerConfigurationImpl(samplerConfig).write(dataOutput);
    }

    if (executionHints.isEmpty()) {
      dataOutput.writeInt(0);
    } else {
      dataOutput.writeInt(executionHints.size());
      for (Entry<String,String> entry : executionHints.entrySet()) {
        dataOutput.writeUTF(entry.getKey());
        dataOutput.writeUTF(entry.getValue());
      }
    }
    if (this.consistencyLevel == null) {
      dataOutput.writeBoolean(false);
    } else {
      dataOutput.writeBoolean(true);
      dataOutput.writeUTF(this.consistencyLevel.name());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    // load iterators
    long iterSize = dataInput.readInt();
    if (iterSize > 0) {
      iterators = new LinkedHashMap<>();
    }
    for (int i = 0; i < iterSize; i++) {
      IteratorSetting newIter = new IteratorSetting(dataInput);
      iterators.put(newIter.getName(), newIter);
    }
    // load ranges
    long rangeSize = dataInput.readInt();
    if (rangeSize > 0) {
      ranges = new ArrayList<>();
    }
    for (int i = 0; i < rangeSize; i++) {
      Range range = new Range();
      range.readFields(dataInput);
      ranges.add(range);
    }
    // load columns
    long columnSize = dataInput.readInt();
    if (columnSize > 0) {
      columns = new HashSet<>();
    }
    for (int i = 0; i < columnSize; i++) {
      long numPairs = dataInput.readInt();
      Text colFam = new Text();
      colFam.readFields(dataInput);
      if (numPairs == 1) {
        columns.add(new IteratorSetting.Column(colFam, null));
      } else if (numPairs == 2) {
        Text colQual = new Text();
        colQual.readFields(dataInput);
        columns.add(new IteratorSetting.Column(colFam, colQual));
      }
    }
    autoAdjustRanges = dataInput.readBoolean();
    useLocalIterators = dataInput.readBoolean();
    useIsolatedScanners = dataInput.readBoolean();
    offlineScan = dataInput.readBoolean();

    if (dataInput.readBoolean()) {
      samplerConfig = new SamplerConfigurationImpl(dataInput).toSamplerConfiguration();
    }

    executionHints = new HashMap<>();
    int numHints = dataInput.readInt();
    for (int i = 0; i < numHints; i++) {
      String k = dataInput.readUTF();
      String v = dataInput.readUTF();
      executionHints.put(k, v);
    }
    if (dataInput.readBoolean()) {
      this.consistencyLevel = ConsistencyLevel.valueOf(dataInput.readUTF());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InputTableConfig that = (InputTableConfig) o;

    if (autoAdjustRanges != that.autoAdjustRanges) {
      return false;
    }
    if (offlineScan != that.offlineScan) {
      return false;
    }
    if (useIsolatedScanners != that.useIsolatedScanners) {
      return false;
    }
    if (useLocalIterators != that.useLocalIterators) {
      return false;
    }
    if (!Objects.equals(columns, that.columns)) {
      return false;
    }
    if (!Objects.equals(iterators, that.iterators)) {
      return false;
    }
    if (!Objects.equals(ranges, that.ranges)) {
      return false;
    }
    if (!Objects.equals(executionHints, that.executionHints)) {
      return false;
    }
    if (!Objects.equals(consistencyLevel, that.consistencyLevel)) {
      return false;
    }
    return Objects.equals(samplerConfig, that.samplerConfig);
  }

  @Override
  public int hashCode() {
    int result = 31 * (iterators != null ? iterators.hashCode() : 0);
    result = 31 * result + (ranges != null ? ranges.hashCode() : 0);
    result = 31 * result + (columns != null ? columns.hashCode() : 0);
    result = 31 * result + (autoAdjustRanges ? 1 : 0);
    result = 31 * result + (useLocalIterators ? 1 : 0);
    result = 31 * result + (useIsolatedScanners ? 1 : 0);
    result = 31 * result + (offlineScan ? 1 : 0);
    result = 31 * result + (samplerConfig == null ? 0 : samplerConfig.hashCode());
    result = 31 * result + (executionHints == null ? 0 : executionHints.hashCode());
    result = 31 * result + (consistencyLevel == null ? 0 : consistencyLevel.hashCode());
    return result;
  }
}

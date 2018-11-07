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
package org.apache.accumulo.hadoop.mapreduce;

import static org.apache.accumulo.hadoopImpl.mapreduce.AbstractInputFormat.setClassLoaderContext;
import static org.apache.accumulo.hadoopImpl.mapreduce.AbstractInputFormat.setClientInfo;
import static org.apache.accumulo.hadoopImpl.mapreduce.AbstractInputFormat.setScanAuthorizations;
import static org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBase.setAutoAdjustRanges;
import static org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBase.setBatchScan;
import static org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBase.setExecutionHints;
import static org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBase.setInputTableName;
import static org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBase.setLocalIterators;
import static org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBase.setOfflineTableScan;
import static org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBase.setRanges;
import static org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBase.setSamplerConfiguration;
import static org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBase.setScanIsolation;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.hadoopImpl.mapreduce.AbstractInputFormat;
import org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBase;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.InputConfigurator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This class allows MapReduce jobs to use Accumulo as the source of data. This {@link InputFormat}
 * provides row names as {@link Text} as keys, and a corresponding {@link PeekingIterator} as a
 * value, which in turn makes the {@link Key}/{@link Value} pairs for that row available to the Map
 * function.
 *
 * The user must specify the following via static configurator method:
 *
 * <ul>
 * <li>{@link AccumuloRowInputFormat#setInfo(Job, InputInfo)}
 * </ul>
 *
 * For required parameters and all available options use {@link InputInfo#builder()}
 *
 * @since 2.0
 */
public class AccumuloRowInputFormat extends InputFormat<Text,PeekingIterator<Entry<Key,Value>>> {
  private static Class CLASS = AccumuloRowInputFormat.class;

  @Override
  public RecordReader<Text,PeekingIterator<Entry<Key,Value>>> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new InputFormatBase.RecordReaderBase<Text,PeekingIterator<Entry<Key,Value>>>() {
      RowIterator rowIterator;

      @Override
      public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
        super.initialize(inSplit, attempt);
        rowIterator = new RowIterator(scannerIterator);
        currentK = new Text();
        currentV = null;
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!rowIterator.hasNext())
          return false;
        currentV = new PeekingIterator<>(rowIterator.next());
        numKeysRead = rowIterator.getKVCount();
        currentKey = currentV.peek().getKey();
        currentK = new Text(currentKey.getRow());
        return true;
      }
    };
  }

  /**
   * Gets the splits of the tables that have been set on the job by reading the metadata table for
   * the specified ranges.
   *
   * @return the splits from the tables based on the ranges.
   * @throws java.io.IOException
   *           if a table set on the job doesn't exist or an error occurs initializing the tablet
   *           locator
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    return AbstractInputFormat.getSplits(context);
  }

  /**
   * Sets all the information required for this map reduce job.
   */
  public static void setInfo(Job job, InputInfo info) {
    setClientInfo(job, info.getClientInfo());
    setScanAuthorizations(job, info.getScanAuths());
    setInputTableName(job, info.getTableName());

    // all optional values
    if (info.getContext().isPresent())
      setClassLoaderContext(job, info.getContext().get());
    if (info.getRanges().size() > 0)
      setRanges(job, info.getRanges());
    if (info.getIterators().size() > 0)
      InputConfigurator.writeIteratorsToConf(CLASS, job.getConfiguration(), info.getIterators());
    if (info.getFetchColumns().size() > 0)
      InputConfigurator.fetchColumns(CLASS, job.getConfiguration(), info.getFetchColumns());
    if (info.getSamplerConfig().isPresent())
      setSamplerConfiguration(job, info.getSamplerConfig().get());
    if (info.getExecutionHints().size() > 0)
      setExecutionHints(job, info.getExecutionHints());
    setAutoAdjustRanges(job, info.isAutoAdjustRanges());
    setScanIsolation(job, info.isScanIsolation());
    setLocalIterators(job, info.isLocalIterators());
    setOfflineTableScan(job, info.isOfflineScan());
    setBatchScan(job, info.isBatchScan());
  }
}

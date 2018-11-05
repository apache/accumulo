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
package org.apache.accumulo.hadoop.mapred;

import static org.apache.accumulo.hadoopImpl.mapred.AbstractInputFormat.setClassLoaderContext;
import static org.apache.accumulo.hadoopImpl.mapred.AbstractInputFormat.setClientInfo;
import static org.apache.accumulo.hadoopImpl.mapred.AbstractInputFormat.setScanAuthorizations;
import static org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.fetchColumns;
import static org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setAutoAdjustRanges;
import static org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setBatchScan;
import static org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setExecutionHints;
import static org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setInputTableName;
import static org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setLocalIterators;
import static org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setOfflineTableScan;
import static org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setRanges;
import static org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setSamplerConfiguration;
import static org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setScanIsolation;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.hadoop.mapreduce.InputInfo;
import org.apache.accumulo.hadoopImpl.mapred.AbstractInputFormat;
import org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.RecordReaderBase;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.InputConfigurator;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class allows MapReduce jobs to use Accumulo as the source of data. This
 * {@link org.apache.hadoop.mapred.InputFormat} provides keys and values of type {@link Key} and
 * {@link Value} to the Map function.
 *
 * The user must specify the following via static configurator method:
 *
 * <ul>
 * <li>{@link AccumuloInputFormat#setInfo(JobConf, InputInfo)}
 * </ul>
 *
 * For required parameters and all available options use {@link InputInfo#builder()}
 *
 * @since 2.0
 */
public class AccumuloInputFormat implements InputFormat<Key,Value> {
  private static Class CLASS = AccumuloInputFormat.class;
  private static Logger log = LoggerFactory.getLogger(CLASS);

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
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return AbstractInputFormat.getSplits(job, numSplits);
  }

  @Override
  public RecordReader<Key,Value> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {

    RecordReaderBase<Key,Value> recordReader = new RecordReaderBase<Key,Value>() {

      @Override
      public boolean next(Key key, Value value) throws IOException {
        if (scannerIterator.hasNext()) {
          ++numKeysRead;
          Entry<Key,Value> entry = scannerIterator.next();
          key.set(currentKey = entry.getKey());
          value.set(entry.getValue().get());
          if (log.isTraceEnabled())
            log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
          return true;
        }
        return false;
      }

      @Override
      public Key createKey() {
        return new Key();
      }

      @Override
      public Value createValue() {
        return new Value();
      }

    };
    recordReader.initialize(split, job);
    return recordReader;
  }

  public static void setInfo(JobConf job, InputInfo info) {
    setClientInfo(job, info.getClientInfo());
    setScanAuthorizations(job, info.getScanAuths());
    setInputTableName(job, info.getTableName());

    // all optional values
    if (info.getContext().isPresent())
      setClassLoaderContext(job, info.getContext().get());
    if (info.getRanges().size() > 0)
      setRanges(job, info.getRanges());
    if (info.getIterators().size() > 0)
      InputConfigurator.writeIteratorsToConf(CLASS, job, info.getIterators());
    if (info.getFetchColumns().size() > 0)
      InputConfigurator.fetchColumns(CLASS, job, info.getFetchColumns());
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

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

import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.hadoop.mapreduce.InputInfo;
import org.apache.accumulo.hadoopImpl.mapred.InputFormatBase;
import org.apache.accumulo.hadoopImpl.mapreduce.RangeInputSplit;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Level;

/**
 * This class allows MapReduce jobs to use Accumulo as the source of data. This {@link InputFormat}
 * provides keys and values of type {@link Key} and {@link Value} to the Map function.
 *
 * The user must specify the following via static configurator method:
 *
 * <ul>
 * <li>{@link AccumuloInputFormat#setInfo(JobConf, InputInfo)}
 * </ul>
 *
 * For required parameters and all available options use {@link InputInfo#builder()}
 */
public class AccumuloInputFormat extends InputFormatBase<Key,Value> {

  @Override
  public RecordReader<Key,Value> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    log.setLevel(getLogLevel(job));

    // Override the log level from the configuration as if the RangeInputSplit has one it's the more
    // correct one to use.
    if (split instanceof RangeInputSplit) {
      RangeInputSplit accSplit = (RangeInputSplit) split;
      Level level = accSplit.getLogLevel();
      if (null != level) {
        log.setLevel(level);
      }
    } else {
      throw new IllegalArgumentException("No RecordReader for " + split.getClass());
    }

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
      addAllIteratorsMapRed(job, info);
    if (info.getFetchColumns().size() > 0)
      convertFetchColumnsMapRed(job, info.getFetchColumns());
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

  private static void convertFetchColumnsMapRed(JobConf job,
      Collection<Pair<byte[],byte[]>> fetchColumns) {
    fetchColumns(job, fetchColumns.stream().map(p -> {
      Text cf = (p.getFirst() != null) ? new Text(p.getFirst()) : null;
      Text cq = (p.getSecond() != null) ? new Text(p.getSecond()) : null;
      return new Pair<>(cf, cq);
    }).collect(Collectors.toList()));
  }

  private static void addAllIteratorsMapRed(JobConf job, InputInfo info) {
    for (IteratorSetting cfg : info.getIterators())
      addIterator(job, cfg);
  }
}

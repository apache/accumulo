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
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.hadoop.mapreduce.InputInfo;
import org.apache.accumulo.hadoopImpl.mapred.InputFormatBase;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * This class allows MapReduce jobs to use Accumulo as the source of data. This {@link InputFormat}
 * provides row names as {@link Text} as keys, and a corresponding {@link PeekingIterator} as a
 * value, which in turn makes the {@link Key}/{@link Value} pairs for that row available to the Map
 * function.
 *
 * The user must specify the following via static configurator method:
 *
 * <ul>
 * <li>{@link AccumuloRowInputFormat#setInfo(JobConf, InputInfo)}
 * </ul>
 *
 * For required parameters and all available options use {@link InputInfo#builder()}
 */
public class AccumuloRowInputFormat
    extends InputFormatBase<Text,PeekingIterator<Entry<Key,Value>>> {
  @Override
  public RecordReader<Text,PeekingIterator<Entry<Key,Value>>> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    // @formatter:off
    RecordReaderBase<Text,PeekingIterator<Entry<Key,Value>>> recordReader =
      new RecordReaderBase<Text,PeekingIterator<Entry<Key,Value>>>() {
    // @formatter:on
          RowIterator rowIterator;

          @Override
          public void initialize(InputSplit inSplit, JobConf job) throws IOException {
            super.initialize(inSplit, job);
            rowIterator = new RowIterator(scannerIterator);
          }

          @Override
          public boolean next(Text key, PeekingIterator<Entry<Key,Value>> value)
              throws IOException {
            if (!rowIterator.hasNext())
              return false;
            value.initialize(rowIterator.next());
            numKeysRead = rowIterator.getKVCount();
            key.set((currentKey = value.peek().getKey()).getRow());
            return true;
          }

          @Override
          public Text createKey() {
            return new Text();
          }

          @Override
          public PeekingIterator<Entry<Key,Value>> createValue() {
            return new PeekingIterator<>();
          }
        };
    recordReader.initialize(split, job);
    return recordReader;
  }

  /**
   * Sets all the information required for this map reduce job.
   */
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
      addAllIterators(job, info);
    if (info.getFetchColumns().size() > 0)
      convertFetchColumns(job, info.getFetchColumns());
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

  private static void convertFetchColumns(JobConf job,
      Collection<Pair<byte[],byte[]>> fetchColumns) {
    InputFormatBase.fetchColumns(job, fetchColumns.stream().map(p -> {
      Text cf = (p.getFirst() != null) ? new Text(p.getFirst()) : null;
      Text cq = (p.getSecond() != null) ? new Text(p.getSecond()) : null;
      return new Pair<>(cf, cq);
    }).collect(Collectors.toList()));
  }

  private static void addAllIterators(JobConf job, InputInfo info) {
    for (IteratorSetting cfg : info.getIterators())
      InputFormatBase.addIterator(job, cfg);
  }
}

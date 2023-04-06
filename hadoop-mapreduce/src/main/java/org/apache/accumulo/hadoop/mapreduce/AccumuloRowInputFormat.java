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
package org.apache.accumulo.hadoop.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.hadoopImpl.mapreduce.AccumuloRecordReader;
import org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBuilderImpl;
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
 * function. Configure the job using the {@link #configure()} method, which provides a fluent API.
 * For Example:
 *
 * <pre>
 * AccumuloRowInputFormat.configure().clientProperties(props).table(name) // required
 *     .auths(auths).addIterator(iter1).ranges(ranges).fetchColumns(columns).executionHints(hints)
 *     .samplerConfiguration(sampleConf).autoAdjustRanges(false) // enabled by default
 *     .scanIsolation(true) // not available with batchScan()
 *     .offlineScan(true) // not available with batchScan()
 *     .store(job);
 * </pre>
 *
 * For descriptions of all options see
 * {@link org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder.InputFormatOptions}
 *
 * @since 2.0
 */
public class AccumuloRowInputFormat extends InputFormat<Text,PeekingIterator<Entry<Key,Value>>> {
  private static final Class<AccumuloRowInputFormat> CLASS = AccumuloRowInputFormat.class;

  @Override
  public RecordReader<Text,PeekingIterator<Entry<Key,Value>>> createRecordReader(InputSplit split,
      TaskAttemptContext context) {
    return new AccumuloRecordReader<>(CLASS) {
      RowIterator rowIterator;

      @Override
      public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
        super.initialize(inSplit, attempt);
        rowIterator = new RowIterator(scannerIterator);
        currentK = new Text();
        currentV = null;
      }

      @Override
      public boolean nextKeyValue() {
        if (!rowIterator.hasNext()) {
          return false;
        }
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
   * @throws java.io.IOException if a table set on the job doesn't exist or an error occurs
   *         initializing the tablet locator
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    return AccumuloRecordReader.getSplits(context, CLASS);
  }

  /**
   * Sets all the information required for this map reduce job.
   */
  public static InputFormatBuilder.ClientParams<Job> configure() {
    return new InputFormatBuilderImpl<>(CLASS);
  }
}

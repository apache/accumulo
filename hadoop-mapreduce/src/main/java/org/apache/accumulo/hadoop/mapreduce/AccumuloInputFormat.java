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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.hadoopImpl.mapreduce.AccumuloRecordReader;
import org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBuilderImpl;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class allows MapReduce jobs to use Accumulo as the source of data. This {@link InputFormat}
 * provides keys and values of type {@link Key} and {@link Value} to the Map function. Configure the
 * job using the {@link #configure()} method, which provides a fluent API. For Example:
 *
 * <pre>
 * AccumuloInputFormat.configure().clientProperties(props).table(name) // required
 *     .auths(auths).addIterator(iter1).ranges(ranges).fetchColumns(columns).executionHints(hints)
 *     .samplerConfiguration(sampleConf).autoAdjustRanges(false) // enabled by default
 *     .scanIsolation(true) // not available with batchScan()
 *     .offlineScan(true) // not available with batchScan()
 *     .store(job);
 * </pre>
 *
 * Multiple tables can be set by configuring clientProperties once and then calling .table() for
 * each table. The methods following a call to .table() apply only to that table. For Example:
 *
 * <pre>
 * AccumuloInputFormat.configure().clientProperties(props) // set client props once
 *     .table(table1).auths(auths1).fetchColumns(cols1).batchScan(true) // options for table1
 *     .table(table2).ranges(range2).auths(auths2).addIterator(iter2) // options for table2
 *     .table(table3).ranges(range3).auths(auths3).addIterator(iter3) // options for table3
 *     .store(job); // store all tables in the job when finished
 * </pre>
 *
 * For descriptions of all options see
 * {@link org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder.InputFormatOptions}
 *
 * @since 2.0
 */
public class AccumuloInputFormat extends InputFormat<Key,Value> {
  private static final Class<AccumuloInputFormat> CLASS = AccumuloInputFormat.class;
  private static final Logger log = LoggerFactory.getLogger(CLASS);

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

  @Override
  public RecordReader<Key,Value> createRecordReader(InputSplit split, TaskAttemptContext context) {

    return new AccumuloRecordReader<>(CLASS) {
      @Override
      public boolean nextKeyValue() {
        if (scannerIterator.hasNext()) {
          ++numKeysRead;
          Entry<Key,Value> entry = scannerIterator.next();
          currentK = currentKey = entry.getKey();
          currentV = entry.getValue();
          if (log.isTraceEnabled()) {
            log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
          }
          return true;
        }
        return false;
      }
    };
  }

  /**
   * Sets all the information required for this map reduce job.
   */
  public static InputFormatBuilder.ClientParams<Job> configure() {
    return new InputFormatBuilderImpl<>(CLASS);
  }
}

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
package org.apache.accumulo.hadoop.mapred;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder;
import org.apache.accumulo.hadoopImpl.mapred.AccumuloRecordReader;
import org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBuilderImpl;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat
 *
 * @since 2.0
 */
public class AccumuloInputFormat implements InputFormat<Key,Value> {
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
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return AccumuloRecordReader.getSplits(job, CLASS);
  }

  @Override
  public RecordReader<Key,Value> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {

    AccumuloRecordReader<Key,Value> recordReader = new AccumuloRecordReader<>(CLASS) {

      @Override
      public boolean next(Key key, Value value) {
        if (scannerIterator.hasNext()) {
          ++numKeysRead;
          Entry<Key,Value> entry = scannerIterator.next();
          key.set(currentKey = entry.getKey());
          value.set(entry.getValue().get());
          if (log.isTraceEnabled()) {
            log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
          }
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

  /**
   * Sets all the information required for this map reduce job.
   */
  public static InputFormatBuilder.ClientParams<JobConf> configure() {
    return new InputFormatBuilderImpl<>(CLASS);
  }
}

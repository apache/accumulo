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

import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder;
import org.apache.accumulo.hadoopImpl.mapred.AccumuloRecordReader;
import org.apache.accumulo.hadoopImpl.mapreduce.InputFormatBuilderImpl;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * @see org.apache.accumulo.hadoop.mapreduce.AccumuloRowInputFormat
 *
 * @since 2.0
 */
public class AccumuloRowInputFormat implements InputFormat<Text,PeekingIterator<Entry<Key,Value>>> {
  private static final Class<AccumuloRowInputFormat> CLASS = AccumuloRowInputFormat.class;

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
  public RecordReader<Text,PeekingIterator<Entry<Key,Value>>> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    AccumuloRecordReader<Text,PeekingIterator<Entry<Key,Value>>> recordReader =
        new AccumuloRecordReader<>(CLASS) {
          RowIterator rowIterator;

          @Override
          public void initialize(InputSplit inSplit, JobConf job) throws IOException {
            super.initialize(inSplit, job);
            rowIterator = new RowIterator(scannerIterator);
          }

          @Override
          public boolean next(Text key, PeekingIterator<Entry<Key,Value>> value) {
            if (!rowIterator.hasNext()) {
              return false;
            }
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
  public static InputFormatBuilder.ClientParams<JobConf> configure() {
    return new InputFormatBuilderImpl<>(CLASS);
  }
}

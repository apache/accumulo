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
package org.apache.accumulo.core.client.mapred;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Level;

/**
 * This class allows MapReduce jobs to use Accumulo as the source of data. This {@link InputFormat} provides keys and values of type {@link Key} and
 * {@link Value} to the Map function.
 *
 * The user must specify the following via static configurator methods:
 *
 * <ul>
 * <li>{@link AccumuloInputFormat#setConnectorInfo(JobConf, String, AuthenticationToken)}
 * <li>{@link AccumuloInputFormat#setConnectorInfo(JobConf, String, String)}
 * <li>{@link AccumuloInputFormat#setScanAuthorizations(JobConf, Authorizations)}
 * <li>{@link AccumuloInputFormat#setZooKeeperInstance(JobConf, ClientConfiguration)} OR {@link AccumuloInputFormat#setMockInstance(JobConf, String)}
 * </ul>
 *
 * Other static methods are optional.
 */
public class AccumuloInputFormat extends InputFormatBase<Key,Value> {

  @Override
  public RecordReader<Key,Value> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    log.setLevel(getLogLevel(job));

    // Override the log level from the configuration as if the RangeInputSplit has one it's the more correct one to use.
    if (split instanceof org.apache.accumulo.core.client.mapreduce.RangeInputSplit) {
      org.apache.accumulo.core.client.mapreduce.RangeInputSplit risplit = (org.apache.accumulo.core.client.mapreduce.RangeInputSplit) split;
      Level level = risplit.getLogLevel();
      if (null != level) {
        log.setLevel(level);
      }
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
}

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
import java.util.Map;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.InputTableConfig;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * This class allows MapReduce jobs to use multiple Accumulo tables as the source of data. This {@link org.apache.hadoop.mapred.InputFormat} provides keys and
 * values of type {@link Key} and {@link Value} to the Map function.
 *
 * The user must specify the following via static configurator methods:
 *
 * <ul>
 * <li>{@link AccumuloInputFormat#setConnectorInfo(JobConf, String, org.apache.accumulo.core.client.security.tokens.AuthenticationToken)}
 * <li>{@link AccumuloInputFormat#setConnectorInfo(JobConf, String, String)}
 * <li>{@link AccumuloInputFormat#setScanAuthorizations(JobConf, org.apache.accumulo.core.security.Authorizations)}
 * <li>{@link AccumuloInputFormat#setZooKeeperInstance(JobConf, ClientConfiguration)}
 * <li>{@link AccumuloMultiTableInputFormat#setInputTableConfigs(org.apache.hadoop.mapred.JobConf, java.util.Map)}
 * </ul>
 *
 * Other static methods are optional.
 */

public class AccumuloMultiTableInputFormat extends AbstractInputFormat<Key,Value> {

  /**
   * Sets the {@link InputTableConfig} objects on the given Hadoop configuration
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param configs
   *          the table query configs to be set on the configuration.
   * @since 1.6.0
   */
  public static void setInputTableConfigs(JobConf job, Map<String,InputTableConfig> configs) {
    InputConfigurator.setInputTableConfigs(CLASS, job, configs);
  }

  @Override
  public RecordReader<Key,Value> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    log.setLevel(getLogLevel(job));
    InputFormatBase.RecordReaderBase<Key,Value> recordReader = new InputFormatBase.RecordReaderBase<Key,Value>() {

      @Override
      public boolean next(Key key, Value value) throws IOException {
        if (scannerIterator.hasNext()) {
          ++numKeysRead;
          Map.Entry<Key,Value> entry = scannerIterator.next();
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

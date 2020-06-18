/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.client.mapreduce;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;

/**
 * This class allows MapReduce jobs to use Accumulo as the source of data. This {@link InputFormat}
 * provides keys and values of type {@link Key} and {@link Value} to the Map function.
 *
 * The user must specify the following via static configurator methods:
 *
 * <ul>
 * <li>{@link AccumuloInputFormat#setConnectorInfo(Job, String, AuthenticationToken)}
 * <li>{@link AccumuloInputFormat#setConnectorInfo(Job, String, String)}
 * <li>{@link AccumuloInputFormat#setInputTableName(Job, String)}
 * <li>{@link AccumuloInputFormat#setScanAuthorizations(Job, Authorizations)}
 * </ul>
 *
 * Other static methods are optional.
 *
 * @deprecated since 2.0.0; Use org.apache.accumulo.hadoop.mapreduce instead from the
 *             accumulo-hadoop-mapreduce.jar
 */
@Deprecated
public class AccumuloInputFormat extends InputFormatBase<Key,Value> {

  @Override
  public RecordReader<Key,Value> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    log.setLevel(getLogLevel(context));

    // Override the log level from the configuration as if the InputSplit has one it's the more
    // correct one to use.
    if (split instanceof org.apache.accumulo.core.client.mapreduce.RangeInputSplit) {
      org.apache.accumulo.core.client.mapreduce.RangeInputSplit accSplit =
          (org.apache.accumulo.core.client.mapreduce.RangeInputSplit) split;
      Level level = accSplit.getLogLevel();
      if (level != null) {
        log.setLevel(level);
      }
    } else {
      throw new IllegalArgumentException("No RecordReader for " + split.getClass());
    }

    return new RecordReaderBase<>() {
      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (scannerIterator.hasNext()) {
          ++numKeysRead;
          Entry<Key,Value> entry = scannerIterator.next();
          currentK = currentKey = entry.getKey();
          currentV = entry.getValue();
          if (log.isTraceEnabled())
            log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
          return true;
        }
        return false;
      }
    };
  }
}

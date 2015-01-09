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
package org.apache.accumulo.core.client.mapreduce;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This class allows MapReduce jobs to use Accumulo as the source of data. This {@link InputFormat} provides row names as {@link Text} as keys, and a
 * corresponding {@link PeekingIterator} as a value, which in turn makes the {@link Key}/{@link Value} pairs for that row available to the Map function.
 *
 * The user must specify the following via static configurator methods:
 *
 * <ul>
 * <li>{@link AccumuloRowInputFormat#setConnectorInfo(Job, String, AuthenticationToken)}
 * <li>{@link AccumuloRowInputFormat#setInputTableName(Job, String)}
 * <li>{@link AccumuloRowInputFormat#setScanAuthorizations(Job, Authorizations)}
 * <li>{@link AccumuloRowInputFormat#setZooKeeperInstance(Job, ClientConfiguration)} OR {@link AccumuloRowInputFormat#setMockInstance(Job, String)}
 * </ul>
 *
 * Other static methods are optional.
 */
public class AccumuloRowInputFormat extends InputFormatBase<Text,PeekingIterator<Entry<Key,Value>>> {
  @Override
  public RecordReader<Text,PeekingIterator<Entry<Key,Value>>> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    log.setLevel(getLogLevel(context));
    return new RecordReaderBase<Text,PeekingIterator<Entry<Key,Value>>>() {
      RowIterator rowIterator;

      @Override
      public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
        super.initialize(inSplit, attempt);
        rowIterator = new RowIterator(scannerIterator);
        currentK = new Text();
        currentV = null;
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!rowIterator.hasNext())
          return false;
        currentV = new PeekingIterator<Entry<Key,Value>>(rowIterator.next());
        numKeysRead = rowIterator.getKVCount();
        currentKey = currentV.peek().getKey();
        currentK = new Text(currentKey.getRow());
        return true;
      }
    };
  }
}

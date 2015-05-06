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
package org.apache.accumulo.examples.simple.client;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal class used to verify validity of data read.
 */
class CountingVerifyingReceiver {
  private static final Logger log = LoggerFactory.getLogger(CountingVerifyingReceiver.class);

  long count = 0;
  int expectedValueSize = 0;
  HashMap<Text,Boolean> expectedRows;

  CountingVerifyingReceiver(HashMap<Text,Boolean> expectedRows, int expectedValueSize) {
    this.expectedRows = expectedRows;
    this.expectedValueSize = expectedValueSize;
  }

  public void receive(Key key, Value value) {

    String row = key.getRow().toString();
    long rowid = Integer.parseInt(row.split("_")[1]);

    byte expectedValue[] = RandomBatchWriter.createValue(rowid, expectedValueSize);

    if (!Arrays.equals(expectedValue, value.get())) {
      log.error("Got unexpected value for " + key + " expected : " + new String(expectedValue, UTF_8) + " got : " + new String(value.get(), UTF_8));
    }

    if (!expectedRows.containsKey(key.getRow())) {
      log.error("Got unexpected key " + key);
    } else {
      expectedRows.put(key.getRow(), true);
    }

    count++;
  }
}

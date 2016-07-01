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
package org.apache.accumulo.test;

import java.util.TreeMap;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

class TextMemoryUsageTest extends MemoryUsageTest {

  private int keyLen;
  private int colFamLen;
  private int colQualLen;
  private int dataLen;
  private TreeMap<Text,Value> map;
  private int passes;

  TextMemoryUsageTest(int passes, int keyLen, int colFamLen, int colQualLen, int dataLen) {
    this.keyLen = keyLen;
    this.colFamLen = colFamLen;
    this.colQualLen = colQualLen;
    this.dataLen = dataLen;
    this.passes = passes;

  }

  @Override
  void init() {
    map = new TreeMap<>();
  }

  @Override
  public void addEntry(int i) {
    Text key = new Text(String.format("%0" + keyLen + "d:%0" + colFamLen + "d:%0" + colQualLen + "d", i, 0, 0).getBytes());
    //
    byte data[] = new byte[dataLen];
    for (int j = 0; j < data.length; j++) {
      data[j] = (byte) (j % 10 + 65);
    }
    Value value = new Value(data);

    map.put(key, value);

  }

  @Override
  public void clear() {
    map.clear();
    map = null;
  }

  @Override
  public int getEstimatedBytesPerEntry() {
    return keyLen + colFamLen + colQualLen + dataLen;
  }

  @Override
  int getNumPasses() {
    return passes;
  }

  @Override
  String getName() {
    return "Text " + keyLen + " " + colFamLen + " " + colQualLen + " " + dataLen;
  }

}

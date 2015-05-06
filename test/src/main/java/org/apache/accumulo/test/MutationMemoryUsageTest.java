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

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

class MutationMemoryUsageTest extends MemoryUsageTest {

  private int keyLen;
  private int colFamLen;
  private int colQualLen;
  private int dataLen;

  private Mutation[] mutations;
  private Text key;
  private Text colf;
  private Text colq;
  private int passes;

  MutationMemoryUsageTest(int passes, int keyLen, int colFamLen, int colQualLen, int dataLen) {
    this.keyLen = keyLen;
    this.colFamLen = colFamLen;
    this.colQualLen = colQualLen;
    this.dataLen = dataLen;
    this.passes = passes;
    mutations = new Mutation[passes];

  }

  @Override
  void init() {
    key = new Text();

    colf = new Text(String.format("%0" + colFamLen + "d", 0));
    colq = new Text(String.format("%0" + colQualLen + "d", 0));

    byte data[] = new byte[dataLen];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 10 + 65);
    }
  }

  @Override
  public void addEntry(int i) {
    key.set(String.format("%0" + keyLen + "d", i));

    Mutation m = new Mutation(key);

    byte data[] = new byte[dataLen];
    for (int j = 0; j < data.length; j++) {
      data[j] = (byte) (j % 10 + 65);
    }
    Value idata = new Value(data);

    m.put(colf, colq, idata);

    mutations[i] = m;
  }

  @Override
  public int getEstimatedBytesPerEntry() {
    return keyLen + colFamLen + colQualLen + dataLen;
  }

  @Override
  public void clear() {
    key = null;
    colf = null;
    colq = null;
    mutations = null;
  }

  @Override
  int getNumPasses() {
    return passes;
  }

  @Override
  String getName() {
    return "Mutation " + keyLen + " " + colFamLen + " " + colQualLen + " " + dataLen;
  }
}

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
package org.apache.accumulo.tserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.Key;

class MemKey extends Key {

  int kvCount;

  public MemKey(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean del, boolean copy, int mc) {
    super(row, cf, cq, cv, ts, del, copy);
    this.kvCount = mc;
  }

  public MemKey() {
    super();
    this.kvCount = Integer.MAX_VALUE;
  }

  public MemKey(Key key, int mc) {
    super(key);
    this.kvCount = mc;
  }

  public String toString() {
    return super.toString() + " mc=" + kvCount;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(kvCount);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    kvCount = in.readInt();
  }

  @Override
  public int compareTo(Key k) {

    int cmp = super.compareTo(k);

    if (cmp == 0 && k instanceof MemKey) {
      cmp = ((MemKey) k).kvCount - kvCount;
    }

    return cmp;
  }

}

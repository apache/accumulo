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
package org.apache.accumulo.core.iterators.aggregation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.WritableUtils;

/**
 * @deprecated since 1.4, replaced by {@link org.apache.accumulo.core.iterators.user.SummingArrayCombiner} with
 *             {@link org.apache.accumulo.core.iterators.user.SummingArrayCombiner.Type#VARLEN}
 */
@Deprecated
public class NumArraySummation implements Aggregator {
  long[] sum = new long[0];

  @Override
  public Value aggregate() {
    try {
      return new Value(NumArraySummation.longArrayToBytes(sum));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void collect(Value value) {
    long[] la;
    try {
      la = NumArraySummation.bytesToLongArray(value.get());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (la.length > sum.length) {
      for (int i = 0; i < sum.length; i++) {
        la[i] = NumSummation.safeAdd(la[i], sum[i]);
      }
      sum = la;
    } else {
      for (int i = 0; i < la.length; i++) {
        sum[i] = NumSummation.safeAdd(sum[i], la[i]);
      }
    }
  }

  public static byte[] longArrayToBytes(long[] la) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    WritableUtils.writeVInt(dos, la.length);
    for (int i = 0; i < la.length; i++) {
      WritableUtils.writeVLong(dos, la[i]);
    }

    return baos.toByteArray();
  }

  public static long[] bytesToLongArray(byte[] b) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
    int len = WritableUtils.readVInt(dis);

    long[] la = new long[len];

    for (int i = 0; i < len; i++) {
      la[i] = WritableUtils.readVLong(dis);
    }

    return la;
  }

  @Override
  public void reset() {
    sum = new long[0];
  }

}

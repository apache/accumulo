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
 * @deprecated since 1.4, replaced by {@link org.apache.accumulo.core.iterators.user.SummingCombiner} with
 *             {@link org.apache.accumulo.core.iterators.LongCombiner.Type#VARLEN}
 */
@Deprecated
public class NumSummation implements Aggregator {
  long sum = 0l;

  public Value aggregate() {
    try {
      return new Value(NumSummation.longToBytes(sum));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void collect(Value value) {
    long l;
    try {
      l = NumSummation.bytesToLong(value.get());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    sum = NumSummation.safeAdd(sum, l);
  }

  public static byte[] longToBytes(long l) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    WritableUtils.writeVLong(dos, l);

    return baos.toByteArray();
  }

  public static long bytesToLong(byte[] b) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
    return WritableUtils.readVLong(dis);
  }

  public static long safeAdd(long a, long b) {
    long aSign = Long.signum(a);
    long bSign = Long.signum(b);
    if ((aSign != 0) && (bSign != 0) && (aSign == bSign)) {
      if (aSign > 0) {
        if (Long.MAX_VALUE - a < b)
          return Long.MAX_VALUE;
      } else {
        if (Long.MIN_VALUE - a > b)
          return Long.MIN_VALUE;
      }
    }
    return a + b;
  }

  public void reset() {
    sum = 0l;
  }

}

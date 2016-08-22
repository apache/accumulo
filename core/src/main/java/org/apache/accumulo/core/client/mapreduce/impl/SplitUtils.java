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

package org.apache.accumulo.core.client.mapreduce.impl;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mapreduce.InputTableConfig;
import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.DeprecationUtil;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;

public class SplitUtils {

  /**
   * Central place to set common split configuration not handled by split constructors. The intention is to make it harder to miss optional setters in future
   * refactor.
   */
  public static void updateSplit(RangeInputSplit split, Instance instance, InputTableConfig tableConfig, String principal, AuthenticationToken token,
      Authorizations auths, Level logLevel) {
    split.setInstanceName(instance.getInstanceName());
    split.setZooKeepers(instance.getZooKeepers());
    DeprecationUtil.setMockInstance(split, DeprecationUtil.isMockInstance(instance));

    split.setPrincipal(principal);
    split.setToken(token);
    split.setAuths(auths);

    split.setFetchedColumns(tableConfig.getFetchedColumns());
    split.setIterators(tableConfig.getIterators());
    split.setLogLevel(logLevel);

    split.setSamplerConfiguration(tableConfig.getSamplerConfiguration());
  }

  public static float getProgress(ByteSequence start, ByteSequence end, ByteSequence position) {
    int maxDepth = Math.min(Math.max(end.length(), start.length()), position.length());
    BigInteger startBI = new BigInteger(SplitUtils.extractBytes(start, maxDepth));
    BigInteger endBI = new BigInteger(SplitUtils.extractBytes(end, maxDepth));
    BigInteger positionBI = new BigInteger(SplitUtils.extractBytes(position, maxDepth));
    return (float) (positionBI.subtract(startBI).doubleValue() / endBI.subtract(startBI).doubleValue());
  }

  public static long getRangeLength(Range range) throws IOException {
    Text startRow = range.isInfiniteStartKey() ? new Text(new byte[] {Byte.MIN_VALUE}) : range.getStartKey().getRow();
    Text stopRow = range.isInfiniteStopKey() ? new Text(new byte[] {Byte.MAX_VALUE}) : range.getEndKey().getRow();
    int maxCommon = Math.min(7, Math.min(startRow.getLength(), stopRow.getLength()));
    long diff = 0;

    byte[] start = startRow.getBytes();
    byte[] stop = stopRow.getBytes();
    for (int i = 0; i < maxCommon; ++i) {
      diff |= 0xff & (start[i] ^ stop[i]);
      diff <<= Byte.SIZE;
    }

    if (startRow.getLength() != stopRow.getLength())
      diff |= 0xff;

    return diff + 1;
  }

  static byte[] extractBytes(ByteSequence seq, int numBytes) {
    byte[] bytes = new byte[numBytes + 1];
    bytes[0] = 0;
    for (int i = 0; i < numBytes; i++) {
      if (i >= seq.length())
        bytes[i + 1] = 0;
      else
        bytes[i + 1] = seq.byteAt(i);
    }
    return bytes;
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.hadoopImpl.mapreduce;

import java.math.BigInteger;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

public class SplitUtils {

  /**
   * Central place to set common split configuration not handled by split constructors. The
   * intention is to make it harder to miss optional setters in future refactor.
   */
  public static void updateSplit(RangeInputSplit split, InputTableConfig tableConfig) {
    split.setFetchedColumns(tableConfig.getFetchedColumns());
    split.setIterators(tableConfig.getIterators());
    split.setSamplerConfiguration(tableConfig.getSamplerConfiguration());
    split.setExecutionHints(tableConfig.getExecutionHints());
  }

  public static float getProgress(ByteSequence start, ByteSequence end, ByteSequence position) {
    int maxDepth = Math.min(Math.max(end.length(), start.length()), position.length());
    BigInteger startBI = new BigInteger(SplitUtils.extractBytes(start, maxDepth));
    BigInteger endBI = new BigInteger(SplitUtils.extractBytes(end, maxDepth));
    BigInteger positionBI = new BigInteger(SplitUtils.extractBytes(position, maxDepth));
    return (float) (positionBI.subtract(startBI).doubleValue()
        / endBI.subtract(startBI).doubleValue());
  }

  public static long getRangeLength(Range range) {
    Text startRow = range.isInfiniteStartKey() ? new Text(new byte[] {Byte.MIN_VALUE})
        : range.getStartKey().getRow();
    Text stopRow = range.isInfiniteStopKey() ? new Text(new byte[] {Byte.MAX_VALUE})
        : range.getEndKey().getRow();
    int maxCommon = Math.min(7, Math.min(startRow.getLength(), stopRow.getLength()));
    long diff = 0;

    byte[] start = startRow.getBytes();
    byte[] stop = stopRow.getBytes();
    for (int i = 0; i < maxCommon; ++i) {
      diff |= 0xff & (start[i] ^ stop[i]);
      diff <<= Byte.SIZE;
    }

    if (startRow.getLength() != stopRow.getLength()) {
      diff |= 0xff;
    }

    return diff + 1;
  }

  static byte[] extractBytes(ByteSequence seq, int numBytes) {
    byte[] bytes = new byte[numBytes + 1];
    bytes[0] = 0;
    for (int i = 0; i < numBytes; i++) {
      if (i >= seq.length()) {
        bytes[i + 1] = 0;
      } else {
        bytes[i + 1] = seq.byteAt(i);
      }
    }
    return bytes;
  }

}

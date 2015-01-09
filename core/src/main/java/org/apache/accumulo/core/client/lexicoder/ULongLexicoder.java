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
package org.apache.accumulo.core.client.lexicoder;

/**
 * Unsigned long lexicoder. The lexicographic encoding sorts first 0l and -1l last. This encoding does not correspond to the sort of Long because it does not
 * consider the sign bit. If Java had an unsigned long type, this encoder would correspond to its sort order.
 *
 * @since 1.6.0
 */
public class ULongLexicoder implements Lexicoder<Long> {

  @Override
  public byte[] encode(Long l) {
    int shift = 56;
    int index;
    int prefix = l < 0 ? 0xff : 0x00;

    for (index = 0; index < 8; index++) {
      if (((l >>> shift) & 0xff) != prefix)
        break;

      shift -= 8;
    }

    byte ret[] = new byte[9 - index];
    ret[0] = (byte) (8 - index);
    for (index = 1; index < ret.length; index++) {
      ret[index] = (byte) (l >>> shift);
      shift -= 8;
    }

    if (l < 0)
      ret[0] = (byte) (16 - ret[0]);

    return ret;

  }

  @Override
  public Long decode(byte[] data) {

    long l = 0;
    int shift = 0;

    if (data[0] < 0 || data[0] > 16)
      throw new IllegalArgumentException("Unexpected length " + (0xff & data[0]));

    for (int i = data.length - 1; i >= 1; i--) {
      l += (data[i] & 0xffl) << shift;
      shift += 8;
    }

    // fill in 0xff prefix
    if (data[0] > 8)
      l |= -1l << ((16 - data[0]) << 3);

    return l;
  }
}

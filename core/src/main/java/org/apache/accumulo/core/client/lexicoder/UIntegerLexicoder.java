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
 * A lexicoder for an unsigned integer. It sorts 0 before -1 and does not preserve the native sort order of a Java integer because Java does not contain an
 * unsigned integer. If Java had an unsigned integer type, this would correspond to its sort order.
 *
 * @since 1.6.0
 */
public class UIntegerLexicoder implements Lexicoder<Integer> {

  @Override
  public byte[] encode(Integer i) {
    int shift = 56;
    int index;
    int prefix = i < 0 ? 0xff : 0x00;

    for (index = 0; index < 4; index++) {
      if (((i >>> shift) & 0xff) != prefix)
        break;

      shift -= 8;
    }

    byte ret[] = new byte[5 - index];
    ret[0] = (byte) (4 - index);
    for (index = 1; index < ret.length; index++) {
      ret[index] = (byte) (i >>> shift);
      shift -= 8;
    }

    if (i < 0)
      ret[0] = (byte) (8 - ret[0]);

    return ret;

  }

  @Override
  public Integer decode(byte[] data) {

    if (data[0] < 0 || data[0] > 8)
      throw new IllegalArgumentException("Unexpected length " + (0xff & data[0]));

    int i = 0;
    int shift = 0;

    for (int idx = data.length - 1; idx >= 1; idx--) {
      i += (data[idx] & 0xffl) << shift;
      shift += 8;
    }

    // fill in 0xff prefix
    if (data[0] > 4)
      i |= -1 << ((8 - data[0]) << 3);

    return i;
  }

}

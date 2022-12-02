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
package org.apache.accumulo.core.client.lexicoder;

/**
 * Unsigned long lexicoder. The lexicographic encoding sorts first 0l and -1l last. This encoding
 * does not correspond to the sort of Long because it does not consider the sign bit. If Java had an
 * unsigned long type, this encoder would correspond to its sort order.
 *
 * @since 1.6.0
 */
public class ULongLexicoder extends AbstractLexicoder<Long> {

  @Override
  public byte[] encode(Long l) {
    int shift = 56;
    int index;
    int prefix = l < 0 ? 0xff : 0x00;

    for (index = 0; index < 8; index++) {
      if (((l >>> shift) & 0xff) != prefix) {
        break;
      }

      shift -= 8;
    }

    byte[] ret = new byte[9 - index];
    ret[0] = (byte) (8 - index);
    for (index = 1; index < ret.length; index++) {
      ret[index] = (byte) (l >>> shift);
      shift -= 8;
    }

    if (l < 0) {
      ret[0] = (byte) (16 - ret[0]);
    }

    return ret;

  }

  @Override
  protected Long decodeUnchecked(byte[] data, int offset, int len) {

    long l = 0;
    int shift = 0;

    if (data[offset] < 0 || data[offset] > 16) {
      throw new IllegalArgumentException("Unexpected length " + (0xff & data[offset]));
    }

    for (int i = (offset + len) - 1; i >= offset + 1; i--) {
      l += (data[i] & 0xffL) << shift;
      shift += 8;
    }

    // fill in 0xff prefix
    if (data[offset] > 8) {
      l |= -1L << ((16 - data[offset]) << 3);
    }

    return l;
  }

  @Override
  public Long decode(byte[] b) {
    // This concrete implementation is provided for binary compatibility, since the corresponding
    // superclass method has type-erased return type Object. See ACCUMULO-3789 and #1285.
    return super.decode(b);
  }
}

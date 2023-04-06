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
package org.apache.accumulo.core.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Preconditions;

public class FastFormat {
  private static final byte[] EMPTY_BYTES = new byte[] {};

  // this 7 to 8 times faster than String.format("%s%06d",prefix, num)
  public static byte[] toZeroPaddedString(long num, int width, int radix, byte[] prefix) {
    Preconditions.checkArgument(num >= 0);
    String strNum = Long.toString(num, radix);
    byte[] ret = new byte[Math.max(strNum.length(), width) + prefix.length];
    if (toZeroPaddedString(ret, 0, strNum, width, prefix) != ret.length) {
      throw new RuntimeException(" Did not format to expected width " + num + " " + width + " "
          + radix + " " + new String(prefix, UTF_8));
    }
    return ret;
  }

  public static byte[] toZeroPaddedHex(long hexadecimal) {
    return toZeroPaddedString(hexadecimal, 16, 16, EMPTY_BYTES);
  }

  public static int toZeroPaddedString(byte[] output, int outputOffset, long num, int width,
      int radix, byte[] prefix) {
    Preconditions.checkArgument(num >= 0);

    String strNum = Long.toString(num, radix);

    return toZeroPaddedString(output, outputOffset, strNum, width, prefix);
  }

  private static int toZeroPaddedString(byte[] output, int outputOffset, String strNum, int width,
      byte[] prefix) {

    int index = outputOffset;

    for (byte b : prefix) {
      output[index++] = b;
    }

    int end = width - strNum.length() + index;

    while (index < end) {
      output[index++] = '0';
    }

    for (int i = 0; i < strNum.length(); i++) {
      output[index++] = (byte) strNum.charAt(i);
    }

    return index - outputOffset;
  }

  /**
   * Create a zero padded string from a hexadecimal number. This is a faster replacement for:
   * String.format("%s%016x%s", PREFIX, tid, SUFFIX);
   */
  public static String toHexString(String prefix, long hexadecimal, String suffix) {
    return prefix + new String(toZeroPaddedString(hexadecimal, 16, 16, EMPTY_BYTES), UTF_8)
        + suffix;
  }

  /**
   * Create a zero padded string from a hexadecimal number. This is a faster replacement for:
   * String.format("%016x", tid)
   */
  public static String toHexString(long hexadecimal) {
    return toHexString("", hexadecimal, "");
  }
}

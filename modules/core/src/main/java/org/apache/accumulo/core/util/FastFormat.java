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
package org.apache.accumulo.core.util;

import static java.nio.charset.StandardCharsets.UTF_8;

public class FastFormat {
  // this 7 to 8 times faster than String.format("%s%06d",prefix, num)
  public static byte[] toZeroPaddedString(long num, int width, int radix, byte[] prefix) {
    byte ret[] = new byte[width + prefix.length];
    if (toZeroPaddedString(ret, 0, num, width, radix, prefix) != ret.length)
      throw new RuntimeException(" Did not format to expected width " + num + " " + width + " " + radix + " " + new String(prefix, UTF_8));
    return ret;
  }

  public static int toZeroPaddedString(byte output[], int outputOffset, long num, int width, int radix, byte[] prefix) {
    if (num < 0)
      throw new IllegalArgumentException();

    String s = Long.toString(num, radix);

    int index = outputOffset;

    for (int i = 0; i < prefix.length; i++) {
      output[index++] = prefix[i];
    }

    int end = width - s.length() + index;

    while (index < end)
      output[index++] = '0';

    for (int i = 0; i < s.length(); i++) {
      output[index++] = (byte) s.charAt(i);
    }

    return index - outputOffset;
  }
}

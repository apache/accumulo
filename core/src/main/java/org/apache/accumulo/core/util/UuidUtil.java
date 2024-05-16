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

public class UuidUtil {
  private static boolean isHex(String s, int offset, int start, int end) {
    for (int i = start; i < end; i++) {
      var c = s.charAt(i + offset);
      boolean isHex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
      if (!isHex) {
        return false;
      }
    }

    return true;
  }

  /**
   * A fast method for verifying a suffix of a string looks like a uuid.
   *
   * @param offset location where the uuid starts. Its expected the uuid occupies the rest of the
   *        string.
   */
  public static boolean isUUID(String uuid, int offset) {
    return uuid.length() - offset == 36 && isHex(uuid, offset, 0, 8)
        && uuid.charAt(8 + offset) == '-' && isHex(uuid, offset, 9, 13)
        && uuid.charAt(13 + offset) == '-' && isHex(uuid, offset, 14, 18)
        && uuid.charAt(18 + offset) == '-' && isHex(uuid, offset, 19, 23)
        && uuid.charAt(23 + offset) == '-' && isHex(uuid, offset, 24, 36);
  }
}

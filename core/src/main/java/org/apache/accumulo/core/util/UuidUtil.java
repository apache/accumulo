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
  /**
   * A fast method for verifying a suffix of a string looks like a uuid.
   *
   * @param offset location where the uuid starts. Its expected the uuid occupies the rest of the
   *        string.
   */
  public static boolean isUUID(String uuid, int offset) {
    if (uuid.length() - offset != 36) {
      return false;
    }
    for (int i = 0; i < 36; i++) {
      var c = uuid.charAt(i + offset);
      if (i == 8 || i == 13 || i == 18 || i == 23) {
        if (c != '-') {
          // expect '-' char at above positions, did not see it
          return false;
        }
      } else if (c < '0' || (c > '9' && c < 'A') || (c > 'F' && c < 'a') || c > 'f') {
        // expected hex at all other positions, did not see hex chars
        return false;
      }
    }
    return true;
  }
}

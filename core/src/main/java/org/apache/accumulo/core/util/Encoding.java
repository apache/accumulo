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

import java.util.Base64;

import org.apache.hadoop.io.Text;

public class Encoding {

  public static String encodeAsBase64FileName(Text data) {
    String encodedRow = Base64.getUrlEncoder().encodeToString(TextUtil.getBytes(data));

    int index = encodedRow.length() - 1;
    while (index >= 0 && encodedRow.charAt(index) == '=') {
      index--;
    }

    encodedRow = encodedRow.substring(0, index + 1);
    return encodedRow;
  }

  public static byte[] decodeBase64FileName(String node) {
    return Base64.getUrlDecoder().decode(node);
  }

}

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
package org.apache.accumulo.core.metadata.schema;

import static com.google.common.hash.Hashing.murmur3_32_fixed;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Strings;

/*
 * A subprefix used to remove sort skew from some of the metadata generated entries, for example: file deletes
 * prefixed with ~del.  NOTE:  This is persisted data so any change to this processing should
 * consider any existing data.
 */
public class SortSkew {

  // A specified length for the skew code used is necessary to parse the key correctly.
  // The Hex code for an integer will always be <= 8
  public static final int SORTSKEW_LENGTH = Integer.BYTES * 2;

  /**
   * Creates a left justified hex string for the path hashcode of a deterministic length, therefore
   * if necessary it is right padded with zeros
   *
   * @param keypart value to be coded
   * @return coded value of keypart
   */
  public static String getCode(String keypart) {
    int hashCode = murmur3_32_fixed().hashString(keypart, UTF_8).asInt();
    return Strings.padStart(Integer.toHexString(hashCode), SORTSKEW_LENGTH, '0');
  }

}

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
package org.apache.accumulo.core.fate;

import java.util.regex.Pattern;

import org.apache.accumulo.core.util.FastFormat;

import com.google.common.base.Preconditions;

public class FateTxId {

  private static final String PREFIX = "FATE[";
  private static final String SUFFIX = "]";

  private final static Pattern PATTERN =
      Pattern.compile(Pattern.quote(PREFIX) + "[0-9a-fA-F]+" + Pattern.quote(SUFFIX));

  private static String getHex(String fmtTid) {
    return fmtTid.substring(PREFIX.length(), fmtTid.length() - SUFFIX.length());
  }

  /**
   * @return true if string was created by {@link #formatTid(long)} and false otherwise.
   */
  public static boolean isFormatedTid(String fmtTid) {
    return PATTERN.matcher(fmtTid).matches();
  }

  /**
   * Reverses {@link #formatTid(long)}
   */
  public static long fromString(String fmtTid) {
    Preconditions.checkArgument(fmtTid.startsWith(PREFIX) && fmtTid.endsWith(SUFFIX));
    return Long.parseLong(getHex(fmtTid), 16);
  }

  /**
   * Formats transaction ids in a consistent way that is useful for logging and persisting.
   */
  public static String formatTid(long tid) {
    // do not change how this formats without considering implications for persistence
    return FastFormat.toHexString(PREFIX, tid, SUFFIX);
  }

  public static long parseTidFromUserInput(String s) {
    if (isFormatedTid(s)) {
      return fromString(s);
    } else {
      return Long.parseLong(s, 16);
    }
  }

}

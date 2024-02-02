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

import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.manager.thrift.TFateId;
import org.apache.accumulo.core.util.FastFormat;

/**
 * A strongly typed FATE Transaction ID. This is used to uniquely identify a FATE transaction.
 * Consists of its {@link FateInstanceType} and its transaction id (long). The canonical string is
 * of the form "FATE:[FateInstanceType]:[hex long tid]" (without the brackets).
 */
public class FateId extends AbstractId<FateId> {

  private static final long serialVersionUID = 1L;
  private static final String PREFIX = "FATE:";
  private static final Pattern HEX_PATTERN = Pattern.compile("^[0-9a-fA-F]+$");

  private FateId(String canonical) {
    super(canonical);
  }

  /**
   * @return the {@link FateInstanceType}
   */
  public FateInstanceType getType() {
    return FateInstanceType.valueOf(canonical().split(":")[1]);
  }

  /**
   * @return the decimal value of the transaction id
   */
  public long getTid() {
    return Long.parseLong(getHexTid(), 16);
  }

  /**
   * @return the hexadecimal value of the transaction id
   */
  public String getHexTid() {
    return canonical().split(":")[2];
  }

  /**
   * Creates a new FateId object from the given parameters
   *
   * @param type the {@link FateInstanceType}
   * @param tid the decimal transaction id
   * @return a new FateId object
   */
  public static FateId from(FateInstanceType type, long tid) {
    return new FateId(PREFIX + type + ":" + formatTid(tid));
  }

  /**
   * Creates a new FateId object from the given parameters
   *
   * @param type the {@link FateInstanceType}
   * @param hexTid the hexadecimal transaction id
   * @return a new FateId object
   */
  public static FateId from(FateInstanceType type, String hexTid) {
    if (HEX_PATTERN.matcher(hexTid).matches()) {
      return new FateId(PREFIX + type + ":" + hexTid);
    } else {
      throw new IllegalArgumentException("Invalid Hex Transaction ID: " + hexTid);
    }
  }

  public static FateId fromThrift(TFateId tFateId) {
    FateInstanceType type;
    long tid = tFateId.getTid();

    switch (tFateId.getType()) {
      case USER:
        type = FateInstanceType.USER;
        break;
      case META:
        type = FateInstanceType.META;
        break;
      default:
        throw new IllegalArgumentException("Invalid TFateInstanceType: " + tFateId.getType());
    }

    return new FateId(PREFIX + type + ":" + formatTid(tid));
  }

  /**
   * Formats transaction ids in a consistent way that is useful for logging and persisting.
   */
  public static String formatTid(long tid) {
    // do not change how this formats without considering implications for persistence
    return FastFormat.toHexString(tid);
  }
}

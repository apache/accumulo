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

import static org.apache.accumulo.core.util.UuidUtil.isUUID;

import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.manager.thrift.TFateId;
import org.apache.accumulo.core.manager.thrift.TFateInstanceType;

import com.google.common.base.Preconditions;

/**
 * A strongly typed FATE Transaction ID. This is used to uniquely identify a FATE transaction.
 * Consists of its {@link FateInstanceType} and its transaction {@link UUID}. The canonical string
 * is of the form "FATE:[FateInstanceType]:[UUID]" (without the brackets).
 */
public class FateId extends AbstractId<FateId> {

  private static final long serialVersionUID = 1L;
  private static final String PREFIX = "FATE:";

  private static final String META_PREFIX = FateInstanceType.META.name() + ":";
  private static final String USER_PREFIX = FateInstanceType.USER.name() + ":";

  static {
    // Validate the assumptions made by this class
    Preconditions.checkState(Set.of(FateInstanceType.values())
        .equals(Set.of(FateInstanceType.USER, FateInstanceType.META)));
    Preconditions.checkState(META_PREFIX.length() == USER_PREFIX.length());
  }

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
   * @return the transaction {@link UUID}
   */
  public UUID getTxUUID() {
    return UUID.fromString(getTxUUIDStr());
  }

  /**
   * @return the transaction {@link UUID} as a String
   */
  public String getTxUUIDStr() {
    return canonical().split(":")[2];
  }

  /**
   * Creates a new FateId object from the given parameters
   *
   * @param type the {@link FateInstanceType}
   * @param txUUID the {@link UUID}
   * @return a new FateId object
   */
  public static FateId from(FateInstanceType type, UUID txUUID) {
    return new FateId(PREFIX + type + ":" + txUUID);
  }

  /**
   * Creates a new FateId object from the given parameters
   *
   * @param type the {@link FateInstanceType}
   * @param txUUIDStr the transaction {@link UUID} as a String
   * @return a new FateId object
   */
  public static FateId from(FateInstanceType type, String txUUIDStr) {
    if (isUUID(txUUIDStr, 0)) {
      return new FateId(PREFIX + type + ":" + txUUIDStr);
    } else {
      throw new IllegalArgumentException("Invalid Transaction UUID: " + txUUIDStr);
    }
  }

  /**
   * @param fateIdStr the string representation of the FateId
   * @return a new FateId object from the given string
   */
  public static FateId from(String fateIdStr) {
    if (isFateId(fateIdStr)) {
      return new FateId(fateIdStr);
    } else {
      throw new IllegalArgumentException("Invalid Fate ID: " + fateIdStr);
    }
  }

  /**
   * @param fateIdStr the string representation of the FateId
   * @return true if the string is a valid FateId, false otherwise
   */
  public static boolean isFateId(String fateIdStr) {

    if (!fateIdStr.startsWith(PREFIX)) {
      return false;
    }

    if (!fateIdStr.startsWith(META_PREFIX, PREFIX.length())
        && !fateIdStr.startsWith(USER_PREFIX, PREFIX.length())) {
      return false;
    }

    // assuming that META_PREFIX and USER_PREFIX are the same length, so UUID always starts a same
    // place
    return isUUID(fateIdStr, PREFIX.length() + META_PREFIX.length());
  }

  /**
   * @param tFateId the TFateId
   * @return the FateId equivalent of the given TFateId
   */
  public static FateId fromThrift(TFateId tFateId) {
    FateInstanceType type;
    String txUUIDStr = tFateId.getTxUUIDStr();

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

    if (isUUID(txUUIDStr, 0)) {
      return new FateId(PREFIX + type + ":" + txUUIDStr);
    } else {
      throw new IllegalArgumentException("Invalid Transaction UUID: " + txUUIDStr);
    }
  }

  /**
   *
   * @return the TFateId equivalent of the FateId
   */
  public TFateId toThrift() {
    TFateInstanceType thriftType;
    FateInstanceType type = getType();
    switch (type) {
      case USER:
        thriftType = TFateInstanceType.USER;
        break;
      case META:
        thriftType = TFateInstanceType.META;
        break;
      default:
        throw new IllegalArgumentException("Invalid FateInstanceType: " + type);
    }
    return new TFateId(thriftType, getTxUUIDStr());
  }
}

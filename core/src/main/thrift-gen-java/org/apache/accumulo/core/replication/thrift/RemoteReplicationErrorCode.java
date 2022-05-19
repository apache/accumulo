/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.16.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.replication.thrift;


public enum RemoteReplicationErrorCode implements org.apache.thrift.TEnum {
  COULD_NOT_DESERIALIZE(0),
  COULD_NOT_APPLY(1),
  TABLE_DOES_NOT_EXIST(2),
  CANNOT_AUTHENTICATE(3),
  CANNOT_INSTANTIATE_REPLAYER(4);

  private final int value;

  private RemoteReplicationErrorCode(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  @org.apache.thrift.annotation.Nullable
  public static RemoteReplicationErrorCode findByValue(int value) { 
    switch (value) {
      case 0:
        return COULD_NOT_DESERIALIZE;
      case 1:
        return COULD_NOT_APPLY;
      case 2:
        return TABLE_DOES_NOT_EXIST;
      case 3:
        return CANNOT_AUTHENTICATE;
      case 4:
        return CANNOT_INSTANTIATE_REPLAYER;
      default:
        return null;
    }
  }
}

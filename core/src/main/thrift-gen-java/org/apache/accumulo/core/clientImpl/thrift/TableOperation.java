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
package org.apache.accumulo.core.clientImpl.thrift;


public enum TableOperation implements org.apache.thrift.TEnum {
  CREATE(0),
  DELETE(1),
  RENAME(2),
  SET_PROPERTY(3),
  REMOVE_PROPERTY(4),
  OFFLINE(5),
  ONLINE(6),
  FLUSH(7),
  PERMISSION(8),
  CLONE(9),
  MERGE(10),
  DELETE_RANGE(11),
  BULK_IMPORT(12),
  COMPACT(13),
  IMPORT(14),
  EXPORT(15),
  COMPACT_CANCEL(16);

  private final int value;

  private TableOperation(int value) {
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
  public static TableOperation findByValue(int value) { 
    switch (value) {
      case 0:
        return CREATE;
      case 1:
        return DELETE;
      case 2:
        return RENAME;
      case 3:
        return SET_PROPERTY;
      case 4:
        return REMOVE_PROPERTY;
      case 5:
        return OFFLINE;
      case 6:
        return ONLINE;
      case 7:
        return FLUSH;
      case 8:
        return PERMISSION;
      case 9:
        return CLONE;
      case 10:
        return MERGE;
      case 11:
        return DELETE_RANGE;
      case 12:
        return BULK_IMPORT;
      case 13:
        return COMPACT;
      case 14:
        return IMPORT;
      case 15:
        return EXPORT;
      case 16:
        return COMPACT_CANCEL;
      default:
        return null;
    }
  }
}

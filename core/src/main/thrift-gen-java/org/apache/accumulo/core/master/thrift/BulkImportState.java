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
/**
 * Autogenerated by Thrift Compiler (0.17.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.master.thrift;


public enum BulkImportState implements org.apache.thrift.TEnum {
  INITIAL(0),
  MOVING(1),
  PROCESSING(2),
  ASSIGNING(3),
  LOADING(4),
  COPY_FILES(5),
  CLEANUP(6);

  private final int value;

  private BulkImportState(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  @Override
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  @org.apache.thrift.annotation.Nullable
  public static BulkImportState findByValue(int value) { 
    switch (value) {
      case 0:
        return INITIAL;
      case 1:
        return MOVING;
      case 2:
        return PROCESSING;
      case 3:
        return ASSIGNING;
      case 4:
        return LOADING;
      case 5:
        return COPY_FILES;
      case 6:
        return CLEANUP;
      default:
        return null;
    }
  }
}

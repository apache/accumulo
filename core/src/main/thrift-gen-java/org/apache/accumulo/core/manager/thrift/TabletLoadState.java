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
 * Autogenerated by Thrift Compiler (0.15.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.manager.thrift;


public enum TabletLoadState implements org.apache.thrift.TEnum {
  LOADED(0),
  LOAD_FAILURE(1),
  UNLOADED(2),
  UNLOAD_FAILURE_NOT_SERVING(3),
  UNLOAD_ERROR(4),
  CHOPPED(5);

  private final int value;

  private TabletLoadState(int value) {
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
  public static TabletLoadState findByValue(int value) { 
    switch (value) {
      case 0:
        return LOADED;
      case 1:
        return LOAD_FAILURE;
      case 2:
        return UNLOADED;
      case 3:
        return UNLOAD_FAILURE_NOT_SERVING;
      case 4:
        return UNLOAD_ERROR;
      case 5:
        return CHOPPED;
      default:
        return null;
    }
  }
}

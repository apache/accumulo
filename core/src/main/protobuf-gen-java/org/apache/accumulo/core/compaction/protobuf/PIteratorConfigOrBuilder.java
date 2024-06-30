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
// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: compaction-coordinator.proto

// Protobuf Java Version: 3.25.3
package org.apache.accumulo.core.compaction.protobuf;

public interface PIteratorConfigOrBuilder extends
    // @@protoc_insertion_point(interface_extends:compaction_coordinator.PIteratorConfig)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>repeated .compaction_coordinator.PIteratorSetting iterators = 1;</code>
   */
  java.util.List<org.apache.accumulo.core.compaction.protobuf.PIteratorSetting> 
      getIteratorsList();
  /**
   * <code>repeated .compaction_coordinator.PIteratorSetting iterators = 1;</code>
   */
  org.apache.accumulo.core.compaction.protobuf.PIteratorSetting getIterators(int index);
  /**
   * <code>repeated .compaction_coordinator.PIteratorSetting iterators = 1;</code>
   */
  int getIteratorsCount();
  /**
   * <code>repeated .compaction_coordinator.PIteratorSetting iterators = 1;</code>
   */
  java.util.List<? extends org.apache.accumulo.core.compaction.protobuf.PIteratorSettingOrBuilder> 
      getIteratorsOrBuilderList();
  /**
   * <code>repeated .compaction_coordinator.PIteratorSetting iterators = 1;</code>
   */
  org.apache.accumulo.core.compaction.protobuf.PIteratorSettingOrBuilder getIteratorsOrBuilder(
      int index);
}

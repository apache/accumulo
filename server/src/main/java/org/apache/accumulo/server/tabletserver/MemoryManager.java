/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.tabletserver;

import java.util.List;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.server.conf.ServerConfiguration;

/**
 * A MemoryManager in accumulo currently determines when minor compactions should occur and when ingest should be put on hold. The goal of a memory manager
 * implementation is to maximize ingest throughput and minimize the number of minor compactions.
 *
 *
 *
 */

public interface MemoryManager {

  /**
   * Initialize the memory manager.
   */
  void init(ServerConfiguration conf);

  /**
   * An implementation of this function will be called periodically by accumulo and should return a list of tablets to minor compact.
   *
   * Instructing a tablet that is already minor compacting (this can be inferred from the TabletState) to minor compact has no effect.
   *
   * Holding all ingest does not affect metadata tablets.
   */

  MemoryManagementActions getMemoryManagementActions(List<TabletState> tablets);

  /**
   * This method is called when a tablet is closed. A memory manger can clean up any per tablet state it is keeping when this is called.
   */
  void tabletClosed(KeyExtent extent);
}

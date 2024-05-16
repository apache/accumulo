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
package org.apache.accumulo.core.client.admin.compaction;

import java.util.Date;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.TabletInformation;

/**
 * Provides information about a table compaction started using
 * {@link org.apache.accumulo.core.client.admin.TableOperations#compact(String, CompactionConfig)}
 *
 * @since 4.0.0
 */
public interface TableCompaction {

  /**
   * @return the unique identifier for this table compaction
   */
  TableCompactionId getId();

  /**
   * @return the time this table compaction was initiated
   */
  Date getStartTime();

  /**
   * @return the configuration used to initiate this table compaction.
   */
  CompactionConfig getConfig();

  /**
   * @return information about tablets involved in this table compaction
   */
  Stream<TabletInformation> getTablets();

  /**
   * @return information about tablet compactions that are currently running on behalf of this table
   *         compaction.
   */
  Stream<ActiveCompaction> getRunningCompactions();

}

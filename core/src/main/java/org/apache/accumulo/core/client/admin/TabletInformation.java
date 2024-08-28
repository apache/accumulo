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
package org.apache.accumulo.core.client.admin;

import java.util.Collection;
import java.util.Optional;

import org.apache.accumulo.core.client.admin.compaction.ActiveCompactionId;
import org.apache.accumulo.core.client.admin.compaction.TableCompactionId;
import org.apache.accumulo.core.data.TabletId;

/**
 * @since 4.0.0
 */
public interface TabletInformation {

  /**
   * @return the TabletId for this tablet.
   */
  TabletId getTabletId();

  /**
   * @return the number of files in the tablet directory.
   */
  int getNumFiles();

  /**
   * @return the number of write-ahead logs associated with the tablet.
   */
  int getNumWalLogs();

  /**
   * @return an estimated number of entries in the tablet.
   */
  long getEstimatedEntries();

  /**
   * @return an estimated size of the tablet data on disk, which is likely the compressed size of
   *         the data.
   */
  long getEstimatedSize();

  /**
   * @return the tablet hosting state.
   */
  String getTabletState();

  /**
   * @return the Location of the tablet as a String.
   */
  Optional<String> getLocation();

  /**
   * @return the directory name of the tablet.
   */
  String getTabletDir();

  /**
   * @return the TabletAvailability object.
   */
  TabletAvailability getTabletAvailability();

  interface RunningCompactionInformation {
    /**
     * @return an id that uniquely identifies a running compaction
     */
    ActiveCompactionId getId();

    /**
     * If this tablet compaction was initiated as part of a table compaction, then return the id of
     * that table compaction.
     */
    Optional<TableCompactionId> getTableCompactionId();
  }

  /**
   * @return information about compactions that are currently running against this tablet
   */
  Collection<RunningCompactionInformation> getRunningCompactions();

  /**
   * @return active table compaction operations that have run to completion against this tablet.
   *         Once a table compaction completes for all tablets then it will no longer show up here.
   */
  Collection<TableCompactionId> getCompletedTableCompactions();
}

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
package org.apache.accumulo.core.spi.compaction;

import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

/**
 * A CompactionFinalizer is used by the CompactionCoordinator to update the state of a compaction
 * running on a Compactor.
 *
 * @since 2.1.4
 */
public interface CompactionFinalizer {

  /**
   * This interface exists so the API can evolve and additional parameters can be passed to the
   * method in the future.
   *
   * @since 2.1.4
   */
  public interface InitParameters {

    ServiceEnvironment getServiceEnvironment();

    ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor();
  }

  /**
   * Initialize the CompactionFinalizer
   *
   * @param params initialization parameters
   */
  void init(InitParameters params);

  /**
   * Called by the CompactionCoordinator when the compaction has completed successfully.
   * Implementations will create an ExternalCompactionFinalState object from the input arguments
   * with a FinalState of FINISHED, and insert it into the metadata table.
   *
   * @param ecid ExternalCompactionId
   * @param extent KeyExtent
   * @param fileSize size of newly compacted file
   * @param fileEntries number of entries in the newly compacted file
   */
  void commitCompaction(String ecid, TabletId extent, long fileSize, long fileEntries);

  /**
   * Called by the CompactionCoordinator when compactions have failed. Implementations will create
   * ExternalCompactionFinalState objects from the input arguments with a FinalState of FAILED and
   * zero for the file size and number of entries. The implementation will insert the
   * ExternalCompactionFinalState objects into the metadata table.
   *
   * @param compactionsToFail map of ExternalCompactionId to KeyExtent
   */
  void failCompactions(Map<String,TabletId> compactionsToFail);

}

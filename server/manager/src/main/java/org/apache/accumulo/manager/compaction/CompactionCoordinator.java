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
package org.apache.accumulo.manager.compaction;

import java.util.Collection;

import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.compaction.CompactionJob;

/**
 * This interface exposes the compaction coordination functionality to other parts of the manager.
 */
public interface CompactionCoordinator extends MetricsProducer {

  /**
   * Adds compaction jobs for a tablet. Any jobs that are currently queued for the specified tablet
   * are removed when these are added.
   */
  void addJobs(TabletMetadata tabletMetadata, Collection<CompactionJob> jobs);

  /**
   * @return a thrift service that can handle compaction coordination RPC messages
   */
  CompactionCoordinatorService.Iface getThriftService();

  /**
   * Starts any threads the compaction coordinator service needs to function.
   */
  void start();

  /**
   * Stops any threads started by {@link #start()}
   */
  void shutdown();
}

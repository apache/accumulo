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

import java.util.Collection;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner.PlanningParameters;

/**
 * The return value of {@link CompactionPlanner#makePlan(PlanningParameters)} that is created using
 * {@link PlanningParameters#createPlanBuilder()}
 *
 * @since 2.1.0
 * @see CompactionPlanner
 * @see org.apache.accumulo.core.spi.compaction
 */
public interface CompactionPlan {

  /**
   * @since 2.1.0
   * @see PlanningParameters#createPlanBuilder()
   */
  interface Builder {
    /**
     * @param priority This determines the order in which the job is taken off the execution queue.
     *        Larger numbers are taken off the queue first. If two jobs are on the queue, one with a
     *        priority of 4 and another with 5, then the one with 5 will be taken first.
     * @param executor Where the job should run.
     * @param group The files to compact.
     * @return this
     */
    Builder addJob(short priority, CompactionExecutorId executor,
        Collection<CompactableFile> group);

    CompactionPlan build();
  }

  /**
   * Return the set of jobs this plan will submit for compaction.
   */
  Collection<CompactionJob> getJobs();
}

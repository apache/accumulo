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
import java.util.Map;

import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

/**
 * Plans compaction work for a compaction service.
 *
 * @since 2.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */
public interface CompactionPlanner {

  /**
   * This interface exists so the API can evolve and additional parameters can be passed to the
   * method in the future.
   *
   * @since 2.1.0
   */
  public interface InitParameters {
    ServiceEnvironment getServiceEnvironment();

    /**
     * @return The configured options. For example if the system properties
     *         {@code tserver.compaction.major.service.s1.planner.opts.p1=abc} and
     *         {@code tserver.compaction.major.service.s1.planner.opts.p9=123} were set, then this
     *         map would contain {@code p1=abc} and {@code p9=123}. In this example {@code s1} is
     *         the identifier for the compaction service. Each compaction service has a single
     *         planner.
     */
    Map<String,String> getOptions();

    /**
     * @return For a given key from the map returned by {@link #getOptions()} determines the fully
     *         qualified tablet property for that key. For example if a planner was being
     *         initialized for compaction service {@code CS9} and this method were passed
     *         {@code prop1} then it would return
     *         {@code tserver.compaction.major.service.CS9.planner.opts.prop1}.
     */
    String getFullyQualifiedOption(String key);

    /**
     * @return an execution manager that can be used to created thread pools within a compaction
     *         service.
     */
    ExecutorManager getExecutorManager();
  }

  public void init(InitParameters params);

  /**
   * This interface exists so the API can evolve and additional parameters can be passed to the
   * method in the future.
   *
   * @since 2.1.0
   */
  public interface PlanningParameters {

    /**
     * @return The id of the table that compactions are being planned for.
     * @see ServiceEnvironment#getTableName(TableId)
     */
    TableId getTableId();

    ServiceEnvironment getServiceEnvironment();

    CompactionKind getKind();

    /**
     * @return the compaction ratio configured for the table
     */
    double getRatio();

    /**
     * @return the set of all files a tablet has.
     */
    Collection<CompactableFile> getAll();

    /**
     * @return the set of files that could be compacted depending on what {@link #getKind()}
     *         returns.
     */
    Collection<CompactableFile> getCandidates();

    /**
     * @return jobs that are currently running
     */
    Collection<CompactionJob> getRunningCompactions();

    /**
     * @return For a user compaction (when {@link #getKind()} returns {@link CompactionKind#USER})
     *         where the user set execution hints via
     *         {@link CompactionConfig#setExecutionHints(Map)} this will return those hints.
     *         Otherwise this will return an immutable empty map.
     */
    Map<String,String> getExecutionHints();

    /**
     * @return A compaction plan builder that must be used to create a compaction plan.
     */
    CompactionPlan.Builder createPlanBuilder();
  }

  /**
   * <p>
   * Plan what work a compaction service should do. The kind of compaction returned by
   * {@link PlanningParameters#getKind()} determines what must be done with the files returned by
   * {@link PlanningParameters#getCandidates()}. The following are the expectations for the
   * candidates for each kind.
   *
   * <ul>
   * <li>CompactionKind.SYSTEM The planner is not required to do anything with the candidates and
   * can choose to compact zero or more of them. The candidates may represent a subset of all the
   * files in the case where a user compaction is in progress or other compactions are running.
   * <li>CompactionKind.USER and CompactionKind.SELECTED. The planner is required to eventually
   * compact all candidates. Its ok to return a compaction plan that compacts a subset. When the
   * planner compacts a subset, it will eventually be called again later. When it is called later
   * the candidates will contain the files it did not compact and the results of any previous
   * compactions it scheduled. The planner must eventually compact all of the files in the candidate
   * set down to a single file. The compaction service will keep calling the planner until it does.
   * <li>CompactionKind.CHOP. The planner is required to eventually compact all candidates. One
   * major difference with USER compactions is this kind is not required to compact all files to a
   * single file. It is ok to return a compaction plan that compacts a subset of the candidates.
   * When the planner compacts a subset, it will eventually be called later. When it is called later
   * the candidates will contain the files it did not compact.
   * </ul>
   *
   * <p>
   * For a chop compaction assume the following happens.
   * <ol>
   * <li>The candidate set passed to makePlan contains the files {@code [F1,F2,F3,F4]} and kind is
   * CHOP
   * <li>The planner returns a job to compact files {@code [F1,F2]} on executor E1
   * <li>The compaction runs compacting {@code [F1,F2]} into file {@code [F5]}
   * </ol>
   *
   * <p>
   * For the case above, eventually the planner will called again with a candidate set of
   * {@code [F3,F4]} and it must eventually compact those two files.
   *
   * <p>
   * For a user and selector compaction assume the same thing happens, it will result in a slightly
   * different outcome.
   * <ol>
   * <li>The candidate set passed to makePlan contains the files {@code [F1,F2,F3,F4]} and kind is
   * USER
   * <li>The planner returns a job to compact files {@code [F1,F2]} on executor E1
   * <li>The compaction runs compacting {@code [F1,F2]} into file {@code [F5]}
   * </ol>
   *
   * <p>
   * For the case above, eventually the planner will called again with a candidate set of
   * {@code [F3,F4,F5]} and it must eventually compact those three files to one. The difference with
   * CHOP compactions is that the result of intermediate compactions are included in the candidate
   * set.
   *
   * <p>
   * When a planner returns a compactions plan, task will be queued on executors. Previously queued
   * task that do not match the latest plan are removed. The planner is called periodically,
   * whenever a new file is added, and whenever a compaction finishes.
   *
   * <p>
   * Use {@link PlanningParameters#createPlanBuilder()} to build the plan this function returns.
   *
   * @see CompactionKind
   */
  CompactionPlan makePlan(PlanningParameters params);
}

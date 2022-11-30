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
package org.apache.accumulo.tserver.compaction;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;

/**
 * The interface for customizing major compactions.
 * <p>
 * The tablet server has one thread to ask many tablets if they should compact. When the strategy
 * returns true, then tablet is added to the queue of tablets waiting for a compaction thread. Once
 * a thread is available, the {@link #gatherInformation(MajorCompactionRequest)} method is called
 * outside the tablets' lock. This gives the strategy the ability to read information that maybe
 * expensive to fetch. Once the gatherInformation returns, the tablet lock is grabbed and the
 * compactionPlan computed. This should *not* do expensive operations, especially not I/O. Note that
 * the number of files may change between calls to
 * {@link #gatherInformation(MajorCompactionRequest)} and
 * {@link #getCompactionPlan(MajorCompactionRequest)}.
 * <p>
 * <b>Note:</b> the strategy object used for the {@link #shouldCompact(MajorCompactionRequest)} call
 * is going to be different from the one used in the compaction thread.
 *
 * @deprecated since 2.1.0 use {@link CompactionSelector}, {@link CompactionConfigurer}, and
 *             {@link CompactionPlanner} instead. See
 *             {@link org.apache.accumulo.core.client.admin.CompactionStrategyConfig} for more
 *             information about why this was deprecated.
 * @see org.apache.accumulo.core.spi.compaction
 */
// Eclipse might show @SuppressWarnings("removal") as unnecessary.
// Eclipse is wrong. See https://bugs.eclipse.org/bugs/show_bug.cgi?id=565271
@SuppressWarnings("removal")
@Deprecated(since = "2.1.0", forRemoval = true)
public abstract class CompactionStrategy {
  /**
   * The settings for the compaction strategy pulled from zookeeper. The
   * <code>table.compacations.major.strategy.opts</code> part of the setting will be removed.
   */
  public void init(Map<String,String> options) {}

  /**
   * Determine if this tablet is eligible for a major compaction. It's ok if it later determines
   * (through {@link #gatherInformation(MajorCompactionRequest)} and
   * {@link #getCompactionPlan(MajorCompactionRequest)}) that it does not need to. Any state stored
   * during shouldCompact will no longer exist when
   * {@link #gatherInformation(MajorCompactionRequest)} and
   * {@link #getCompactionPlan(MajorCompactionRequest)} are called.
   *
   * <p>
   * Called while holding the tablet lock, so it should not be doing any blocking.
   *
   * <p>
   * Since no blocking should be done in this method, then its unexpected that this method will
   * throw IOException. However since its in the API, it can not be easily removed.
   */
  public abstract boolean shouldCompact(MajorCompactionRequest request) throws IOException;

  /**
   * Called prior to obtaining the tablet lock, useful for examining metadata or indexes. State
   * collected during this method will be available during the call the
   * {@link #getCompactionPlan(MajorCompactionRequest)}.
   *
   * @param request basic details about the tablet
   */
  public void gatherInformation(MajorCompactionRequest request) throws IOException {}

  /**
   * Get the plan for compacting a tablets files. Called while holding the tablet lock, so it should
   * not be doing any blocking.
   *
   * <p>
   * Since no blocking should be done in this method, then its unexpected that this method will
   * throw IOException. However since its in the API, it can not be easily removed.
   *
   * @param request basic details about the tablet
   * @return the plan for a major compaction, or null to cancel the compaction.
   */
  public abstract CompactionPlan getCompactionPlan(MajorCompactionRequest request)
      throws IOException;

}

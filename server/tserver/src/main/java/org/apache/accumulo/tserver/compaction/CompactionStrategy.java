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
package org.apache.accumulo.tserver.compaction;

import java.io.IOException;
import java.util.Map;

/**
 * The interface for customizing major compactions.
 * <p>
 * The tablet server has one thread to ask many tablets if they should compact. When the strategy returns true, then tablet is added to the queue of tablets
 * waiting for a compaction thread. Once a thread is available, the {@link #gatherInformation(MajorCompactionRequest)} method is called outside the tablets'
 * lock. This gives the strategy the ability to read information that maybe expensive to fetch. Once the gatherInformation returns, the tablet lock is grabbed
 * and the compactionPlan computed. This should *not* do expensive operations, especially not I/O. Note that the number of files may change between calls to
 * {@link #gatherInformation(MajorCompactionRequest)} and {@link #getCompactionPlan(MajorCompactionRequest)}.
 * <p>
 * <b>Note:</b> the strategy object used for the {@link #shouldCompact(MajorCompactionRequest)} call is going to be different from the one used in the
 * compaction thread.
 */
public abstract class CompactionStrategy {

  /**
   * The settings for the compaction strategy pulled from zookeeper. The <tt>table.compacations.major.strategy.opts</tt> part of the setting will be removed.
   */
  public void init(Map<String,String> options) {}

  /**
   * Determine if this tablet is eligible for a major compaction. It's ok if it later determines (through {@link #gatherInformation(MajorCompactionRequest)} and
   * {@link #getCompactionPlan(MajorCompactionRequest)}) that it does not need to. Any state stored during shouldCompact will no longer exist when
   * {@link #gatherInformation(MajorCompactionRequest)} and {@link #getCompactionPlan(MajorCompactionRequest)} are called.
   *
   */
  public abstract boolean shouldCompact(MajorCompactionRequest request) throws IOException;

  /**
   * Called prior to obtaining the tablet lock, useful for examining metadata or indexes. State collected during this method will be available during the call
   * the {@link #getCompactionPlan(MajorCompactionRequest)}.
   *
   * @param request
   *          basic details about the tablet
   */
  public void gatherInformation(MajorCompactionRequest request) throws IOException {}

  /**
   * Get the plan for compacting a tablets files. Called while holding the tablet lock, so it should not be doing any blocking.
   *
   * @param request
   *          basic details about the tablet
   * @return the plan for a major compaction, or null to cancel the compaction.
   */
  abstract public CompactionPlan getCompactionPlan(MajorCompactionRequest request) throws IOException;

}

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
package org.apache.accumulo.tserver.compactions;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.slf4j.LoggerFactory;

/**
 * A compaction planner that makes no plans and is intended to be used temporarily when a compaction
 * service has no compaction planner because it is misconfigured.
 */
public class ProvisionalCompactionPlanner implements CompactionPlanner {

  private final CompactionServiceId serviceId;
  private AtomicLong lastWarnNanoTime = new AtomicLong(System.nanoTime());

  public ProvisionalCompactionPlanner(CompactionServiceId serviceId) {
    this.serviceId = serviceId;
  }

  @Override
  public void init(InitParameters params) {

  }

  @Override
  public CompactionPlan makePlan(PlanningParameters params) {
    var nanoTime = System.nanoTime();
    var updatedTime = lastWarnNanoTime.updateAndGet(last -> {
      if (nanoTime - last > TimeUnit.MINUTES.toNanos(5)) {
        return nanoTime;
      }

      return last;
    });

    if (updatedTime == nanoTime) {
      LoggerFactory.getLogger(ProvisionalCompactionPlanner.class)
          .error("The compaction service "
              + "'{}' is currently disabled, likely because it has bad configuration. No "
              + "compactions will occur on this service until it is fixed.", serviceId);
    }

    return params.createPlanBuilder().build();
  }
}

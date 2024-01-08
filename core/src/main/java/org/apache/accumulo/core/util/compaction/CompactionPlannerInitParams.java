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
package org.apache.accumulo.core.util.compaction;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.spi.compaction.GroupManager;

import com.google.common.base.Preconditions;

public class CompactionPlannerInitParams implements CompactionPlanner.InitParameters {
  private final Map<String,String> plannerOpts;
  private final Set<CompactorGroupId> requestedGroups;
  private final ServiceEnvironment senv;
  private final CompactionServiceId serviceId;
  private final String prefix;

  public CompactionPlannerInitParams(CompactionServiceId serviceId, String prefix,
      Map<String,String> plannerOpts, ServiceEnvironment senv) {
    this.serviceId = serviceId;
    this.plannerOpts = plannerOpts;
    this.requestedGroups = new HashSet<>();
    this.senv = senv;
    this.prefix = prefix;
  }

  @Override
  public ServiceEnvironment getServiceEnvironment() {
    return senv;
  }

  @Override
  public Map<String,String> getOptions() {
    return plannerOpts;
  }

  @Override
  public String getFullyQualifiedOption(String key) {
    return prefix + serviceId + ".planner.opts." + key;
  }

  @Override
  public GroupManager getGroupManager() {
    return new GroupManager() {

      @Override
      public CompactorGroupId getGroup(String name) {
        var cgid = CompactorGroupIdImpl.groupId(name);
        Preconditions.checkArgument(!getRequestedGroups().contains(cgid),
            "Duplicate compactor group for group: " + name);
        getRequestedGroups().add(cgid);
        return cgid;
      }
    };
  }

  public Set<CompactorGroupId> getRequestedGroups() {
    return requestedGroups;
  }
}

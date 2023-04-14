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
package org.apache.accumulo.core.clientImpl;

import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.HostingColumnFamily;
import org.apache.accumulo.core.tablet.thrift.THostingGoal;

public enum TabletHostingGoalImpl {

  ALWAYS, NEVER, ONDEMAND;

  public static TabletHostingGoalImpl fromTabletHostingGoal(TabletHostingGoal goal) {
    switch (goal) {
      case ALWAYS:
        return ALWAYS;
      case ONDEMAND:
        return ONDEMAND;
      case NEVER:
        return NEVER;
      default:
        throw new IllegalArgumentException("Unhandled goal: " + goal);
    }
  }

  public static TabletHostingGoalImpl fromThrift(THostingGoal goal) {
    switch (goal) {
      case ALWAYS:
        return ALWAYS;
      case NEVER:
        return NEVER;
      case ONDEMAND:
        return ONDEMAND;
      default:
        throw new IllegalArgumentException("Unhandled value for THostingGoal: " + goal);
    }
  }

  public THostingGoal toThrift() {
    switch (this) {
      case ALWAYS:
        return THostingGoal.ALWAYS;
      case NEVER:
        return THostingGoal.NEVER;
      case ONDEMAND:
        return THostingGoal.ONDEMAND;
      default:
        throw new IllegalArgumentException("Unhandled enum value");
    }
  }

  public static TabletHostingGoalImpl fromValue(Value value) {
    switch (value.toString()) {
      case "ALWAYS":
        return ALWAYS;
      case "NEVER":
        return NEVER;
      case "ONDEMAND":
        return ONDEMAND;
      default:
        throw new IllegalArgumentException("Invalid value for hosting goal: " + value.toString());
    }
  }

  public Value toValue() {
    switch (this) {
      case ALWAYS:
        return new Value(HostingColumnFamily.ALWAYS_GOAL);
      case NEVER:
        return new Value(HostingColumnFamily.NEVER_GOAL);
      case ONDEMAND:
        return new Value(HostingColumnFamily.ONDEMAND_GOAL);
      default:
        throw new IllegalArgumentException("Unhandled enum value");
    }
  }

}

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

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.HostingGoalColumnFamily;
import org.apache.accumulo.core.tablet.thrift.THostingGoal;

public enum TabletHostingGoal {

  ALWAYS, DEFAULT, NEVER, ONDEMAND;

  public static TabletHostingGoal fromThrift(THostingGoal goal) {
    switch (goal) {
      case ALWAYS:
        return ALWAYS;
      case NEVER:
        return NEVER;
      case ONDEMAND:
        return ONDEMAND;
      case DEFAULT:
        return DEFAULT;
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
        return THostingGoal.DEFAULT;

    }
  }

  public static TabletHostingGoal fromValue(Value value) {
    switch (value.toString()) {
      case HostingGoalColumnFamily.ALWAYS:
        return ALWAYS;
      case HostingGoalColumnFamily.NEVER:
        return NEVER;
      case HostingGoalColumnFamily.ONDEMAND:
        return ONDEMAND;
      case HostingGoalColumnFamily.DEFAULT:
        return DEFAULT;
      default:
        throw new IllegalArgumentException("Invalid value for hosting goal: " + value.toString());
    }
  }

  public Value toValue() {
    switch (this) {
      case ALWAYS:
        return new Value(HostingGoalColumnFamily.ALWAYS);
      case NEVER:
        return new Value(HostingGoalColumnFamily.NEVER);
      case ONDEMAND:
        return new Value(HostingGoalColumnFamily.ONDEMAND);
      default:
        return new Value(HostingGoalColumnFamily.DEFAULT);
    }
  }

}

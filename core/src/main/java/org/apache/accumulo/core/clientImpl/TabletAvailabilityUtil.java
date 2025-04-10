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

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.thrift.TTabletAvailability;
import org.apache.accumulo.core.data.Value;

public class TabletAvailabilityUtil {

  public static TabletAvailability fromThrift(TTabletAvailability tAvailability) {
    switch (tAvailability) {
      case HOSTED:
        return TabletAvailability.HOSTED;
      case UNHOSTED:
        return TabletAvailability.UNHOSTED;
      case ONDEMAND:
        return TabletAvailability.ONDEMAND;
      default:
        throw new IllegalArgumentException("Unhandled value for TAvailability: " + tAvailability);
    }
  }

  public static TTabletAvailability toThrift(TabletAvailability tabletAvailability) {
    switch (tabletAvailability) {
      case HOSTED:
        return TTabletAvailability.HOSTED;
      case UNHOSTED:
        return TTabletAvailability.UNHOSTED;
      case ONDEMAND:
        return TTabletAvailability.ONDEMAND;
      default:
        throw new IllegalArgumentException("Unhandled enum value");
    }
  }

  public static TabletAvailability fromValue(Value value) {
    switch (value.toString()) {
      case "HOSTED":
        return TabletAvailability.HOSTED;
      case "UNHOSTED":
        return TabletAvailability.UNHOSTED;
      case "ONDEMAND":
        return TabletAvailability.ONDEMAND;
      default:
        throw new IllegalArgumentException("Invalid value for tablet availability: " + value);
    }
  }

  public static Value toValue(TabletAvailability tabletAvailability) {
    switch (tabletAvailability) {
      case HOSTED:
        return new Value(TabletAvailability.HOSTED.name());
      case UNHOSTED:
        return new Value(TabletAvailability.UNHOSTED.name());
      case ONDEMAND:
        return new Value(TabletAvailability.ONDEMAND.name());
      default:
        throw new IllegalArgumentException("Unhandled enum value");
    }
  }

}

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
package org.apache.accumulo.server.manager.state;

import java.util.Objects;

import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;

import com.google.common.base.Preconditions;

public class UnassignedTablet {
  private final Location location;
  private final Location lastLocation;

  public UnassignedTablet(Location location, Location lastLocation) {
    Preconditions.checkArgument(
        lastLocation == null || lastLocation.getType() == TabletMetadata.LocationType.LAST);
    this.location = location;
    this.lastLocation = lastLocation;
  }

  public Location getLocation() {
    return location;
  }

  public Location getLastLocation() {
    return lastLocation;
  }

  public TServerInstance getServerInstance() {
    return location != null ? location.getServerInstance() : null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UnassignedTablet that = (UnassignedTablet) o;
    return Objects.equals(location, that.location)
        && Objects.equals(lastLocation, that.lastLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(location, lastLocation);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(128);
    sb.append("UnassignedTablet [").append("location:");
    if (location != null) {
      sb.append(location.getType()).append("=").append(location.getServerInstance());
    }
    sb.append(", lastLocation:");
    if (lastLocation != null) {
      sb.append(lastLocation.getType()).append("=").append(lastLocation.getServerInstance());
    }
    sb.append("]");
    return sb.toString();
  }
}

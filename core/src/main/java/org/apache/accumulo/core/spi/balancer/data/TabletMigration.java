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
package org.apache.accumulo.core.spi.balancer.data;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.accumulo.core.data.TabletId;

/**
 * @since 2.1.0
 */
public class TabletMigration {
  private final TabletId tabletId;
  private final TabletServerId oldTabletServer;
  private final TabletServerId newTabletServer;

  public TabletMigration(TabletId tabletId, TabletServerId oldTabletServer,
      TabletServerId newTabletServer) {
    this.tabletId = requireNonNull(tabletId);
    this.oldTabletServer = requireNonNull(oldTabletServer);
    this.newTabletServer = requireNonNull(newTabletServer);
  }

  public TabletId getTablet() {
    return tabletId;
  }

  public TabletServerId getOldTabletServer() {
    return oldTabletServer;
  }

  public TabletServerId getNewTabletServer() {
    return newTabletServer;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TabletMigration that = (TabletMigration) o;
    return tabletId.equals(that.tabletId) && oldTabletServer.equals(that.oldTabletServer)
        && newTabletServer.equals(that.newTabletServer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tabletId, oldTabletServer, newTabletServer);
  }

  @Override
  public String toString() {
    return tabletId + ": " + oldTabletServer + " -> " + newTabletServer;
  }
}

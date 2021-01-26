/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.manager.balancer;

import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;

public class BalanceParamsImpl implements TabletBalancer.BalanceParameters {
  private final SortedMap<TabletServerId,TServerStatus> currentStatus;
  private final Set<TabletId> currentMigrations;
  private final List<TabletMigration> migrationsOut;

  public BalanceParamsImpl(SortedMap<TabletServerId,TServerStatus> currentStatus,
      Set<TabletId> currentMigrations, List<TabletMigration> migrationsOut) {
    this.currentStatus = currentStatus;
    this.currentMigrations = currentMigrations;
    this.migrationsOut = migrationsOut;
  }

  @Override
  public SortedMap<TabletServerId,TServerStatus> currentStatus() {
    return currentStatus;
  }

  @Override
  public Set<TabletId> currentMigrations() {
    return currentMigrations;
  }

  @Override
  public List<TabletMigration> migrationsOut() {
    return migrationsOut;
  }
}

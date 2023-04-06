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
package org.apache.accumulo.core.spi.balancer;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.spi.balancer.data.TabletStatistics;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

/**
 * This interface is an extension of {@link ServiceEnvironment} that exposes system level
 * information that is specific to tablet balancing.
 *
 * @since 2.1.0
 */
public interface BalancerEnvironment extends ServiceEnvironment {
  /**
   * Many Accumulo plugins are given table IDs as this is what Accumulo uses internally to identify
   * tables. This provides a mapping of table names to table IDs for the purposes of translating
   * and/or enumerating the existing tables.
   */
  Map<String,TableId> getTableIdMap();

  /**
   * Accumulo plugins working with a table may need to know if the table is online or not before
   * operating on it.
   *
   * @param tableId The id of the table to check.
   * @return {@code true} if the table is online and {@code false} if not
   */
  boolean isTableOnline(TableId tableId);

  /**
   * Fetch the locations for each of {@code tableId}'s tablets from the metadata table. If there is
   * no location available for a given tablet, then the returned mapping will have a {@code null}
   * value stored for the tablet id.
   *
   * @param tableId The id of the table for which to retrieve tablets.
   * @return a mapping of {@link TabletId} to {@link TabletServerId} (or @null if no location is
   *         available) for each tablet belonging to {@code tableId}
   */
  Map<TabletId,TabletServerId> listTabletLocations(TableId tableId);

  /**
   * Fetch the tablets for the given table by asking the tablet server. Useful if your balance
   * strategy needs details at the tablet level to decide what tablets to move.
   *
   * @param tabletServerId The tablet server to ask.
   * @param tableId The table id
   * @return a list of tablet statistics
   * @throws AccumuloSecurityException tablet server disapproves of your internal System password.
   * @throws AccumuloException any other problem
   */
  List<TabletStatistics> listOnlineTabletsForTable(TabletServerId tabletServerId, TableId tableId)
      throws AccumuloException, AccumuloSecurityException;

  /**
   * Retrieve the classloader context that is configured for {@code tableId}, or {@code null} if
   * none is configured.
   */
  String tableContext(TableId tableId);
}

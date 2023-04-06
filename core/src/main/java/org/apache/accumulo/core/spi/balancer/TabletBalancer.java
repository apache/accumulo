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
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;

/**
 * This class is responsible for managing the distribution of tablets throughout an Accumulo
 * cluster. In most cases, users will want a balancer implementation which ensures a uniform
 * distribution of tablets, so that no individual tablet server is handling significantly more work
 * than any other.
 *
 * <p>
 * Implementations may wish to store configuration in Accumulo's system configuration using the
 * {@link Property#GENERAL_ARBITRARY_PROP_PREFIX}. They may also benefit from using per-table
 * configuration using {@link Property#TABLE_ARBITRARY_PROP_PREFIX}.
 *
 * @since 2.1.0
 */
public interface TabletBalancer {

  /**
   * An interface for grouping parameters required for the balancer to assign unassigned tablets.
   * This interface allows for evolution of the parameter set without changing the balancer's method
   * signature.
   *
   * @since 2.1.0
   */
  interface AssignmentParameters {
    /**
     * @return the current status for all tablet servers (read-only)
     */
    SortedMap<TabletServerId,TServerStatus> currentStatus();

    /**
     * @return the tablets that need to be assigned, mapped to their previous known location
     *         (read-only)
     */
    Map<TabletId,TabletServerId> unassignedTablets();

    /**
     * Assigns {@code tabletId} to {@code tabletServerId}.
     */
    void addAssignment(TabletId tabletId, TabletServerId tabletServerId);
  }

  /**
   * An interface for grouping parameters required for the balancer to balance tablets. This
   * interface allows for evolution of the parameter set without changing the balancer's method
   * signature.
   *
   * @since 2.1.0
   */
  interface BalanceParameters {
    /**
     * @return the current status for all tablet servers (read-only)
     */
    SortedMap<TabletServerId,TServerStatus> currentStatus();

    /**
     * @return the migrations that are currently in progress (read-only)
     */
    Set<TabletId> currentMigrations();

    /**
     * @return a write-only map for storing new assignments made by the balancer. It is important
     *         that any tablets found in {@link #currentMigrations()} are not included in the output
     *         migrations.
     */
    List<TabletMigration> migrationsOut();
  }

  /**
   * Initialize the TabletBalancer. This gives the balancer the opportunity to read the
   * configuration.
   */
  void init(BalancerEnvironment balancerEnvironment);

  /**
   * Assign tablets to tablet servers. This method is called whenever the manager finds tablets that
   * are unassigned.
   */
  void getAssignments(AssignmentParameters params);

  /**
   * Ask the balancer if any migrations are necessary.
   *
   * If the balancer is going to self-abort due to some environmental constraint (e.g. it requires
   * some minimum number of tservers, or a maximum number of outstanding migrations), it should
   * issue a log message to alert operators. The message should be at WARN normally and at ERROR if
   * the balancer knows that the problem can not self correct. It should not issue these messages
   * more than once a minute. This method will not be called when there are unassigned tablets.
   *
   * @return the time, in milliseconds, to wait before re-balancing.
   */
  long balance(BalanceParameters params);
}

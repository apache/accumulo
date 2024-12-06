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

    /**
     * @return map of resource group name to set of TServerInstance objects
     * @since 4.0.0
     */
    Map<String,Set<TabletServerId>> currentResourceGroups();
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

    /**
     * @return map of resource group name to set of TServerInstance objects
     * @since 4.0.0
     */
    Map<String,Set<TabletServerId>> currentResourceGroups();

    /**
     * Return the DataLevel name for which the Manager is currently balancing. Balancers should
     * return migrations for tables within the current DataLevel.
     *
     * @return name of current balancing iteration data level
     * @since 2.1.4
     */
    String currentLevel();
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

  /**
   * Provides access to information related to a tablet that is currently assigned to a tablet
   * server.
   *
   * @since 4.0.0
   */
  interface CurrentAssignment {
    TabletId getTablet();

    TabletServerId getTabletServer();

    String getResourceGroup();
  }

  /**
   * <p>
   * The manager periodically scans all tablets looking for tablets that are assigned to dead tablet
   * servers or unassigned. During the scan this method is also called for tablets that are
   * currently assigned to a live tserver to see if they should be unassigned and reassigned. If
   * this method returns true the tablet will be unloaded from the tablet sever and then later the
   * tablet will be passed to {@link #getAssignments(AssignmentParameters)}.
   * </p>
   *
   * <p>
   * One example use case for this method is a balancer that partitions tablet servers into groups.
   * If the balancers config is changed such that a table that was assigned to tablet server group A
   * should now be assigned to tablet server B, then this method can return true for the tablets in
   * that table assigned to tablet server group A. After those tablets are unloaded and passed to
   * the {@link #getAssignments(AssignmentParameters)} method it can reassign them to tablet server
   * group B.
   * </p>
   *
   * <p>
   * Accumulo may instantiate this plugin in different processes and call this method. When the
   * manager looks for tablets that needs reassignment it currently uses an Accumulo iterator to
   * scan the metadata table and filter tablets. That iterator may run on multiple tablets servers
   * and call this plugin. Keep this in mind when implementing this plugin and considering keeping
   * state between calls to this method.
   * </p>
   *
   * <p>
   * This new method may be used instead of or in addition to {@link #balance(BalanceParameters)}
   * </p>
   *
   * @since 4.0.0
   */
  default boolean needsReassignment(CurrentAssignment currentAssignment) {
    return false;
  }
}

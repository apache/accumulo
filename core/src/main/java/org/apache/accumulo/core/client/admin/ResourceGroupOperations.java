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
package org.apache.accumulo.core.client.admin;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ResourceGroupNotFoundException;
import org.apache.accumulo.core.data.ResourceGroupId;

/**
 * A ResourceGroup is a grouping of Accumulo server processes that have some shared characteristic
 * that is different than server processes in other resource groups. Examples could be homogeneous
 * hardware configurations for the server processes in one resource group but different than other
 * resource groups, or a resource group could be created for physical separation of processing for
 * table(s).
 *
 * A default resource group exists in which all server processes are assigned. The Manager, Monitor,
 * and GarbageCollector are assigned to the default resource group. Compactor, ScanServer and
 * TabletServer processes are also assigned to the default resource group, unless their respective
 * properties are set.
 *
 * This object is for defining, interacting, and removing resource group configurations. When the
 * Accumulo server processes get the system configuration, they will receive a merged view of the
 * system configuration and applicable resource group configuration, with any property defined in
 * the resource group configuration given higher priority.
 *
 * @since 4.0.0
 */
public interface ResourceGroupOperations {

  /**
   * A method to check if a resource group configuration exists in Accumulo.
   *
   * @param group the name of the resource group
   * @return true if the group exists
   */
  boolean exists(String group);

  /**
   * Retrieve a list of resource groups in Accumulo.
   *
   * @return Set of resource groups in accumulo
   */
  Set<ResourceGroupId> list();

  /**
   * Create a configuration node in zookeeper for a resource group. If not defined, then processes
   * running in the resource group will use the values of the properties defined in the system
   * configuration.
   *
   * @param group resource group
   * @throws AccumuloException if a general error occurs
   * @throws AccumuloSecurityException if the user does not have permission
   */
  void create(final ResourceGroupId group) throws AccumuloException, AccumuloSecurityException;

  /**
   * Returns the properties set for this resource group in zookeeper merged with the system
   * configuration.
   *
   * @param group resource group
   * @return Map of property keys/values
   * @throws AccumuloException if a general error occurs
   * @throws AccumuloSecurityException if the user does not have permission
   * @throws ResourceGroupNotFoundException if the specified resource group doesn't exist
   */
  Map<String,String> getConfiguration(final ResourceGroupId group)
      throws AccumuloException, AccumuloSecurityException, ResourceGroupNotFoundException;

  /**
   * Returns the un-merged properties set for this resource group in zookeeper.
   *
   * @param group resource group
   * @return Map of property keys/values
   * @throws AccumuloException if a general error occurs
   * @throws AccumuloSecurityException if the user does not have permission
   * @throws ResourceGroupNotFoundException if the specified resource group doesn't exist
   */
  Map<String,String> getProperties(final ResourceGroupId group)
      throws AccumuloException, AccumuloSecurityException, ResourceGroupNotFoundException;

  /**
   * Sets a resource group property in zookeeper. Servers will pull this setting and override the
   * equivalent setting in accumulo.properties. Changes can be seen using
   * {@code #getProperties(ResourceGroupId)} or {@link InstanceOperations#getSystemConfiguration()}
   * can be used to return a system-level merged view.
   * <p>
   * Only some properties can be changed by this method, an IllegalArgumentException will be thrown
   * if there is an attempt to set a read-only property.
   *
   * @param group resource group
   * @param property the name of a system property
   * @param value the value to set a system property to
   * @throws AccumuloException if a general error occurs
   * @throws AccumuloSecurityException if the user does not have permission
   * @throws ResourceGroupNotFoundException if the specified resource group doesn't exist
   */
  void setProperty(final ResourceGroupId group, final String property, final String value)
      throws AccumuloException, AccumuloSecurityException, ResourceGroupNotFoundException;

  /**
   * Modify resource group properties using a Consumer that accepts a mutable map containing the
   * current system property overrides stored in ZooKeeper. If the supplied Consumer alters the map
   * without throwing an Exception, then the resulting map will atomically replace the current
   * system property overrides in ZooKeeper. Only properties which can be stored in ZooKeeper will
   * be accepted.
   *
   * <p>
   * Accumulo has multiple layers of properties that for many APIs and SPIs are presented as a
   * single merged view. This API does not offer that merged view, it only offers the properties set
   * at the resource group layer to the mapMutator.
   * </p>
   *
   * <p>
   * Below is an example of using this API to conditionally set some instance properties. If while
   * trying to set the compaction planner properties another process modifies the manager balancer
   * properties, then it would automatically retry and call the lambda again with the latest
   * snapshot of instance properties.
   * </p>
   *
   * <pre>
   *         {@code
   *             AccumuloClient client = getClient();
   *             Map<String,String> acceptedProps = client.resourceGroupOperations().modifyProperties(groupId, currProps -> {
   *               var planner = currProps.get("compaction.service.default.planner");
   *               //This code will only change the compaction planner if its currently set to default settings.
   *               //The endsWith() function was used to make the example short, would be better to use equals().
   *               if(planner != null && planner.endsWith("RatioBasedCompactionPlanner") {
   *                 // tservers will eventually see these compaction planner changes and when they do they will see all of the changes at once
   *                 currProps.keySet().removeIf(
   *                    prop -> prop.startsWith("compaction.service.default.planner.opts."));
   *                 currProps.put("compaction.service.default.planner","MyPlannerClassName");
   *                 currProps.put("compaction.service.default.planner.opts.myOpt1","val1");
   *                 currProps.put("compaction.service.default.planner.opts.myOpt2","val2");
   *                }
   *             });
   *
   *             // Since three properties were set may want to check for the values of all
   *             // three, just checking one in this example to keep it short.
   *             if("MyPlannerClassName".equals(acceptedProps.get("compaction.service.default.planner"))){
   *                // the compaction planner change was accepted or already existed, so take action for that outcome
   *             } else {
   *                // the compaction planner change was not done, so take action for that outcome
   *             }
   *           }
   *         }
   * </pre>
   *
   * @param group resource group
   * @param mapMutator This consumer should modify the passed snapshot of resource group properties
   *        to contain the desired keys and values. It should be safe for Accumulo to call this
   *        consumer multiple times, this may be done automatically when certain retryable errors
   *        happen. The consumer should probably avoid accessing the Accumulo client as that could
   *        lead to undefined behavior.
   *
   * @throws AccumuloException if a general error occurs
   * @throws AccumuloSecurityException if the user does not have permission
   * @throws IllegalArgumentException if the Consumer alters the map by adding properties that
   *         cannot be stored in ZooKeeper
   * @throws ResourceGroupNotFoundException if the specified resource group doesn't exist
   *
   * @return The map that became Accumulo's new properties for this resource group. This map is
   *         immutable and contains the snapshot passed to mapMutator and the changes made by
   *         mapMutator.
   */
  Map<String,String> modifyProperties(final ResourceGroupId group,
      final Consumer<Map<String,String>> mapMutator) throws AccumuloException,
      AccumuloSecurityException, IllegalArgumentException, ResourceGroupNotFoundException;

  /**
   * Removes a resource group property from zookeeper. Changes can be seen using
   * {@code #getProperties(ResourceGroupId)} or {@link InstanceOperations#getSystemConfiguration()}
   * can be used to return a system-level merged view.
   *
   * @param group resource group
   * @param property the name of a system property
   * @throws AccumuloException if a general error occurs
   * @throws AccumuloSecurityException if the user does not have permission
   * @throws ResourceGroupNotFoundException if the specified resource group doesn't exist
   */
  void removeProperty(final ResourceGroupId group, final String property)
      throws AccumuloException, AccumuloSecurityException, ResourceGroupNotFoundException;

  /**
   * Removes a configuration node in zookeeper for a resource group. If not defined, then processes
   * running in the resource group will use the values of the properties defined in the system
   * configuration.
   *
   * @param group resource group
   * @throws AccumuloException if a general error occurs
   * @throws AccumuloSecurityException if the user does not have permission
   * @throws ResourceGroupNotFoundException if the specified resource group doesn't exist
   */
  void remove(final ResourceGroupId group)
      throws AccumuloException, AccumuloSecurityException, ResourceGroupNotFoundException;

}

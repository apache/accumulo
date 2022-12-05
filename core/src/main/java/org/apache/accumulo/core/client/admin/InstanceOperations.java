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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.InstanceId;

public interface InstanceOperations {

  /**
   * Sets a system property in zookeeper. Tablet servers will pull this setting and override the
   * equivalent setting in accumulo.properties. Changes can be seen using
   * {@link #getSystemConfiguration()}.
   * <p>
   * Only some properties can be changed by this method, an IllegalArgumentException will be thrown
   * if there is an attempt to set a read-only property.
   *
   * @param property the name of a system property
   * @param value the value to set a system property to
   * @throws AccumuloException if a general error occurs
   * @throws AccumuloSecurityException if the user does not have permission
   */
  void setProperty(final String property, final String value)
      throws AccumuloException, AccumuloSecurityException;

  /**
   * Modify system properties using a Consumer that accepts a mutable map containing the current
   * system property overrides stored in ZooKeeper. If the supplied Consumer alters the map without
   * throwing an Exception, then the resulting map will atomically replace the current system
   * property overrides in ZooKeeper. Only properties which can be stored in ZooKeeper will be
   * accepted.
   *
   * <p>
   * Accumulo has multiple layers of properties that for many APIs and SPIs are presented as a
   * single merged view. This API does not offer that merged view, it only offers the properties set
   * at the system layer to the mapMutator.
   * </p>
   *
   * <p>
   * This new API offers two distinct advantages over the older {@link #setProperty(String, String)}
   * API. The older API offered the ability to unconditionally set a single property. This new API
   * offers the following.
   * </p>
   *
   * <ul>
   * <li>Ability to unconditionally set multiple properties atomically. If five properties are
   * mutated by this API, then eventually all of the servers will see those changes all at once.
   * This is really important for configuring something like a scan iterator that requires setting
   * multiple properties.</li>
   * <li>Ability to conditionally set multiple properties atomically. With this new API a snapshot
   * of the current instance configuration is passed in to the mapMutator. Code can inspect the
   * current config and decide what if any changes it would like to make. If the config changes
   * while mapMutator is doing inspection and modification, then those actions will be ignored and
   * it will be called again with the latest snapshot of the config.</li>
   * </ul>
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
   *             Map<String,String> acceptedProps = client.instanceOperations().modifyProperties(currProps -> {
   *               var planner = currProps.get("tserver.compaction.major.service.default.planner");
   *               //This code will only change the compaction planner if its currently set to default settings.
   *               //The endsWith() function was used to make the example short, would be better to use equals().
   *               if(planner != null && planner.endsWith("DefaultCompactionPlanner") {
   *                 // tservers will eventually see these compaction planner changes and when they do they will see all of the changes at once
   *                 currProps.keySet().removeIf(
   *                    prop -> prop.startsWith("tserver.compaction.major.service.default.planner.opts."));
   *                 currProps.put("tserver.compaction.major.service.default.planner","MyPlannerClassName");
   *                 currProps.put("tserver.compaction.major.service.default.planner.opts.myOpt1","val1");
   *                 currProps.put("tserver.compaction.major.service.default.planner.opts.myOpt2","val2");
   *                }
   *             });
   *
   *             // Since three properties were set may want to check for the values of all
   *             // three, just checking one in this example to keep it short.
   *             if("MyPlannerClassName".equals(acceptedProps.get("tserver.compaction.major.service.default.planner"))){
   *                // the compaction planner change was accepted or already existed, so take action for that outcome
   *             } else {
   *                // the compaction planner change was not done, so take action for that outcome
   *             }
   *           }
   *         }
   * </pre>
   *
   * @param mapMutator This consumer should modify the passed snapshot of instance properties to
   *        contain the desired keys and values. It should be safe for Accumulo to call this
   *        consumer multiple times, this may be done automatically when certain retryable errors
   *        happen. The consumer should probably avoid accessing the Accumulo client as that could
   *        lead to undefined behavior.
   *
   * @throws AccumuloException if a general error occurs
   * @throws AccumuloSecurityException if the user does not have permission
   * @throws IllegalArgumentException if the Consumer alters the map by adding properties that
   *         cannot be stored in ZooKeeper
   *
   * @return The map that became Accumulo's new properties for this table. This map is immutable and
   *         contains the snapshot passed to mapMutator and the changes made by mapMutator.
   * @since 2.1.0
   */
  Map<String,String> modifyProperties(Consumer<Map<String,String>> mapMutator)
      throws AccumuloException, AccumuloSecurityException, IllegalArgumentException;

  /**
   * Removes a system property from zookeeper. Changes can be seen using
   * {@link #getSystemConfiguration()}
   *
   * @param property the name of a system property
   * @throws AccumuloException if a general error occurs
   * @throws AccumuloSecurityException if the user does not have permission
   */
  void removeProperty(final String property) throws AccumuloException, AccumuloSecurityException;

  /**
   * Retrieve the system-wide configuration.
   *
   * @return A map of system properties set in zookeeper. If a property is not set in zookeeper,
   *         then it will return the value set in accumulo.properties on some server. If nothing is
   *         set in an accumulo.properties file, the default value for each property will be used.
   */
  Map<String,String> getSystemConfiguration() throws AccumuloException, AccumuloSecurityException;

  /**
   * Retrieve the site configuration (that is set in the server configuration file).
   *
   * @return A map of system properties set in accumulo.properties on some server. If nothing is set
   *         in an accumulo.properties file, the default value for each property will be used.
   */
  Map<String,String> getSiteConfiguration() throws AccumuloException, AccumuloSecurityException;

  /**
   * Returns the location(s) of the accumulo manager and any redundant servers.
   *
   * @return a list of locations in <code>hostname:port</code> form.
   * @since 2.1.0
   */
  List<String> getManagerLocations();

  /**
   * Returns the locations of the active scan servers
   *
   * @return A set of currently active scan servers.
   */
  Set<String> getScanServers();

  /**
   * List the currently active tablet servers participating in the accumulo instance
   *
   * @return A list of currently active tablet servers.
   */
  List<String> getTabletServers();

  /**
   * List the active scans on a tablet server.
   *
   * @param tserver The tablet server address. This should be of the form
   *        {@code <ip address>:<port>}
   * @return A list of active scans on tablet server.
   */
  List<ActiveScan> getActiveScans(String tserver)
      throws AccumuloException, AccumuloSecurityException;

  /**
   * List the active compaction running on a tablet server. Using this method with
   * {@link #getTabletServers()} will only show compactions running on tservers, leaving out any
   * external compactions running on compactors. Use {@link #getActiveCompactions()} to get a list
   * of all compactions running on tservers and compactors.
   *
   * @param tserver The tablet server address. This should be of the form
   *        {@code <ip address>:<port>}
   * @return the list of active compactions
   * @since 1.5.0
   */
  List<ActiveCompaction> getActiveCompactions(String tserver)
      throws AccumuloException, AccumuloSecurityException;

  /**
   * List all internal and external compactions running in Accumulo.
   *
   * @return the list of active compactions
   * @since 2.1.0
   */
  List<ActiveCompaction> getActiveCompactions() throws AccumuloException, AccumuloSecurityException;

  /**
   * Throws an exception if a tablet server can not be contacted.
   *
   * @param tserver The tablet server address. This should be of the form
   *        {@code <ip address>:<port>}
   * @since 1.5.0
   */
  void ping(String tserver) throws AccumuloException;

  /**
   * Test to see if the instance can load the given class as the given type. This check does not
   * consider per table classpaths, see
   * {@link TableOperations#testClassLoad(String, String, String)}
   *
   * @return true if the instance can load the given class as the given type, false otherwise
   */
  boolean testClassLoad(final String className, final String asTypeName)
      throws AccumuloException, AccumuloSecurityException;

  /**
   * Waits for the tablet balancer to run and return no migrations.
   *
   * @since 1.7.0
   */
  void waitForBalance() throws AccumuloException;

  /**
   * Returns a unique string that identifies this instance of accumulo.
   *
   * @return a String
   * @since 2.0.0
   *
   * @deprecated in 2.1.0 Use {@link #getInstanceId()}
   */
  @Deprecated(since = "2.1.0")
  String getInstanceID();

  /**
   * Returns a unique ID object that identifies this instance of accumulo.
   *
   * @return an InstanceId
   * @since 2.1.0
   */
  InstanceId getInstanceId();
}

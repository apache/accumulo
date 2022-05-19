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

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
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
   * @param property
   *          the name of a system property
   * @param value
   *          the value to set a system property to
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
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
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws IllegalArgumentException
   *           if the Consumer alters the map by adding properties that cannot be stored in
   *           ZooKeeper
   * @throws ConcurrentModificationException
   *           without altering the stored properties if the server reports that the properties have
   *           been changed by another process
   */
  void modifyProperties(Consumer<Map<String,String>> mapMutator) throws AccumuloException,
      AccumuloSecurityException, IllegalArgumentException, ConcurrentModificationException;

  /**
   * Removes a system property from zookeeper. Changes can be seen using
   * {@link #getSystemConfiguration()}
   *
   * @param property
   *          the name of a system property
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
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
   * List the currently active tablet servers participating in the accumulo instance
   *
   * @return A list of currently active tablet servers.
   */
  List<String> getTabletServers();

  /**
   * List the active scans on a tablet server.
   *
   * @param tserver
   *          The tablet server address. This should be of the form {@code <ip address>:<port>}
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
   * @param tserver
   *          The tablet server address. This should be of the form {@code <ip address>:<port>}
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
   * @param tserver
   *          The tablet server address. This should be of the form {@code <ip address>:<port>}
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

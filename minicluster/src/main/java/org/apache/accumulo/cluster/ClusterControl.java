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
package org.apache.accumulo.cluster;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.function.Predicate;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.minicluster.MiniAccumuloServerConfig;
import org.apache.accumulo.minicluster.MiniAccumuloServerConfig.ResourceGroup;
import org.apache.accumulo.minicluster.ServerType;

/**
 * Basic functionality required to control an Accumulo cluster
 */
public interface ClusterControl {

  /**
   * Execute the given class against the cluster with the provided arguments and waits for
   * completion. Returns the exit code of the process.
   */
  int exec(Class<?> clz, String[] args) throws IOException;

  /**
   * Execute the given class against the cluster with the provided arguments and waits for
   * completion. Returns the exit code of the process with the stdout.
   */
  Entry<Integer,String> execWithStdout(Class<?> clz, String[] args) throws IOException;

  /**
   * Issue an orderly shutdown of the cluster, throws an exception if it fails to return
   * successfully (return value of 0).
   */
  void adminStopAll() throws IOException;

  /**
   * @return current resource groups that determines what is running on this cluster
   */
  default MiniAccumuloServerConfig getServerConfiguration() {
    // TODO implement
    throw new UnsupportedOperationException();
  }

  /**
   * Makes the cluster match the numbers of servers specified in the given resource groups object.
   * This will start and stop server processes depending on what was set and the current state of
   * the cluster. For example if the current resource groups for the cluster are :
   *
   * <ul>
   * <li>MANAGER, "default" ,1</li>
   * <li>TSERVER, "default", 2</li>
   * <li>COMPACTOR, "default",3</li>
   * <li>COMPACTOR, "RGA",1</li>
   * <li>COMPACTOR, "RGB",2</li>
   * </ul>
   *
   * and the following is passed in :
   *
   * <ul>
   * <li>MANAGER, "default" ,1</li>
   * <li>TSERVER, "default", 3</li>
   * <li>COMPACTOR, "RGA",3</li>
   * <li>COMPACTOR, "RGB",3</li>
   * </ul>
   *
   * Then this would start a new tserver, stop all compactors in the default resource group, start
   * two compactors in RGA, and start one compactor in RGB. This would all be done before the method
   * returns.
   */
  default void setServerConfiguration(MiniAccumuloServerConfig miniAccumuloServerConfig) {
    // TODO implement
    throw new UnsupportedOperationException();
  }

  /**
   * Start instances of Compactors
   *
   * @param compactor compactor class
   * @param limit number of compactors to start
   * @param queueName name of queue
   */
  // TODO remove in favor of setServerConfiguration
  void startCompactors(Class<? extends Compactor> compactor, int limit, String queueName)
      throws IOException;

  /**
   * Start an instance of CompactionCoordinator
   *
   * @param coordinator compaction coordinator class
   */
  // TODO remove in favor of setServerConfiguration
  void startCoordinator(Class<? extends CompactionCoordinator> coordinator) throws IOException;

  /**
   * Starts all occurrences of the given server
   */
  // TODO remove in favor of setServerConfiguration
  void startAllServers(ServerType server) throws IOException;

  /**
   * Start the given process on the host
   */
  // TODO remove in favor of setServerConfiguration
  void start(ServerType server, String hostname) throws IOException;

  /**
   * Stops all occurrences of the given server
   */
  // TODO remove in favor of setServerConfiguration
  default void stopAllServers(ServerType server) throws IOException {
    var originalSC = getServerConfiguration();
    setServerConfiguration(MiniAccumuloServerConfig.builder().put(originalSC)
        .removeIf(rg -> rg.getServerType() == server).build());
  }

  /**
   * Restarts all servers in any resource groups that matches the predicate
   */

  default void restartGroups(Predicate<ResourceGroup> rgPredicate) {
    // save the current resource groups
    var originalSC = getServerConfiguration();
    // stop any resource groups that matched the predicate
    setServerConfiguration(
        MiniAccumuloServerConfig.builder().put(originalSC).removeIf(rgPredicate).build());
    // restart those resource groups using the orginal config
    setServerConfiguration(originalSC);
  }

  /**
   * Stop the given process on the host
   */
  // TODO remove in favor of setServerConfiguration... also no test ever uses a hostname
  void stop(ServerType server, String hostname) throws IOException;

  /**
   * Send the provided signal to the process on the host
   */
  // TODO remove no test seems to use this
  void signal(ServerType server, String hostname, String signal) throws IOException;

  /**
   * Send SIGSTOP to the given process on the host
   */
  // TODO remove no test seems to use this
  void suspend(ServerType server, String hostname) throws IOException;

  /**
   * Send SIGCONT to the given process on the host
   */
  // TODO remove no test seems to use this
  void resume(ServerType server, String hostname) throws IOException;

  /**
   * Send SIGKILL to the given process on the host
   */
  // TODO remove in favor of setServerConfiguration.. could have a variant of setServerConfiguration
  // that
  // specifies how processes will be killed
  void kill(ServerType server, String hostname) throws IOException;
}

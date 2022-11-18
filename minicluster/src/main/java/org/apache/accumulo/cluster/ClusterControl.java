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

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.CompactionCoordinator;
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
   * Start instances of Compactors
   *
   * @param compactor compactor class
   * @param limit number of compactors to start
   * @param queueName name of queue
   */
  void startCompactors(Class<? extends Compactor> compactor, int limit, String queueName)
      throws IOException;

  /**
   * Start an instance of CompactionCoordinator
   *
   * @param coordinator compaction coordinator class
   */
  void startCoordinator(Class<? extends CompactionCoordinator> coordinator) throws IOException;

  /**
   * Starts all occurrences of the given server
   */
  void startAllServers(ServerType server) throws IOException;

  /**
   * Start the given process on the host
   */
  void start(ServerType server, String hostname) throws IOException;

  /**
   * Stops all occurrences of the given server
   */
  void stopAllServers(ServerType server) throws IOException;

  /**
   * Stop the given process on the host
   */
  void stop(ServerType server, String hostname) throws IOException;

  /**
   * Send the provided signal to the process on the host
   */
  void signal(ServerType server, String hostname, String signal) throws IOException;

  /**
   * Send SIGSTOP to the given process on the host
   */
  void suspend(ServerType server, String hostname) throws IOException;

  /**
   * Send SIGCONT to the given process on the host
   */
  void resume(ServerType server, String hostname) throws IOException;

  /**
   * Send SIGKILL to the given process on the host
   */
  void kill(ServerType server, String hostname) throws IOException;
}

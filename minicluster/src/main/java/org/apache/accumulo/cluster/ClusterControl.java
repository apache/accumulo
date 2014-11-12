/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.cluster;

import java.io.IOException;

import org.apache.accumulo.minicluster.ServerType;

/**
 * Basic functionality required to control an Accumulo cluster
 */
public interface ClusterControl {

  // void startAll();

  // void stopAll();

  /**
   * Start the given process on the host
   */
  void start(ServerType server, String hostname) throws IOException;

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

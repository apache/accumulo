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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;

/**
 * Defines a minimum required set of methods to run an Accumulo cluster.
 *
 * (Experimental, not guaranteed to stay backwards compatible)
 *
 * @since 1.6.0
 */

public interface AccumuloCluster {
  /**
   * Starts Accumulo and Zookeeper processes. Can only be called once.
   *
   * @throws IllegalStateException
   *           if already started
   */
  public void start() throws IOException, InterruptedException;

  /**
   * @return Accumulo instance name
   */
  public String getInstanceName();

  /**
   * @return zookeeper connection string
   */
  public String getZooKeepers();

  /**
   * Stops Accumulo and Zookeeper processes. If stop is not called, there is a shutdown hook that is setup to kill the processes. However its probably best to
   * call stop in a finally block as soon as possible.
   */
  public void stop() throws IOException, InterruptedException;

  /**
   * Get the configuration for the cluster
   */
  public AccumuloConfig getConfig();

  /**
   * Utility method to get a connector to the cluster.
   */
  public Connector getConnector(String user, String passwd) throws AccumuloException, AccumuloSecurityException;

  /**
   * Get the client configuration for the cluster
   */
  public ClientConfiguration getClientConfig();
}

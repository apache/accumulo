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
package org.apache.accumulo.miniclusterImpl.cluster;

import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Defines a minimum required set of methods to run an Accumulo cluster.
 *
 * (Experimental, not guaranteed to stay backwards compatible)
 *
 * @since 1.6.0
 */
public interface AccumuloCluster {

  enum State {
    STARTED, STOPPED, TERMINATED;
  }

  /**
   * @return Accumulo instance name
   */
  String getInstanceName();

  /**
   * @return zookeeper connection string
   */
  String getZooKeepers();

  /**
   * An AccumuloCluster can be started and stopped. Callers that keep a reference to the
   * ServerContext may experience errors when trying to use it while the cluster is stopped.
   *
   * @return ServerContext
   */
  ServerContext getServerContext();

  /**
   * @return {@link AccumuloClient} that must be closed by user
   * @since 2.0.0
   */
  AccumuloClient createAccumuloClient(String user, AuthenticationToken token);

  /**
   * @return client connection info for cluster
   */
  Properties getClientProperties();

  /**
   * Get server side config derived from accumulo.properties
   */
  AccumuloConfiguration getSiteConfiguration();

  /**
   * @return Path to accumulo.properties
   * @since 2.0.0
   */
  String getAccumuloPropertiesPath();

  /**
   * @return Path to accumulo-client.properties
   * @since 2.0.0
   */
  String getClientPropsPath();

  /**
   * Get an object that can manage a cluster
   *
   * @return Manage the state of the cluster
   */
  ClusterControl getClusterControl();

  /**
   * Start the AccumuloCluster
   */
  void start() throws Exception;

  /**
   * Stop the AccumuloCluster
   */
  void stop() throws Exception;

  /**
   * Cleans up the AccumuloCluster, stopping it if necessary. Cannot be restarted after this call.
   */
  void terminate() throws Exception;

  /**
   * @return the {@link FileSystem} in use by this cluster
   */
  FileSystem getFileSystem();

  /**
   * @return A path on {@link FileSystem} this cluster is running on that can be used for temporary
   *         files
   */
  Path getTemporaryPath();
}

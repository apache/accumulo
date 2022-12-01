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
package org.apache.accumulo.minicluster;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.ClientInfoImpl;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;

import com.google.common.base.Preconditions;

/**
 * A utility class that will create Zookeeper and Accumulo processes that write all of their data to
 * a single local directory. This class makes it easy to test code against a real Accumulo instance.
 * The use of this utility will yield results which closely match a normal Accumulo instance.
 *
 * @since 1.5.0
 */
public class MiniAccumuloCluster implements AutoCloseable {

  private MiniAccumuloClusterImpl impl;

  private MiniAccumuloCluster(MiniAccumuloConfigImpl config) throws IOException {
    impl = new MiniAccumuloClusterImpl(config);
  }

  /**
   *
   * @param dir An empty or nonexistent temp directory that Accumulo and Zookeeper can store data
   *        in. Creating the directory is left to the user. Java 7, Guava, and Junit provide methods
   *        for creating temporary directories.
   * @param rootPassword Initial root password for instance.
   */
  public MiniAccumuloCluster(File dir, String rootPassword) throws IOException {
    this(new MiniAccumuloConfigImpl(dir, rootPassword));
  }

  /**
   * @param config initial configuration
   */
  public MiniAccumuloCluster(MiniAccumuloConfig config) throws IOException {
    this(config.getImpl());
  }

  /**
   * Starts Accumulo and Zookeeper processes. Can only be called once.
   */
  public void start() throws IOException, InterruptedException {
    impl.start();
  }

  /**
   * @return generated remote debug ports if in debug mode.
   * @since 1.6.0
   */
  public Set<Pair<ServerType,Integer>> getDebugPorts() {
    return impl.getDebugPorts();
  }

  /**
   * @return Accumulo instance name
   */
  public String getInstanceName() {
    return impl.getInstanceName();
  }

  /**
   * @return zookeeper connection string
   */
  public String getZooKeepers() {
    return impl.getZooKeepers();
  }

  /**
   * Stops Accumulo and Zookeeper processes. If stop is not called, there is a shutdown hook that is
   * setup to kill the processes. However its probably best to call stop in a finally block as soon
   * as possible.
   */
  public void stop() throws IOException, InterruptedException {
    impl.stop();
  }

  /**
   * @since 2.0.1
   */
  @Override
  public void close() throws IOException {
    try {
      this.stop();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * @since 1.6.0
   */
  public MiniAccumuloConfig getConfig() {
    return new MiniAccumuloConfig(impl.getConfig());
  }

  /**
   * Utility method to create an {@link AccumuloClient} with connection to the MAC. The
   * AccumuloClient object should be closed by user
   *
   * @since 2.0.0
   */
  public AccumuloClient createAccumuloClient(String user, AuthenticationToken token) {
    return impl.createAccumuloClient(user, token);
  }

  /**
   * @return A copy of the connection properties for the cluster
   * @since 2.0.0
   */
  public Properties getClientProperties() {
    return impl.getClientProperties();
  }

  /**
   * Construct client {@link Properties} using a {@link MiniAccumuloCluster} directory
   *
   * @param directory MiniAccumuloCluster directory
   * @return {@link Properties} for that directory
   * @since 2.0.0
   */
  public static Properties getClientProperties(File directory) {
    File clientProps = new File(new File(directory, "conf"), "accumulo-client.properties");
    Preconditions.checkArgument(clientProps.exists());
    return ClientInfoImpl.toProperties(clientProps.toPath());
  }
}

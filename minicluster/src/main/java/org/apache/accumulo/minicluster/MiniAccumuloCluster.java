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
package org.apache.accumulo.minicluster;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.cluster.ClusterServerType;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;

/**
 * A utility class that will create Zookeeper and Accumulo processes that write all of their data to a single local directory. This class makes it easy to test
 * code against a real Accumulo instance. Its much more accurate for testing than {@link org.apache.accumulo.core.client.mock.MockAccumulo}, but much slower.
 *
 * @since 1.5.0
 */
public class MiniAccumuloCluster {

  private MiniAccumuloClusterImpl impl;

  private MiniAccumuloCluster(MiniAccumuloConfigImpl config) throws IOException {
    impl = new MiniAccumuloClusterImpl(config);
  }

  /**
   *
   * @param dir
   *          An empty or nonexistant temp directoy that Accumulo and Zookeeper can store data in. Creating the directory is left to the user. Java 7, Guava,
   *          and Junit provide methods for creating temporary directories.
   * @param rootPassword
   *          Initial root password for instance.
   */
  public MiniAccumuloCluster(File dir, String rootPassword) throws IOException {
    this(new MiniAccumuloConfigImpl(dir, rootPassword));
  }

  /**
   * @param config
   *          initial configuration
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
    Set<Pair<ClusterServerType,Integer>> implPorts = impl.getDebugPorts();
    Set<Pair<ServerType,Integer>> returnPorts = new HashSet<Pair<ServerType,Integer>>();
    for (Pair<ClusterServerType,Integer> pair : implPorts) {
      ServerType st = pair.getFirst().toServerType();
      if (null != st) {
        returnPorts.add(new Pair<ServerType,Integer>(st, pair.getSecond()));
      }
    }
    return returnPorts;
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
   * Stops Accumulo and Zookeeper processes. If stop is not called, there is a shutdown hook that is setup to kill the processes. However its probably best to
   * call stop in a finally block as soon as possible.
   */
  public void stop() throws IOException, InterruptedException {
    impl.stop();
  }

  /**
   * @since 1.6.0
   */
  public MiniAccumuloConfig getConfig() {
    return new MiniAccumuloConfig(impl.getConfig());
  }

  /**
   * Utility method to get a connector to the MAC.
   *
   * @since 1.6.0
   */
  public Connector getConnector(String user, String passwd) throws AccumuloException, AccumuloSecurityException {
    return impl.getConnector(user, new PasswordToken(passwd));
  }

  /**
   * @since 1.6.0
   */
  public ClientConfiguration getClientConfig() {
    return impl.getClientConfig();
  }
}

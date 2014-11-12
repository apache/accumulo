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
package org.apache.accumulo.cluster.standalone;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;

/**
 * AccumuloCluster implementation to connect to an existing deployment of Accumulo
 */
public class StandaloneAccumuloCluster implements AccumuloCluster {

  private Instance instance;
  private String accumuloHome;

  public StandaloneAccumuloCluster(String instanceName, String zookeepers) {
    this.instance = new ZooKeeperInstance(instanceName, zookeepers);
  }

  public StandaloneAccumuloCluster(Instance instance) {
    this.instance = instance;
  }

  public String getAccumuloHome() {
    return accumuloHome;
  }

  public void setAccumuloHome(String accumuloHome) {
    this.accumuloHome = accumuloHome;
  }

  @Override
  public String getInstanceName() {
    return instance.getInstanceName();
  }

  @Override
  public String getZooKeepers() {
    return instance.getZooKeepers();
  }

  @Override
  public Connector getConnector(String user, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    return instance.getConnector(user, token);
  }

  @Override
  public ClientConfiguration getClientConfig() {
    return ClientConfiguration.loadDefault().withInstance(getInstanceName()).withZkHosts(getZooKeepers());
  }

  @Override
  public ClusterControl getControl() {
    return new StandaloneClusterControl(null == accumuloHome ? System.getenv("ACCUMULO_HOME") : accumuloHome);
  }

}

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

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.minicluster.ServerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AccumuloCluster implementation to connect to an existing deployment of Accumulo
 */
public class StandaloneAccumuloCluster implements AccumuloCluster {
  private static final Logger log = LoggerFactory.getLogger(StandaloneAccumuloCluster.class);

  private Instance instance;
  private String accumuloHome, accumuloConfDir;

  public StandaloneAccumuloCluster(String instanceName, String zookeepers) {
    this(new ZooKeeperInstance(instanceName, zookeepers));
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

  public String getAccumuloConfDir() {
    return accumuloConfDir;
  }

  public void setAccumuloConfDir(String accumuloConfDir) {
    this.accumuloConfDir = accumuloConfDir;
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
  public StandaloneClusterControl getClusterControl() {
    return new StandaloneClusterControl(null == accumuloHome ? System.getenv("ACCUMULO_HOME") : accumuloHome,
        null == accumuloConfDir ? System.getenv("ACCUMULO_CONF_DIR") : accumuloConfDir);
  }

  @Override
  public void start() throws IOException {
    StandaloneClusterControl control = getClusterControl();
    File confDir = control.getConfDir();

    // TODO We can check the hosts files, but that requires us to be on a host with the installation. Limitation at the moment.

    for (String master : control.getHosts(new File(confDir, "masters"))) {
      control.start(ServerType.MASTER, master);
    }

    for (String tserver : control.getHosts(new File(confDir, "slaves"))) {
      control.start(ServerType.TABLET_SERVER, tserver);
    }

    for (String tracer : control.getHosts(new File(confDir, "tracers"))) {
      control.start(ServerType.TRACER, tracer);
    }

    for (String gc : control.getHosts(new File(confDir, "gc"))) {
      control.start(ServerType.GARBAGE_COLLECTOR, gc);
    }

    for (String monitor : control.getHosts(new File(confDir, "monitor"))) {
      control.start(ServerType.MONITOR, monitor);
    }
  }

  @Override
  public void stop() throws IOException {
    StandaloneClusterControl control = getClusterControl();
    File confDir = control.getConfDir();

    // TODO We can check the hosts files, but that requires us to be on a host with the installation. Limitation at the moment.

    for (String master : control.getHosts(new File(confDir, "masters"))) {
      control.stop(ServerType.MASTER, master);
    }

    for (String tserver : control.getHosts(new File(confDir, "slaves"))) {
      control.stop(ServerType.TABLET_SERVER, tserver);
    }

    for (String tracer : control.getHosts(new File(confDir, "tracers"))) {
      control.stop(ServerType.TRACER, tracer);
    }

    for (String gc : control.getHosts(new File(confDir, "gc"))) {
      control.stop(ServerType.GARBAGE_COLLECTOR, gc);
    }

    for (String monitor : control.getHosts(new File(confDir, "monitor"))) {
      control.stop(ServerType.MONITOR, monitor);
    }
  }

}

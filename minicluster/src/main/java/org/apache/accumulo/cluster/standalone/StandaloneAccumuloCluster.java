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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * AccumuloCluster implementation to connect to an existing deployment of Accumulo
 */
public class StandaloneAccumuloCluster implements AccumuloCluster {
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(StandaloneAccumuloCluster.class);

  static final List<ServerType> ALL_SERVER_TYPES = Collections.unmodifiableList(Arrays.asList(ServerType.MASTER, ServerType.TABLET_SERVER, ServerType.TRACER,
      ServerType.GARBAGE_COLLECTOR, ServerType.MONITOR));

  private Instance instance;
  private ClientConfiguration clientConf;
  private String accumuloHome, clientAccumuloConfDir, serverAccumuloConfDir, hadoopConfDir;
  private Path tmp;
  private List<ClusterUser> users;
  private String clientCmdPrefix;
  private String serverCmdPrefix;

  public StandaloneAccumuloCluster(ClientConfiguration clientConf, Path tmp, List<ClusterUser> users) {
    this(new ZooKeeperInstance(clientConf), clientConf, tmp, users);
  }

  public StandaloneAccumuloCluster(Instance instance, ClientConfiguration clientConf, Path tmp, List<ClusterUser> users) {
    this.instance = instance;
    this.clientConf = clientConf;
    this.tmp = tmp;
    this.users = users;
  }

  public String getAccumuloHome() {
    return accumuloHome;
  }

  public void setAccumuloHome(String accumuloHome) {
    this.accumuloHome = accumuloHome;
  }

  public String getClientAccumuloConfDir() {
    return clientAccumuloConfDir;
  }

  public void setClientAccumuloConfDir(String accumuloConfDir) {
    this.clientAccumuloConfDir = accumuloConfDir;
  }

  public String getServerAccumuloConfDir() {
    return serverAccumuloConfDir;
  }

  public void setServerAccumuloConfDir(String accumuloConfDir) {
    this.serverAccumuloConfDir = accumuloConfDir;
  }

  public void setServerCmdPrefix(String serverCmdPrefix) {
    this.serverCmdPrefix = serverCmdPrefix;
  }

  public void setClientCmdPrefix(String clientCmdPrefix) {
    this.clientCmdPrefix = clientCmdPrefix;
  }

  public String getHadoopConfDir() {
    if (null == hadoopConfDir) {
      hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    }
    if (null == hadoopConfDir) {
      throw new IllegalArgumentException("Cannot determine HADOOP_CONF_DIR for standalone cluster");
    }
    return hadoopConfDir;
  }

  public void setHadoopConfDir(String hadoopConfDir) {
    this.hadoopConfDir = hadoopConfDir;
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
    return clientConf;
  }

  @Override
  public StandaloneClusterControl getClusterControl() {
    return new StandaloneClusterControl(accumuloHome, clientAccumuloConfDir, serverAccumuloConfDir, clientCmdPrefix, serverCmdPrefix);
  }

  @Override
  public void start() throws IOException {
    StandaloneClusterControl control = getClusterControl();

    // TODO We can check the hosts files, but that requires us to be on a host with the installation. Limitation at the moment.

    control.setGoalState(MasterGoalState.NORMAL.toString());

    for (ServerType type : ALL_SERVER_TYPES) {
      control.startAllServers(type);
    }
  }

  @Override
  public void stop() throws IOException {
    StandaloneClusterControl control = getClusterControl();

    // TODO We can check the hosts files, but that requires us to be on a host with the installation. Limitation at the moment.

    for (ServerType type : ALL_SERVER_TYPES) {
      control.stopAllServers(type);
    }
  }

  public Configuration getHadoopConfiguration() {
    String confDir = getHadoopConfDir();
    // Using CachedConfiguration will make repeatedly calling this method much faster
    final Configuration conf = CachedConfiguration.getInstance();
    conf.addResource(new Path(confDir, "core-site.xml"));
    // Need hdfs-site.xml for NN HA
    conf.addResource(new Path(confDir, "hdfs-site.xml"));
    return conf;
  }

  @Override
  public FileSystem getFileSystem() throws IOException {
    Configuration conf = getHadoopConfiguration();
    return FileSystem.get(conf);
  }

  @Override
  public Path getTemporaryPath() {
    return tmp;
  }

  public ClusterUser getUser(int offset) {
    checkArgument(offset >= 0 && offset < users.size(), "Invalid offset, should be non-negative and less than " + users.size());
    return users.get(offset);
  }

  @Override
  public AccumuloConfiguration getSiteConfiguration() {
    Configuration conf = new Configuration(false);
    Path accumuloSite = new Path(serverAccumuloConfDir, "accumulo-site.xml");
    conf.addResource(accumuloSite);
    return new ConfigurationCopy(Iterables.concat(DefaultConfiguration.getInstance(), conf));
  }
}

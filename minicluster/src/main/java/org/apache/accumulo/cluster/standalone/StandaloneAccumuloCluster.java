/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.cluster.standalone;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.ClientConfConverter;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * AccumuloCluster implementation to connect to an existing deployment of Accumulo
 */
public class StandaloneAccumuloCluster implements AccumuloCluster {
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(StandaloneAccumuloCluster.class);

  static final List<ServerType> ALL_SERVER_TYPES =
      Collections.unmodifiableList(Arrays.asList(ServerType.MASTER, ServerType.TABLET_SERVER,
          ServerType.TRACER, ServerType.GARBAGE_COLLECTOR, ServerType.MONITOR));

  private ClientInfo info;
  private String accumuloHome, clientAccumuloConfDir, serverAccumuloConfDir, hadoopConfDir;
  private Path tmp;
  private List<ClusterUser> users;
  private String clientCmdPrefix;
  private String serverCmdPrefix;
  private SiteConfiguration siteConfig;
  private ServerContext context;

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input file name")
  public StandaloneAccumuloCluster(ClientInfo info, Path tmp, List<ClusterUser> users,
      String serverAccumuloConfDir) {
    this.info = info;
    this.tmp = tmp;
    this.users = users;
    this.serverAccumuloConfDir = serverAccumuloConfDir;
    siteConfig =
        SiteConfiguration.fromFile(new File(serverAccumuloConfDir, "accumulo.properties")).build();
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

  public void setServerCmdPrefix(String serverCmdPrefix) {
    this.serverCmdPrefix = serverCmdPrefix;
  }

  public void setClientCmdPrefix(String clientCmdPrefix) {
    this.clientCmdPrefix = clientCmdPrefix;
  }

  public String getHadoopConfDir() {
    if (hadoopConfDir == null) {
      hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    }
    if (hadoopConfDir == null) {
      throw new IllegalArgumentException("Cannot determine HADOOP_CONF_DIR for standalone cluster");
    }
    return hadoopConfDir;
  }

  public void setHadoopConfDir(String hadoopConfDir) {
    this.hadoopConfDir = hadoopConfDir;
  }

  @Override
  public String getInstanceName() {
    return info.getInstanceName();
  }

  @Override
  public String getZooKeepers() {
    return info.getZooKeepers();
  }

  @Override
  public synchronized ServerContext getServerContext() {
    if (context == null) {
      context = ServerContext.override(siteConfig, info.getInstanceName(), info.getZooKeepers(),
          info.getZooKeepersSessionTimeOut());
    }
    return context;
  }

  @Override
  public AccumuloClient createAccumuloClient(String user, AuthenticationToken token) {
    return Accumulo.newClient().to(getInstanceName(), getZooKeepers()).as(user, token).build();
  }

  @Override
  @Deprecated
  public org.apache.accumulo.core.client.ClientConfiguration getClientConfig() {
    return ClientConfConverter.toClientConf(info.getProperties());
  }

  @Override
  public Properties getClientProperties() {
    return info.getProperties();
  }

  @Override
  public StandaloneClusterControl getClusterControl() {
    return new StandaloneClusterControl(accumuloHome, clientAccumuloConfDir, serverAccumuloConfDir,
        clientCmdPrefix, serverCmdPrefix);
  }

  @Override
  public void start() throws IOException {
    StandaloneClusterControl control = getClusterControl();

    // TODO We can check the hosts files, but that requires us to be on a host with the
    // installation. Limitation at the moment.

    control.setGoalState(MasterGoalState.NORMAL.toString());

    for (ServerType type : ALL_SERVER_TYPES) {
      control.startAllServers(type);
    }
  }

  @Override
  public void stop() throws IOException {
    StandaloneClusterControl control = getClusterControl();

    // TODO We can check the hosts files, but that requires us to be on a host with the
    // installation. Limitation at the moment.

    for (ServerType type : ALL_SERVER_TYPES) {
      control.stopAllServers(type);
    }
  }

  public Configuration getHadoopConfiguration() {
    String confDir = getHadoopConfDir();
    // Using CachedConfiguration will make repeatedly calling this method much faster
    final Configuration conf = getServerContext().getHadoopConf();
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
    checkArgument(offset >= 0 && offset < users.size(),
        "Invalid offset, should be non-negative and less than " + users.size());
    return users.get(offset);
  }

  @Override
  public AccumuloConfiguration getSiteConfiguration() {
    return new ConfigurationCopy(siteConfig);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input file name")
  @Override
  public String getAccumuloPropertiesPath() {
    return new File(serverAccumuloConfDir, "accumulo.properties").toString();
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input file name")
  @Override
  public String getClientPropsPath() {
    return new File(clientAccumuloConfDir, "accumulo-client.properties").toString();
  }
}

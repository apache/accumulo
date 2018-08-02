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
package org.apache.accumulo.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.ClientConfConverter;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.InstanceOperationsImpl;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.metrics.MetricsSystemHelper;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerInfo implements ClientInfo {

  private static final Logger log = LoggerFactory.getLogger(ServerInfo.class);

  private static ServerInfo serverInfoInstance = null;
  private String instanceID;
  private String instanceName;
  private String zooKeepers;
  private int zooKeepersSessionTimeOut;
  private String zooKeeperRoot;
  private ServerConfigurationFactory serverConfFactory = null;
  private VolumeManager volumeManager;
  private String applicationName = null;
  private String applicationClassName = null;
  private String hostname = null;
  private ZooCache zooCache;

  public ServerInfo(ClientInfo info) {
    this(info.getInstanceName(), info.getZooKeepers(), info.getZooKeepersSessionTimeOut());
  }

  public ServerInfo(String instanceName, String zooKeepers, int zooKeepersSessionTimeOut) {
    this.instanceName = instanceName;
    this.zooKeepers = zooKeepers;
    this.zooKeepersSessionTimeOut = zooKeepersSessionTimeOut;
    zooCache = new ZooCacheFactory().getZooCache(zooKeepers, zooKeepersSessionTimeOut);
    String instanceNamePath = Constants.ZROOT + Constants.ZINSTANCES + "/" + instanceName;
    byte[] iidb = zooCache.get(instanceNamePath);
    if (iidb == null) {
      throw new RuntimeException("Instance name " + instanceName + " does not exist in zookeeper. "
          + "Run \"accumulo org.apache.accumulo.server.util.ListInstances\" to see a list.");
    }
    instanceID = new String(iidb, UTF_8);
    if (zooCache.get(Constants.ZROOT + "/" + instanceID) == null) {
      if (instanceName == null) {
        throw new RuntimeException("Instance id " + instanceID + " does not exist in zookeeper");
      }
      throw new RuntimeException("Instance id " + instanceID + " pointed to by the name "
          + instanceName + " does not exist in zookeeper");
    }
    zooKeeperRoot = ZooUtil.getRoot(instanceID);
  }

  public ServerInfo() {
    this(SiteConfiguration.getInstance());
  }

  public ServerInfo(AccumuloConfiguration config) {
    try {
      volumeManager = VolumeManagerImpl.get();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    Path instanceIdPath = Accumulo.getAccumuloInstanceIdPath(volumeManager);
    instanceID = ZooUtil.getInstanceIDFromHdfs(instanceIdPath, config);
    zooKeeperRoot = ZooUtil.getRoot(instanceID);
    zooKeepers = config.get(Property.INSTANCE_ZK_HOST);
    zooKeepersSessionTimeOut = (int) config.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
    zooCache = new ZooCacheFactory().getZooCache(zooKeepers, zooKeepersSessionTimeOut);
    instanceName = InstanceOperationsImpl.lookupInstanceName(zooCache, UUID.fromString(instanceID));
  }

  synchronized public static ServerInfo getInstance() {
    if (serverInfoInstance == null) {
      serverInfoInstance = new ServerInfo();
    }
    return serverInfoInstance;
  }

  public void setupServer(String appName, String appClassName, String hostname) {
    applicationName = appName;
    applicationClassName = appClassName;
    this.hostname = hostname;
    SecurityUtil.serverLogin(SiteConfiguration.getInstance());
    log.info("Version " + Constants.VERSION);
    log.info("Instance " + instanceID);
    try {
      Accumulo.init(volumeManager, getInstanceID(), getServerConfFactory(), applicationName);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    MetricsSystemHelper.configure(applicationClassName);
    DistributedTrace.enable(hostname, applicationName,
        getServerConfFactory().getSystemConfiguration());
  }

  public VolumeManager getVolumeManager() {
    return volumeManager;
  }

  public String getInstanceID() {
    return instanceID;
  }

  public String getZooKeeperRoot() {
    return zooKeeperRoot;
  }

  public String getZooKeepers() {
    return zooKeepers;
  }

  public int getZooKeepersSessionTimeOut() {
    return zooKeepersSessionTimeOut;
  }

  @Override
  public String getPrincipal() {
    return getCredentials().getPrincipal();
  }

  @Override
  public AuthenticationToken getAuthenticationToken() {
    return getCredentials().getToken();
  }

  @Override
  public boolean saslEnabled() {
    return getServerConfFactory().getSystemConfiguration()
        .getBoolean(Property.INSTANCE_RPC_SASL_ENABLED);
  }

  @Override
  public Properties getProperties() {
    Properties properties = ClientConfConverter.toProperties(getServerConfFactory().getSystemConfiguration());
    properties.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), getZooKeepers());
    properties.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getKey(),
        Integer.toString(getZooKeepersSessionTimeOut()));
    properties.setProperty(ClientProperty.INSTANCE_NAME.getKey(), getInstanceName());
    ClientProperty.setAuthenticationToken(properties, getAuthenticationToken());
    properties.setProperty(ClientProperty.AUTH_PRINCIPAL.getKey(), getPrincipal());
    return properties;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public Credentials getCredentials() {
    return SystemCredentials.get(getInstanceID());
  }

  public Connector getConnector(String principal, AuthenticationToken token)
      throws AccumuloSecurityException, AccumuloException {
    return Connector.builder().forInstance(instanceName, zooKeepers).usingToken(principal, token)
        .withZkTimeout(zooKeepersSessionTimeOut).build();
  }

  public synchronized ServerConfigurationFactory getServerConfFactory() {
    if (serverConfFactory == null) {
      serverConfFactory = new ServerConfigurationFactory(this);
    }
    return serverConfFactory;
  }

  public String getApplicationName() {
    Objects.requireNonNull(applicationName);
    return applicationName;
  }

  public String getApplicationClassName() {
    Objects.requireNonNull(applicationClassName);
    return applicationName;
  }

  public String getHostname() {
    Objects.requireNonNull(hostname);
    return hostname;
  }

  public void teardownServer() {
    DistributedTrace.disable();
  }

  public List<String> getMasterLocations() {

    String masterLocPath = ZooUtil.getRoot(instanceID) + Constants.ZMASTER_LOCK;

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Looking up master location in zoocache.", Thread.currentThread().getId());
      timer = new OpTimer().start();
    }

    byte[] loc = ZooLock.getLockData(zooCache, masterLocPath, null);

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Found master at {} in {}", Thread.currentThread().getId(),
          (loc == null ? "null" : new String(loc, UTF_8)),
          String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
    }

    if (loc == null) {
      return Collections.emptyList();
    }

    return Collections.singletonList(new String(loc, UTF_8));
  }

  public String getRootTabletLocation() {
    String zRootLocPath = ZooUtil.getRoot(instanceID) + RootTable.ZROOT_TABLET_LOCATION;

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Looking up root tablet location in zoocache.",
          Thread.currentThread().getId());
      timer = new OpTimer().start();
    }

    byte[] loc = zooCache.get(zRootLocPath);

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Found root tablet at {} in {}", Thread.currentThread().getId(),
          (loc == null ? "null" : new String(loc, UTF_8)),
          String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
    }

    if (loc == null) {
      return null;
    }

    return new String(loc, UTF_8).split("\\|")[0];
  }
}

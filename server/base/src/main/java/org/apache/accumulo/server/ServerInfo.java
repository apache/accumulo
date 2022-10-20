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
package org.apache.accumulo.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Properties;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.ClientConfConverter;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.clientImpl.InstanceOperationsImpl;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ServerInfo implements ClientInfo {

  private final SiteConfiguration siteConfig;
  private final Configuration hadoopConf;
  private final InstanceId instanceID;
  private final String instanceName;
  private final String zooKeepers;
  private final int zooKeepersSessionTimeOut;
  private final VolumeManager volumeManager;
  private final ZooCache zooCache;
  private final ServerDirs serverDirs;
  private final Credentials credentials;

  ServerInfo(SiteConfiguration siteConfig, String instanceName, String zooKeepers,
      int zooKeepersSessionTimeOut) {
    SingletonManager.setMode(Mode.SERVER);
    this.siteConfig = siteConfig;
    this.hadoopConf = new Configuration();
    this.instanceName = instanceName;
    this.zooKeepers = zooKeepers;
    this.zooKeepersSessionTimeOut = zooKeepersSessionTimeOut;
    try {
      volumeManager = VolumeManagerImpl.get(siteConfig, hadoopConf);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    zooCache = new ZooCacheFactory().getZooCache(zooKeepers, zooKeepersSessionTimeOut);
    String instanceNamePath = Constants.ZROOT + Constants.ZINSTANCES + "/" + instanceName;
    byte[] iidb = zooCache.get(instanceNamePath);
    if (iidb == null) {
      throw new RuntimeException("Instance name " + instanceName + " does not exist in zookeeper. "
          + "Run \"accumulo org.apache.accumulo.server.util.ListInstances\" to see a list.");
    }
    instanceID = InstanceId.of(new String(iidb, UTF_8));
    if (zooCache.get(Constants.ZROOT + "/" + instanceID) == null) {
      if (instanceName == null) {
        throw new RuntimeException("Instance id " + instanceID + " does not exist in zookeeper");
      }
      throw new RuntimeException("Instance id " + instanceID + " pointed to by the name "
          + instanceName + " does not exist in zookeeper");
    }
    serverDirs = new ServerDirs(siteConfig, hadoopConf);
    credentials = SystemCredentials.get(instanceID, siteConfig);
  }

  ServerInfo(SiteConfiguration config) {
    SingletonManager.setMode(Mode.SERVER);
    siteConfig = config;
    hadoopConf = new Configuration();
    try {
      volumeManager = VolumeManagerImpl.get(siteConfig, hadoopConf);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    serverDirs = new ServerDirs(siteConfig, hadoopConf);
    Path instanceIdPath = serverDirs.getInstanceIdLocation(volumeManager.getFirst());
    instanceID = VolumeManager.getInstanceIDFromHdfs(instanceIdPath, hadoopConf);
    zooKeepers = config.get(Property.INSTANCE_ZK_HOST);
    zooKeepersSessionTimeOut = (int) config.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
    zooCache = new ZooCacheFactory().getZooCache(zooKeepers, zooKeepersSessionTimeOut);
    instanceName = InstanceOperationsImpl.lookupInstanceName(zooCache, instanceID);
    credentials = SystemCredentials.get(instanceID, siteConfig);
  }

  ServerInfo(SiteConfiguration config, String instanceName, InstanceId instanceID) {
    SingletonManager.setMode(Mode.SERVER);
    siteConfig = config;
    hadoopConf = new Configuration();
    try {
      volumeManager = VolumeManagerImpl.get(siteConfig, hadoopConf);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    this.instanceID = instanceID;
    zooKeepers = config.get(Property.INSTANCE_ZK_HOST);
    zooKeepersSessionTimeOut = (int) config.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
    zooCache = new ZooCacheFactory().getZooCache(zooKeepers, zooKeepersSessionTimeOut);
    this.instanceName = instanceName;
    serverDirs = new ServerDirs(siteConfig, hadoopConf);
    credentials = SystemCredentials.get(instanceID, siteConfig);
  }

  public SiteConfiguration getSiteConfiguration() {
    return siteConfig;
  }

  public VolumeManager getVolumeManager() {
    return volumeManager;
  }

  public InstanceId getInstanceID() {
    return instanceID;
  }

  @Override
  public String getZooKeepers() {
    return zooKeepers;
  }

  @Override
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
    return getSiteConfiguration().getBoolean(Property.INSTANCE_RPC_SASL_ENABLED);
  }

  @Override
  public Properties getProperties() {
    Properties properties = ClientConfConverter.toProperties(getSiteConfiguration());
    properties.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), getZooKeepers());
    properties.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getKey(),
        Integer.toString(getZooKeepersSessionTimeOut()));
    properties.setProperty(ClientProperty.INSTANCE_NAME.getKey(), getInstanceName());
    ClientProperty.setAuthenticationToken(properties, getAuthenticationToken());
    properties.setProperty(ClientProperty.AUTH_PRINCIPAL.getKey(), getPrincipal());
    return properties;
  }

  @Override
  public String getInstanceName() {
    return instanceName;
  }

  public Credentials getCredentials() {
    return credentials;
  }

  @Override
  public Configuration getHadoopConf() {
    return this.hadoopConf;
  }

  public ServerDirs getServerDirs() {
    return serverDirs;
  }
}

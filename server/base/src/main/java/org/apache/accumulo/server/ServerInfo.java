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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.ClientConfConverter;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;

public class ServerInfo implements ClientInfo {

  private final SiteConfiguration siteConfig;
  private final Configuration hadoopConf;
  private final InstanceId instanceID;
  private final String instanceName;
  private final String zooKeepers;
  private final int zooKeepersSessionTimeOut;
  private final VolumeManager volumeManager;
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
      throw new UncheckedIOException(e);
    }
    final ZooReader zooReader = new ZooReader(this.zooKeepers, this.zooKeepersSessionTimeOut);
    final String instanceNamePath = Constants.ZROOT + Constants.ZINSTANCES + "/" + instanceName;
    try {
      byte[] iidb = zooReader.getData(instanceNamePath);
      if (iidb == null) {
        throw new IllegalStateException(
            "Instance name " + instanceName + " does not exist in zookeeper. "
                + "Run \"accumulo org.apache.accumulo.server.util.ListInstances\" to see a list.");
      }
      instanceID = InstanceId.of(new String(iidb, UTF_8));
      if (zooReader.getData(ZooUtil.getRoot(instanceID)) == null) {
        if (instanceName == null) {
          throw new IllegalStateException(
              "Instance id " + instanceID + " does not exist in zookeeper");
        }
        throw new IllegalStateException("Instance id " + instanceID + " pointed to by the name "
            + instanceName + " does not exist in zookeeper");
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalArgumentException("Unabled to create client, instanceId not found", e);
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
      throw new UncheckedIOException(e);
    }
    serverDirs = new ServerDirs(siteConfig, hadoopConf);
    Path instanceIdPath = serverDirs.getInstanceIdLocation(volumeManager.getFirst());
    instanceID = VolumeManager.getInstanceIDFromHdfs(instanceIdPath, hadoopConf);
    zooKeepers = config.get(Property.INSTANCE_ZK_HOST);
    zooKeepersSessionTimeOut = (int) config.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
    credentials = SystemCredentials.get(instanceID, siteConfig);
    final ZooReader zooReader = new ZooReader(this.zooKeepers, this.zooKeepersSessionTimeOut);
    try {
      instanceName = lookupInstanceName(zooReader, instanceID);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Unable to lookup instanceName for instanceID: " + instanceID,
          e);
    }
  }

  ServerInfo(SiteConfiguration config, String instanceName, InstanceId instanceID) {
    SingletonManager.setMode(Mode.SERVER);
    siteConfig = config;
    hadoopConf = new Configuration();
    try {
      volumeManager = VolumeManagerImpl.get(siteConfig, hadoopConf);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    this.instanceID = instanceID;
    zooKeepers = config.get(Property.INSTANCE_ZK_HOST);
    zooKeepersSessionTimeOut = (int) config.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
    this.instanceName = instanceName;
    serverDirs = new ServerDirs(siteConfig, hadoopConf);
    credentials = SystemCredentials.get(instanceID, siteConfig);
  }

  private String lookupInstanceName(ZooReader zr, InstanceId instanceId)
      throws KeeperException, InterruptedException {
    checkArgument(zr != null, "zooReader is null");
    checkArgument(instanceId != null, "instanceId is null");
    for (String name : zr.getChildren(Constants.ZROOT + Constants.ZINSTANCES)) {
      var bytes = zr.getData(Constants.ZROOT + Constants.ZINSTANCES + "/" + name);
      InstanceId iid = InstanceId.of(new String(bytes, UTF_8));
      if (iid.equals(instanceId)) {
        return name;
      }
    }
    return null;
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

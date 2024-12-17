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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.ClientConfConverter;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.ZooKeeper;

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

  static ServerInfo fromHdfsAndZooKeeper(SiteConfiguration siteConfig) {
    // pass in enough information in the site configuration to connect to HDFS and ZooKeeper
    // then get the instanceId from HDFS, and use it to look up the instanceName from ZooKeeper
    return new ServerInfo(siteConfig, Optional.empty(), Optional.empty(), Optional.empty(),
        OptionalInt.empty());
  }

  static ServerInfo forUtilities(SiteConfiguration siteConfig, ClientInfo info) {
    // get everything from the ClientInfo, which itself would look up the instanceId from the
    // configured name
    return new ServerInfo(siteConfig, Optional.of(info.getInstanceName()),
        Optional.of(info.getInstanceId()), Optional.of(info.getZooKeepers()),
        OptionalInt.of(info.getZooKeepersSessionTimeOut()));
  }

  static ServerInfo initialize(SiteConfiguration siteConfig, String instanceName,
      InstanceId instanceId) {
    // get the ZK hosts and timeout from the site config, but pass the name and ID directly
    return new ServerInfo(siteConfig, Optional.of(instanceName), Optional.of(instanceId),
        Optional.empty(), OptionalInt.empty());
  }

  static ServerInfo forTesting(SiteConfiguration siteConfig, String instanceName, String zooKeepers,
      int zooKeepersSessionTimeOut) {
    return new ServerInfo(siteConfig, Optional.ofNullable(instanceName), Optional.empty(),
        Optional.ofNullable(zooKeepers), OptionalInt.of(zooKeepersSessionTimeOut));
  }

  private ServerInfo(SiteConfiguration siteConfig, Optional<String> instanceNameOpt,
      Optional<InstanceId> instanceIdOpt, Optional<String> zkHostsOpt,
      OptionalInt zkSessionTimeoutOpt) {
    SingletonManager.setMode(Mode.SERVER);
    this.siteConfig = requireNonNull(siteConfig);
    requireNonNull(instanceNameOpt);
    requireNonNull(instanceIdOpt);
    requireNonNull(zkHostsOpt);
    requireNonNull(zkSessionTimeoutOpt);

    this.hadoopConf = new Configuration();
    try {
      this.volumeManager = VolumeManagerImpl.get(siteConfig, hadoopConf);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    this.serverDirs = new ServerDirs(siteConfig, hadoopConf);

    this.zooKeepers = zkHostsOpt.orElseGet(() -> siteConfig.get(Property.INSTANCE_ZK_HOST));
    this.zooKeepersSessionTimeOut = zkSessionTimeoutOpt
        .orElseGet(() -> (int) siteConfig.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT));

    // if not provided, look up the instanceId from ZK if the name was provided, HDFS otherwise
    this.instanceID = instanceIdOpt.orElseGet(() -> instanceNameOpt.isPresent()
        ? ZooUtil.getInstanceID(this.zooKeepers, this.zooKeepersSessionTimeOut,
            instanceNameOpt.orElseThrow())
        : VolumeManager.getInstanceIDFromHdfs(
            serverDirs.getInstanceIdLocation(volumeManager.getFirst()), hadoopConf));
    // if not provided, look up the instanceName from ZooKeeper, using the instanceId
    this.instanceName = instanceNameOpt.orElseGet(() -> ZooUtil.getInstanceName(this.zooKeepers,
        this.zooKeepersSessionTimeOut, this.instanceID));

    this.credentials = SystemCredentials.get(instanceID, siteConfig);
  }

  public SiteConfiguration getSiteConfiguration() {
    return siteConfig;
  }

  public VolumeManager getVolumeManager() {
    return volumeManager;
  }

  @Override
  public InstanceId getInstanceId() {
    return instanceID;
  }

  @Override
  public Supplier<ZooKeeper> getZooKeeperSupplier() {
    return () -> ZooUtil.connect(getClass().getSimpleName(), getZooKeepers(),
        getZooKeepersSessionTimeOut(), getSiteConfiguration().get(Property.INSTANCE_SECRET));
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

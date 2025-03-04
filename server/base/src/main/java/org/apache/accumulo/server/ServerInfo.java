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

import static com.google.common.base.Suppliers.memoize;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.ClientConfConverter;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.hadoop.conf.Configuration;

public class ServerInfo implements ClientInfo {

  private static final Function<ServerInfo,String> GET_ZK_HOSTS_FROM_CONFIG =
      si -> si.getSiteConfiguration().get(Property.INSTANCE_ZK_HOST);

  private static final ToIntFunction<ServerInfo> GET_ZK_TIMEOUT_FROM_CONFIG =
      si -> (int) si.getSiteConfiguration().getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);

  // set things up using the config file, the instanceId from HDFS, and ZK for the instanceName
  static ServerInfo fromServerConfig(SiteConfiguration siteConfig) {
    final Function<ServerInfo,String> instanceNameFromZk = si -> {
      try (var zk =
          si.getZooKeeperSupplier(ServerInfo.class.getSimpleName() + ".getInstanceName()", "")
              .get()) {
        return ZooUtil.getInstanceName(zk, si.getInstanceId());
      }
    };
    final Function<ServerInfo,
        InstanceId> instanceIdFromHdfs = si -> VolumeManager.getInstanceIDFromHdfs(
            si.getServerDirs().getInstanceIdLocation(si.getVolumeManager().getFirst()),
            si.getHadoopConf());
    return new ServerInfo(siteConfig, GET_ZK_HOSTS_FROM_CONFIG, GET_ZK_TIMEOUT_FROM_CONFIG,
        instanceNameFromZk, instanceIdFromHdfs);
  }

  // set things up using a provided instanceName and InstanceId to initialize the system, but still
  // have a ServerContext that is functional without bootstrapping issues, so long as you don't call
  // functions from it that require an instance to have already been initialized
  static ServerInfo initialize(SiteConfiguration siteConfig, String instanceName,
      InstanceId instanceId) {
    requireNonNull(instanceName);
    requireNonNull(instanceId);
    return new ServerInfo(siteConfig, GET_ZK_HOSTS_FROM_CONFIG, GET_ZK_TIMEOUT_FROM_CONFIG,
        si -> instanceName, si -> instanceId);
  }

  // set things up using the config file, and the client config for a server-side CLI utility
  static ServerInfo fromServerAndClientConfig(SiteConfiguration siteConfig, ClientInfo info) {
    // ClientInfo.getInstanceId looks up the ID in ZK using the provided instance name
    return new ServerInfo(siteConfig, si -> info.getZooKeepers(),
        si -> info.getZooKeepersSessionTimeOut(), si -> info.getInstanceName(),
        si -> info.getInstanceId());
  }

  static ServerInfo forTesting(SiteConfiguration siteConfig, String instanceName, String zooKeepers,
      int zooKeepersSessionTimeOut) {
    var props = new Properties();
    props.put(ClientProperty.INSTANCE_NAME, requireNonNull(instanceName));
    props.put(ClientProperty.INSTANCE_ZOOKEEPERS, requireNonNull(zooKeepers));
    props.put(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, zooKeepersSessionTimeOut);
    return fromServerAndClientConfig(siteConfig, ClientInfo.from(props));
  }

  // required parameter
  private final SiteConfiguration siteConfig;

  // suppliers for lazily loading
  private final Supplier<Configuration> hadoopConf;
  private final Supplier<VolumeManager> volumeManager;
  private final Supplier<ServerDirs> serverDirs;
  private final Supplier<String> zooKeepers;
  private final Supplier<Integer> zooKeepersSessionTimeOut; // can't memoize IntSupplier
  private final Supplier<InstanceId> instanceId;
  private final Supplier<String> instanceName;
  private final Supplier<Credentials> credentials;
  private final BiFunction<String,String,ZooSession> zooSessionForName;

  // set up everything to be lazily loaded with memoized suppliers, so if nothing is used, the cost
  // is low; to support different scenarios, plug in the functionality to retrieve certain items
  // from ZooKeeper, HDFS, or from the input, using functions that take "this" and emit the desired
  // object to be memoized on demand; these functions should not have cyclic dependencies on one
  // another, but because things are lazily loaded, it is okay if one depends on another in one
  // direction only
  private ServerInfo(SiteConfiguration siteConfig, Function<ServerInfo,String> zkHostsFunction,
      ToIntFunction<ServerInfo> zkTimeoutFunction, Function<ServerInfo,String> instanceNameFunction,
      Function<ServerInfo,InstanceId> instanceIdFunction) {
    this.siteConfig = requireNonNull(siteConfig);
    requireNonNull(zkHostsFunction);
    requireNonNull(zkTimeoutFunction);
    requireNonNull(instanceNameFunction);
    requireNonNull(instanceIdFunction);

    this.hadoopConf = memoize(Configuration::new);
    this.volumeManager = memoize(() -> {
      try {
        return VolumeManagerImpl.get(getSiteConfiguration(), getHadoopConf());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    this.serverDirs = memoize(() -> new ServerDirs(getSiteConfiguration(), getHadoopConf()));
    this.credentials =
        memoize(() -> SystemCredentials.get(getInstanceId(), getSiteConfiguration()));

    this.zooSessionForName = (name, rootPath) -> new ZooSession(name, getZooKeepers() + rootPath,
        getZooKeepersSessionTimeOut(), getSiteConfiguration().get(Property.INSTANCE_SECRET));

    // from here on, set up the suppliers based on what was passed in, to support different cases
    this.zooKeepers = memoize(() -> zkHostsFunction.apply(this));
    this.zooKeepersSessionTimeOut = memoize(() -> zkTimeoutFunction.applyAsInt(this));
    this.instanceId = memoize(() -> instanceIdFunction.apply(this));
    this.instanceName = memoize(() -> instanceNameFunction.apply(this));
  }

  public SiteConfiguration getSiteConfiguration() {
    return siteConfig;
  }

  public VolumeManager getVolumeManager() {
    return volumeManager.get();
  }

  @Override
  public InstanceId getInstanceId() {
    return instanceId.get();
  }

  @Override
  public Supplier<ZooSession> getZooKeeperSupplier(String clientName, String rootPath) {
    return () -> zooSessionForName.apply(requireNonNull(clientName), requireNonNull(rootPath));
  }

  @Override
  public String getZooKeepers() {
    return zooKeepers.get();
  }

  @Override
  public int getZooKeepersSessionTimeOut() {
    return zooKeepersSessionTimeOut.get();
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
  public Properties getClientProperties() {
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
    return instanceName.get();
  }

  public Credentials getCredentials() {
    return credentials.get();
  }

  @Override
  public Configuration getHadoopConf() {
    return hadoopConf.get();
  }

  public ServerDirs getServerDirs() {
    return serverDirs.get();
  }

}

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
package org.apache.accumulo.core.client;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.ClientConfConverter;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientInfoImpl;
import org.apache.accumulo.core.clientImpl.InstanceOperationsImpl;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of instance that looks in zookeeper to find information needed to connect to an
 * instance of accumulo.
 *
 * <p>
 * The advantage of using zookeeper to obtain information about accumulo is that zookeeper is highly
 * available, very responsive, and supports caching.
 *
 * <p>
 * Because it is possible for multiple instances of accumulo to share a single set of zookeeper
 * servers, all constructors require an accumulo instance name.
 *
 * If you do not know the instance names then run accumulo
 * org.apache.accumulo.server.util.ListInstances on an accumulo server.
 *
 * @deprecated since 2.0.0, Use {@link Accumulo#newClient()} instead
 */
@Deprecated
public class ZooKeeperInstance implements Instance {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperInstance.class);

  private String instanceId = null;
  private String instanceName = null;

  private final ZooCache zooCache;

  private final String zooKeepers;

  private final int zooKeepersSessionTimeOut;

  private ClientConfiguration clientConf;

  /**
   *
   * @param instanceName
   *          The name of specific accumulo instance. This is set at initialization time.
   * @param zooKeepers
   *          A comma separated list of zoo keeper server locations. Each location can contain an
   *          optional port, of the format host:port.
   */
  public ZooKeeperInstance(String instanceName, String zooKeepers) {
    this(ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zooKeepers));
  }

  ZooKeeperInstance(ClientConfiguration config, ZooCacheFactory zcf) {
    checkArgument(config != null, "config is null");
    // Enable singletons before before getting a zoocache
    SingletonManager.setMode(Mode.CONNECTOR);
    this.clientConf = config;
    this.instanceId = clientConf.get(ClientConfiguration.ClientProperty.INSTANCE_ID);
    this.instanceName = clientConf.get(ClientConfiguration.ClientProperty.INSTANCE_NAME);
    if ((instanceId == null) == (instanceName == null))
      throw new IllegalArgumentException(
          "Expected exactly one of instanceName and instanceId to be set; "
              + (instanceName == null ? "neither" : "both") + " were set");
    this.zooKeepers = clientConf.get(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST);
    this.zooKeepersSessionTimeOut = (int) ConfigurationTypeHelper
        .getTimeInMillis(clientConf.get(ClientConfiguration.ClientProperty.INSTANCE_ZK_TIMEOUT));
    zooCache = zcf.getZooCache(zooKeepers, zooKeepersSessionTimeOut);
    if (instanceName != null) {
      // Validates that the provided instanceName actually exists
      getInstanceID();
    }
  }

  /**
   * @param config
   *          Client configuration for specifying connection options. See
   *          {@link ClientConfiguration} which extends Configuration with convenience methods
   *          specific to Accumulo.
   * @since 1.9.0
   */
  public ZooKeeperInstance(ClientConfiguration config) {
    this(config, new ZooCacheFactory());
  }

  @Override
  public String getInstanceID() {
    if (instanceId == null) {
      // want the instance id to be stable for the life of this instance object,
      // so only get it once
      String instanceNamePath = Constants.ZROOT + Constants.ZINSTANCES + "/" + instanceName;
      byte[] iidb = zooCache.get(instanceNamePath);
      if (iidb == null) {
        throw new RuntimeException(
            "Instance name " + instanceName + " does not exist in zookeeper. "
                + "Run \"accumulo org.apache.accumulo.server.util.ListInstances\" to see a list.");
      }
      instanceId = new String(iidb, UTF_8);
    }

    if (zooCache.get(Constants.ZROOT + "/" + instanceId) == null) {
      if (instanceName == null)
        throw new RuntimeException("Instance id " + instanceId + " does not exist in zookeeper");
      throw new RuntimeException("Instance id " + instanceId + " pointed to by the name "
          + instanceName + " does not exist in zookeeper");
    }

    return instanceId;
  }

  @Override
  public List<String> getMasterLocations() {
    String masterLocPath = ZooUtil.getRoot(getInstanceID()) + Constants.ZMASTER_LOCK;

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Looking up master location in zookeeper.", Thread.currentThread().getId());
      timer = new OpTimer().start();
    }

    byte[] loc = ZooUtil.getLockData(zooCache, masterLocPath);

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

  @Override
  public String getRootTabletLocation() {
    String zRootLocPath = ZooUtil.getRoot(getInstanceID()) + RootTable.ZROOT_TABLET_LOCATION;

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Looking up root tablet location in zookeeper.",
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

  @Override
  public String getInstanceName() {
    if (instanceName == null)
      instanceName =
          InstanceOperationsImpl.lookupInstanceName(zooCache, UUID.fromString(getInstanceID()));

    return instanceName;
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
  public Connector getConnector(String principal, AuthenticationToken token)
      throws AccumuloException, AccumuloSecurityException {
    Properties properties = ClientConfConverter.toProperties(clientConf);
    properties.setProperty(ClientProperty.AUTH_PRINCIPAL.getKey(), principal);
    properties.setProperty(ClientProperty.INSTANCE_NAME.getKey(), getInstanceName());
    return new org.apache.accumulo.core.clientImpl.ConnectorImpl(
        new ClientContext(new ClientInfoImpl(properties, token)));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(64);
    sb.append("ZooKeeperInstance: ").append(getInstanceName()).append(" ").append(getZooKeepers());
    return sb.toString();
  }
}

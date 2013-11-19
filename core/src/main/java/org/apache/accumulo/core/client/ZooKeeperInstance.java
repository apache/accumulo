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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.ConnectorImpl;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientConfiguration;
import org.apache.accumulo.core.conf.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * <p>
 * An implementation of instance that looks in zookeeper to find information needed to connect to an instance of accumulo.
 * 
 * <p>
 * The advantage of using zookeeper to obtain information about accumulo is that zookeeper is highly available, very responsive, and supports caching.
 * 
 * <p>
 * Because it is possible for multiple instances of accumulo to share a single set of zookeeper servers, all constructors require an accumulo instance name.
 * 
 * If you do not know the instance names then run accumulo org.apache.accumulo.server.util.ListInstances on an accumulo server.
 * 
 */

public class ZooKeeperInstance implements Instance {

  private static final Logger log = Logger.getLogger(ZooKeeperInstance.class);

  private String instanceId = null;
  private String instanceName = null;

  private final ZooCache zooCache;

  private final String zooKeepers;

  private final int zooKeepersSessionTimeOut;

  private AccumuloConfiguration accumuloConf;
  private ClientConfiguration clientConf;

  private volatile boolean closed = false;

  /**
   * 
   * @param instanceName
   *          The name of specific accumulo instance. This is set at initialization time.
   * @param zooKeepers
   *          A comma separated list of zoo keeper server locations. Each location can contain an optional port, of the format host:port.
   * @deprecated since 1.6.0; Use {@link #ZooKeeperInstance(ClientConfiguration)} instead.
   */
  @Deprecated
  public ZooKeeperInstance(String instanceName, String zooKeepers) {
    this(ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zooKeepers));
  }

  /**
   * 
   * @param instanceName
   *          The name of specific accumulo instance. This is set at initialization time.
   * @param zooKeepers
   *          A comma separated list of zoo keeper server locations. Each location can contain an optional port, of the format host:port.
   * @param sessionTimeout
   *          zoo keeper session time out in milliseconds.
   * @deprecated since 1.6.0; Use {@link #ZooKeeperInstance(ClientConfiguration)} instead.
   */
  @Deprecated
  public ZooKeeperInstance(String instanceName, String zooKeepers, int sessionTimeout) {
    this(ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zooKeepers).withZkTimeout(sessionTimeout));
  }

  /**
   * 
   * @param instanceId
   *          The UUID that identifies the accumulo instance you want to connect to.
   * @param zooKeepers
   *          A comma separated list of zoo keeper server locations. Each location can contain an optional port, of the format host:port.
   * @deprecated since 1.6.0; Use {@link #ZooKeeperInstance(ClientConfiguration)} instead.
   */
  @Deprecated
  public ZooKeeperInstance(UUID instanceId, String zooKeepers) {
    this(ClientConfiguration.loadDefault().withInstance(instanceId).withZkHosts(zooKeepers));
  }

  /**
   * 
   * @param instanceId
   *          The UUID that identifies the accumulo instance you want to connect to.
   * @param zooKeepers
   *          A comma separated list of zoo keeper server locations. Each location can contain an optional port, of the format host:port.
   * @param sessionTimeout
   *          zoo keeper session time out in milliseconds.
   * @deprecated since 1.6.0; Use {@link #ZooKeeperInstance(ClientConfiguration)} instead.
   */
  @Deprecated
  public ZooKeeperInstance(UUID instanceId, String zooKeepers, int sessionTimeout) {
    this(ClientConfiguration.loadDefault().withInstance(instanceId).withZkHosts(zooKeepers).withZkTimeout(sessionTimeout));
  }

  /**
   * @param config
   *          Client configuration for specifying connection options.
   *          See {@link ClientConfiguration} which extends Configuration with convenience methods specific to Accumulo.
   * @since 1.6.0
   */

  public ZooKeeperInstance(Configuration config) {
    ArgumentChecker.notNull(config);
    if (config instanceof ClientConfiguration) {
      this.clientConf = (ClientConfiguration)config;
    } else {
      this.clientConf = new ClientConfiguration(config);
    }
    this.instanceId = clientConf.get(ClientProperty.INSTANCE_ID);
    this.instanceName = clientConf.get(ClientProperty.INSTANCE_NAME);
    if ((instanceId == null) == (instanceName == null))
      throw new IllegalArgumentException("Expected exactly one of instanceName and instanceId to be set");
    this.zooKeepers = clientConf.get(ClientProperty.INSTANCE_ZK_HOST);
    this.zooKeepersSessionTimeOut = (int) AccumuloConfiguration.getTimeInMillis(clientConf.get(ClientProperty.INSTANCE_ZK_TIMEOUT));
    zooCache = ZooCache.getInstance(zooKeepers, zooKeepersSessionTimeOut);
    clientInstances.incrementAndGet();
  }

  @Override
  public String getInstanceID() {
    if (closed)
      throw new RuntimeException("ZooKeeperInstance has been closed.");
    if (instanceId == null) {
      // want the instance id to be stable for the life of this instance object,
      // so only get it once
      String instanceNamePath = Constants.ZROOT + Constants.ZINSTANCES + "/" + instanceName;
      byte[] iidb = zooCache.get(instanceNamePath);
      if (iidb == null) {
        throw new RuntimeException("Instance name " + instanceName
            + " does not exist in zookeeper.  Run \"accumulo org.apache.accumulo.server.util.ListInstances\" to see a list.");
      }
      instanceId = new String(iidb);
    }

    if (zooCache.get(Constants.ZROOT + "/" + instanceId) == null) {
      if (instanceName == null)
        throw new RuntimeException("Instance id " + instanceId + " does not exist in zookeeper");
      throw new RuntimeException("Instance id " + instanceId + " pointed to by the name " + instanceName + " does not exist in zookeeper");
    }

    return instanceId;
  }

  @Override
  public List<String> getMasterLocations() {
    if (closed)
      throw new RuntimeException("ZooKeeperInstance has been closed.");
    String masterLocPath = ZooUtil.getRoot(this) + Constants.ZMASTER_LOCK;

    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Looking up master location in zoocache.");
    byte[] loc = ZooUtil.getLockData(zooCache, masterLocPath);
    opTimer.stop("Found master at " + (loc == null ? null : new String(loc)) + " in %DURATION%");

    if (loc == null) {
      return Collections.emptyList();
    }

    return Collections.singletonList(new String(loc));
  }

  @Override
  public String getRootTabletLocation() {
    if (closed)
      throw new RuntimeException("ZooKeeperInstance has been closed.");
    String zRootLocPath = ZooUtil.getRoot(this) + RootTable.ZROOT_TABLET_LOCATION;

    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Looking up root tablet location in zookeeper.");
    byte[] loc = zooCache.get(zRootLocPath);
    opTimer.stop("Found root tablet at " + (loc == null ? null : new String(loc)) + " in %DURATION%");

    if (loc == null) {
      return null;
    }

    return new String(loc).split("\\|")[0];
  }

  @Override
  public String getInstanceName() {
    if (closed)
      throw new RuntimeException("ZooKeeperInstance has been closed.");
    if (instanceName == null)
      instanceName = lookupInstanceName(zooCache, UUID.fromString(getInstanceID()));

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
  @Deprecated
  public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, TextUtil.getBytes(new Text(pass.toString())));
  }

  @Override
  @Deprecated
  public Connector getConnector(String user, ByteBuffer pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, ByteBufferUtil.toBytes(pass));
  }

  @Override
  public Connector getConnector(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    if (closed)
      throw new RuntimeException("ZooKeeperInstance has been closed.");
    return new ConnectorImpl(this, new Credentials(principal, token));
  }

  @Override
  @Deprecated
  public Connector getConnector(String principal, byte[] pass) throws AccumuloException, AccumuloSecurityException {
    if (closed) {
      throw new RuntimeException("ZooKeeperInstance has been closed.");
    } else {
      return getConnector(principal, new PasswordToken(pass));
    }
  }

  @Override
  public AccumuloConfiguration getConfiguration() {
    if (accumuloConf == null) {
      accumuloConf = clientConf.getAccumuloConfiguration();
    }
    return accumuloConf;
  }

  @Override
  @Deprecated
  public void setConfiguration(AccumuloConfiguration conf) {
    this.accumuloConf = conf;
  }

  /**
   * Given a zooCache and instanceId, look up the instance name.
   */
  public static String lookupInstanceName(ZooCache zooCache, UUID instanceId) {
    ArgumentChecker.notNull(zooCache, instanceId);
    for (String name : zooCache.getChildren(Constants.ZROOT + Constants.ZINSTANCES)) {
      String instanceNamePath = Constants.ZROOT + Constants.ZINSTANCES + "/" + name;
      UUID iid = UUID.fromString(new String(zooCache.get(instanceNamePath)));
      if (iid.equals(instanceId)) {
        return name;
      }
    }
    return null;
  }

  static private final AtomicInteger clientInstances = new AtomicInteger(0);

  @Override
  public synchronized void close() throws AccumuloException {
    if (!closed && clientInstances.decrementAndGet() == 0) {
      try {
        zooCache.close();
        ThriftUtil.close();
      } catch (InterruptedException e) {
        clientInstances.incrementAndGet();
        throw new AccumuloException("Issues closing ZooKeeper.");
      }
    }
    closed = true;
  }

  @Override
  public void finalize() {
    // This method intentionally left blank. Users need to explicitly close Instances if they want things cleaned up nicely.
    if (!closed)
      log.warn("ZooKeeperInstance being cleaned up without being closed. Please remember to call close() before dereferencing to clean up threads.");
  }
}

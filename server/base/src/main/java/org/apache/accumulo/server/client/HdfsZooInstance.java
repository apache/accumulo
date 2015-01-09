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
package org.apache.accumulo.server.client;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.ConnectorImpl;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * An implementation of Instance that looks in HDFS and ZooKeeper to find the master and root tablet location.
 *
 */
public class HdfsZooInstance implements Instance {

  private HdfsZooInstance() {
    AccumuloConfiguration acuConf = ServerConfiguration.getSiteConfiguration();
    zooCache = new ZooCacheFactory().getZooCache(acuConf.get(Property.INSTANCE_ZK_HOST), (int) acuConf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT));
  }

  private static HdfsZooInstance cachedHdfsZooInstance = null;

  public static synchronized Instance getInstance() {
    if (cachedHdfsZooInstance == null)
      cachedHdfsZooInstance = new HdfsZooInstance();
    return cachedHdfsZooInstance;
  }

  private static ZooCache zooCache;
  private static String instanceId = null;
  private static final Logger log = Logger.getLogger(HdfsZooInstance.class);

  @Override
  public String getRootTabletLocation() {
    String zRootLocPath = ZooUtil.getRoot(this) + RootTable.ZROOT_TABLET_LOCATION;

    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Looking up root tablet location in zoocache.");

    byte[] loc = zooCache.get(zRootLocPath);

    opTimer.stop("Found root tablet at " + (loc == null ? null : new String(loc, UTF_8)) + " in %DURATION%");

    if (loc == null) {
      return null;
    }

    return new String(loc, UTF_8).split("\\|")[0];
  }

  @Override
  public List<String> getMasterLocations() {

    String masterLocPath = ZooUtil.getRoot(this) + Constants.ZMASTER_LOCK;

    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Looking up master location in zoocache.");

    byte[] loc = ZooLock.getLockData(zooCache, masterLocPath, null);

    opTimer.stop("Found master at " + (loc == null ? null : new String(loc, UTF_8)) + " in %DURATION%");

    if (loc == null) {
      return Collections.emptyList();
    }

    return Collections.singletonList(new String(loc, UTF_8));
  }

  @Override
  public String getInstanceID() {
    if (instanceId == null)
      _getInstanceID();
    return instanceId;
  }

  private static synchronized void _getInstanceID() {
    if (instanceId == null) {
      AccumuloConfiguration acuConf = ServerConfiguration.getSiteConfiguration();
      // InstanceID should be the same across all volumes, so just choose one
      VolumeManager fs;
      try {
        fs = VolumeManagerImpl.get();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      Path instanceIdPath = Accumulo.getAccumuloInstanceIdPath(fs);
      log.trace("Looking for instanceId from " + instanceIdPath);
      String instanceIdFromFile = ZooUtil.getInstanceIDFromHdfs(instanceIdPath, acuConf);
      instanceId = instanceIdFromFile;
    }
  }

  @Override
  public String getInstanceName() {
    return ZooKeeperInstance.lookupInstanceName(zooCache, UUID.fromString(getInstanceID()));
  }

  @Override
  public String getZooKeepers() {
    return ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_ZK_HOST);
  }

  @Override
  public int getZooKeepersSessionTimeOut() {
    return (int) ServerConfiguration.getSiteConfiguration().getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
  }

  @Override
  public Connector getConnector(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    return new ConnectorImpl(this, new Credentials(principal, token));
  }

  @Deprecated
  @Override
  public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, new PasswordToken(pass));
  }

  @Deprecated
  @Override
  public Connector getConnector(String user, ByteBuffer pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, ByteBufferUtil.toBytes(pass));
  }

  @Deprecated
  @Override
  public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, TextUtil.getBytes(new Text(pass.toString())));
  }

  private AccumuloConfiguration conf = null;

  @Deprecated
  @Override
  public AccumuloConfiguration getConfiguration() {
    if (conf == null)
      conf = new ServerConfiguration(this).getConfiguration();
    return conf;
  }

  @Override
  @Deprecated
  public void setConfiguration(AccumuloConfiguration conf) {
    this.conf = conf;
  }

  public static void main(String[] args) {
    Instance instance = HdfsZooInstance.getInstance();
    System.out.println("Instance Name: " + instance.getInstanceName());
    System.out.println("Instance ID: " + instance.getInstanceID());
    System.out.println("ZooKeepers: " + instance.getZooKeepers());
    System.out.println("Masters: " + StringUtil.join(instance.getMasterLocations(), ", "));
  }
}

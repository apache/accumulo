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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ConnectorImpl;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.InstanceOperationsImpl;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * An implementation of Instance that looks in HDFS and ZooKeeper to find the master and root tablet location.
 *
 */
public class HdfsZooInstance implements Instance {

  private final AccumuloConfiguration site = SiteConfiguration.getInstance();

  private HdfsZooInstance() {
    zooCache = new ZooCacheFactory().getZooCache(site.get(Property.INSTANCE_ZK_HOST), (int) site.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT));
  }

  private static final HdfsZooInstance cachedHdfsZooInstance = new HdfsZooInstance();

  public static Instance getInstance() {
    return cachedHdfsZooInstance;
  }

  private final ZooCache zooCache;
  private static String instanceId = null;
  private static final Logger log = LoggerFactory.getLogger(HdfsZooInstance.class);

  @Override
  public String getRootTabletLocation() {
    String zRootLocPath = ZooUtil.getRoot(this) + RootTable.ZROOT_TABLET_LOCATION;

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Looking up root tablet location in zoocache.", Thread.currentThread().getId());
      timer = new OpTimer().start();
    }

    byte[] loc = zooCache.get(zRootLocPath);

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Found root tablet at {} in {}", Thread.currentThread().getId(), (loc == null ? "null" : new String(loc, UTF_8)),
          String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
    }

    if (loc == null) {
      return null;
    }

    return new String(loc, UTF_8).split("\\|")[0];
  }

  @Override
  public List<String> getMasterLocations() {

    String masterLocPath = ZooUtil.getRoot(this) + Constants.ZMASTER_LOCK;

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Looking up master location in zoocache.", Thread.currentThread().getId());
      timer = new OpTimer().start();
    }

    byte[] loc = ZooLock.getLockData(zooCache, masterLocPath, null);

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Found master at {} in {}", Thread.currentThread().getId(), (loc == null ? "null" : new String(loc, UTF_8)),
          String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
    }

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
      AccumuloConfiguration acuConf = SiteConfiguration.getInstance();
      // InstanceID should be the same across all volumes, so just choose one
      VolumeManager fs;
      try {
        fs = VolumeManagerImpl.get();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      Path instanceIdPath = Accumulo.getAccumuloInstanceIdPath(fs);
      log.trace("Looking for instanceId from {}", instanceIdPath);
      String instanceIdFromFile = ZooUtil.getInstanceIDFromHdfs(instanceIdPath, acuConf);
      instanceId = instanceIdFromFile;
    }
  }

  @Override
  public String getInstanceName() {
    return InstanceOperationsImpl.lookupInstanceName(zooCache, UUID.fromString(getInstanceID()));
  }

  @Override
  public String getZooKeepers() {
    return site.get(Property.INSTANCE_ZK_HOST);
  }

  @Override
  public int getZooKeepersSessionTimeOut() {
    return (int) site.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
  }

  @Override
  public Connector getConnector(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    return new ConnectorImpl(new ClientContext(this, new Credentials(principal, token), site));
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

  public static void main(String[] args) {
    Instance instance = HdfsZooInstance.getInstance();
    System.out.println("Instance Name: " + instance.getInstanceName());
    System.out.println("Instance ID: " + instance.getInstanceID());
    System.out.println("ZooKeepers: " + instance.getZooKeepers());
    System.out.println("Masters: " + Joiner.on(", ").join(instance.getMasterLocations()));
  }
}

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
package org.apache.accumulo.test;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ServerConfigurationUtil;
import org.apache.accumulo.core.client.impl.ThriftTransportKey;
import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.client.impl.ThriftTransportPool.CachedTTransport;
import org.apache.accumulo.core.client.impl.ThriftTransportPool.Closer;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.util.SslConnectionParams;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that {@link ThriftTransportPool} actually adheres to the cachedConnection argument
 */
public class TransportCachingIT extends AccumuloClusterIT {
  private static final Logger log = LoggerFactory.getLogger(TransportCachingIT.class);

  private ArrayList<ThriftTransportKey> getTServers(Instance instance, boolean oneway) {
    long rpcTimeout = DefaultConfiguration.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT.getDefaultValue());
    // create list of servers
    ArrayList<ThriftTransportKey> servers = new ArrayList<ThriftTransportKey>();

    // add tservers
    ZooCache zc = new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    for (String tserver : zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZTSERVERS)) {
      String path = ZooUtil.getRoot(instance) + Constants.ZTSERVERS + "/" + tserver;
      byte[] data = ZooUtil.getLockData(zc, path);
      if (data != null && !new String(data, UTF_8).equals("master"))
        servers.add(new ThriftTransportKey(new ServerServices(new String(data)).getAddressString(Service.TSERV_CLIENT), rpcTimeout, SslConnectionParams
            .forClient(ServerConfigurationUtil.getConfiguration(instance)), oneway));
    }

    return servers;
  }

  @Test
  public void testCachedTransport() {
    final boolean ONEWAY = false;
    Connector conn = getConnector();
    Instance instance = conn.getInstance();

    // create list of servers
    ArrayList<ThriftTransportKey> servers = getTServers(instance, ONEWAY);

    ThriftTransportPool pool = ThriftTransportPool.getInstance();
    TTransport first = null;
    while (null == first) {
      try {
        // Get a transport (cached or not)
        first = pool.getAnyTransport(servers, true, ONEWAY).getSecond();
      } catch (TTransportException e) {
        log.warn("Failed to obtain transport to " + servers);
      }
    }

    assertNotNull(first);
    // Return it to unreserve it
    pool.returnTransport(first);

    TTransport second = null;
    while (null == second) {
      try {
        // Get a cached transport (should be the first)
        second = pool.getAnyTransport(servers, true, ONEWAY).getSecond();
      } catch (TTransportException e) {
        log.warn("Failed obtain 2nd transport to " + servers);
      }
    }

    // We should get the same transport
    assertTrue("Expected the first and second to be the same instance", first == second);
    // Return the 2nd
    pool.returnTransport(second);

    TTransport third = null;
    while (null == third) {
      try {
        // Get a non-cached transport
        third = pool.getAnyTransport(servers, false, ONEWAY).getSecond();
      } catch (TTransportException e) {
        log.warn("Failed obtain 2nd transport to " + servers);
      }
    }

    assertFalse("Expected second and third transport to be different instances", second == third);
    pool.returnTransport(third);
  }

  @Test
  public void onewayCaching() throws Exception {
    final Connector conn = getConnector();
    final Instance instance = conn.getInstance();

    // create list of servers
    final ArrayList<ThriftTransportKey> onewayServers = getTServers(instance, true);
    final ArrayList<ThriftTransportKey> normalServers = getTServers(instance, false);
    final ThriftTransportPool pool = ThriftTransportPool.getInstance();

    TTransport oneway = null;
    while (null == oneway) {
      try {
        // Get a transport (cached or not)
        oneway = pool.getAnyTransport(onewayServers, true, true).getSecond();
      } catch (TTransportException e) {
        log.warn("Failed to obtain transport to " + onewayServers);
      }
    }

    assertNotNull(oneway);

    TTransport secondOneway = null;
    while (null == secondOneway) {
      try {
        secondOneway = pool.getAnyTransport(onewayServers, true, true).getSecond();
      } catch (TTransportException e) {
        log.warn("Failed to obtain transport to " + onewayServers);
      }
    }

    // The first connection is reserved
    assertFalse(oneway == secondOneway);
    pool.returnTransport(oneway);

    TTransport thirdOneway = null;
    while (null == thirdOneway) {
      try {
        thirdOneway = pool.getAnyTransport(onewayServers, true, true).getSecond();
      } catch (TTransportException e) {
        log.warn("Failed to obtain transport to " + onewayServers);
      }
    }

    // After the first is returned, another request should get it back.
    assertTrue(oneway == thirdOneway);

    TTransport normalTransport = null;
    while (null == normalTransport) {
      try {
        normalTransport = pool.getAnyTransport(normalServers, true, false).getSecond();
      } catch (TTransportException e) {
        log.warn("Failed to obtain transport to " + normalServers);
      }
    }

    // We should get a transport
    assertNotNull(normalTransport);
    // It should *not* be the same instance as we got when asking for oneway.
    assertFalse(secondOneway == normalTransport);
    assertFalse(thirdOneway == normalTransport);
  }

  @Test
  public void connectionClosing() throws Exception {
    final long KILL_TIME = 1000;
    final Connector conn = getConnector();
    final Instance instance = conn.getInstance();

    // create list of servers
    final ArrayList<ThriftTransportKey> onewayServers = getTServers(instance, true);
    final ThriftTransportPool pool = new ThriftTransportPool(KILL_TIME);

    TTransport oneway = null;
    while (null == oneway) {
      try {
        oneway = pool.getAnyTransport(onewayServers, true, true).getSecond();
      } catch (TTransportException e) {
        log.warn("Failed to obtain transport to " + onewayServers);
      }
    }

    assertNotNull(oneway);
    assertTrue(oneway instanceof CachedTTransport);
    pool.returnTransport(oneway);

    final CountDownLatch isRunning = new CountDownLatch(1);
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicBoolean keepRunning = new AtomicBoolean(true);
    final Closer closer = new ThriftTransportPool.Closer(pool, latch, keepRunning);
    Thread t = new Thread(new Runnable() {
      @Override public void run() {
        isRunning.countDown();
        closer.run();
      }
    });
    try {
      t.start();

      isRunning.await();

      // Wait for a sufficient time for it to be "idle" and reaped
      Thread.sleep(KILL_TIME * 4);

      ThriftTransportKey ttk = ((CachedTTransport) oneway).getCacheKey();
      assertEquals("Transport should have been evicted", 0, pool.getOnewayCache().get(ttk).size());

      TTransport newOneway = null;
      while (null == newOneway) {
        try {
          // Get a transport (cached or not)
          newOneway = pool.getAnyTransport(onewayServers, true, true).getSecond();
        } catch (TTransportException e) {
          log.warn("Failed to obtain transport to " + onewayServers);
        }
      }

      assertNotNull(newOneway);
      assertFalse("After one transport was closed, we should get a new one", oneway == newOneway);
    } finally {
      keepRunning.set(false);
      if (null != t) {
        while (t.isAlive()) {
          Thread.sleep(500);
        }
      }
    }
  }
}

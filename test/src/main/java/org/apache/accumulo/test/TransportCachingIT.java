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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.ThriftTransportKey;
import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that {@link ThriftTransportPool} actually adheres to the cachedConnection argument
 */
public class TransportCachingIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(TransportCachingIT.class);

  @Test
  public void testCachedTransport() {
    Connector conn = getConnector();
    Instance instance = conn.getInstance();
    ClientConfiguration clientConf = cluster.getClientConfig();
    ClientContext context = new ClientContext(instance, new Credentials(getAdminPrincipal(), getAdminToken()), clientConf);
    long rpcTimeout = ConfigurationTypeHelper.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT.getDefaultValue());

    // create list of servers
    ArrayList<ThriftTransportKey> servers = new ArrayList<>();

    // add tservers
    ZooCache zc = new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    for (String tserver : zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZTSERVERS)) {
      String path = ZooUtil.getRoot(instance) + Constants.ZTSERVERS + "/" + tserver;
      byte[] data = ZooUtil.getLockData(zc, path);
      if (data != null) {
        String strData = new String(data, UTF_8);
        if (!strData.equals("master"))
          servers.add(new ThriftTransportKey(new ServerServices(strData).getAddress(Service.TSERV_CLIENT), rpcTimeout, context));
      }
    }

    ThriftTransportPool pool = ThriftTransportPool.getInstance();
    TTransport first = null;
    while (null == first) {
      try {
        // Get a transport (cached or not)
        first = pool.getAnyTransport(servers, true).getSecond();
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
        second = pool.getAnyTransport(servers, true).getSecond();
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
        third = pool.getAnyTransport(servers, false).getSecond();
      } catch (TTransportException e) {
        log.warn("Failed obtain 2nd transport to " + servers);
      }
    }

    assertFalse("Expected second and third transport to be different instances", second == third);
    pool.returnTransport(third);
  }
}

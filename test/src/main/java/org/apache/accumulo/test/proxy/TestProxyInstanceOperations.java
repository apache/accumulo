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
package org.apache.accumulo.test.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.proxy.Proxy;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestProxyInstanceOperations {
  private static final Logger log = LoggerFactory.getLogger(TestProxyInstanceOperations.class);

  protected static TServer proxy;
  protected static TestProxyClient tpc;
  protected static ByteBuffer userpass;
  protected static final int port = 10197;

  @BeforeClass
  public static void setup() throws Exception {
    Properties prop = new Properties();
    prop.setProperty("useMockInstance", "true");
    prop.put("tokenClass", PasswordToken.class.getName());

    proxy = Proxy.createProxyServer(HostAndPort.fromParts("localhost", port), new TCompactProtocol.Factory(), prop).server;
    log.info("Waiting for proxy to start");
    while (!proxy.isServing()) {
      Thread.sleep(500);
    }
    log.info("Proxy started");
    tpc = new TestProxyClient("localhost", port);
    userpass = tpc.proxy.login("root", Collections.singletonMap("password", ""));
  }

  @AfterClass
  public static void tearDown() throws InterruptedException {
    proxy.stop();
  }

  @Test
  public void properties() throws TException {
    tpc.proxy().setProperty(userpass, "test.systemprop", "whistletips");

    assertEquals(tpc.proxy().getSystemConfiguration(userpass).get("test.systemprop"), "whistletips");
    tpc.proxy().removeProperty(userpass, "test.systemprop");
    assertNull(tpc.proxy().getSystemConfiguration(userpass).get("test.systemprop"));

  }

  @Test
  public void testClassLoad() throws TException {
    assertTrue(tpc.proxy().testClassLoad(userpass, "org.apache.accumulo.core.iterators.user.RegExFilter", "org.apache.accumulo.core.iterators.Filter"));
  }

}

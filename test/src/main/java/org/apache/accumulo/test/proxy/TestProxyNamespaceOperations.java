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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.proxy.Proxy;
import org.apache.accumulo.proxy.thrift.AccumuloException;
import org.apache.accumulo.proxy.thrift.IteratorScope;
import org.apache.accumulo.proxy.thrift.IteratorSetting;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestProxyNamespaceOperations {

  protected static TServer proxy;
  protected static TestProxyClient tpc;
  protected static ByteBuffer userpass;
  protected static final int port = 10198;
  protected static final String testnamespace = "testns";

  @BeforeClass
  public static void setup() throws Exception {
    Properties prop = new Properties();
    prop.setProperty("useMockInstance", "true");
    prop.put("tokenClass", PasswordToken.class.getName());

    proxy = Proxy.createProxyServer(HostAndPort.fromParts("localhost", port), new TCompactProtocol.Factory(), prop).server;
    while (!proxy.isServing()) {
      Thread.sleep(500);
    }
    tpc = new TestProxyClient("localhost", port);
    userpass = tpc.proxy().login("root", Collections.singletonMap("password", ""));
  }

  @AfterClass
  public static void tearDown() throws InterruptedException {
    proxy.stop();
  }

  @Before
  public void makeTestNamespace() throws Exception {
    tpc.proxy().createNamespace(userpass, testnamespace);
  }

  @After
  public void deleteTestNamespace() throws Exception {
    tpc.proxy().deleteNamespace(userpass, testnamespace);
  }

  @Test
  public void createExistsDelete() throws TException {
    tpc.proxy().createNamespace(userpass, "testns2");
    assertTrue(tpc.proxy().namespaceExists(userpass, "testns2"));
    tpc.proxy().deleteNamespace(userpass, "testns2");
    assertFalse(tpc.proxy().namespaceExists(userpass, "testns2"));
  }

  @Test
  public void listRename() throws TException {
    assertFalse(tpc.proxy().namespaceExists(userpass, "testns2"));
    tpc.proxy().renameNamespace(userpass, testnamespace, "testns2");
    assertTrue(tpc.proxy().namespaceExists(userpass, "testns2"));
    tpc.proxy().renameNamespace(userpass, "testns2", testnamespace);
    assertTrue(tpc.proxy().listNamespaces(userpass).contains(testnamespace));
    assertFalse(tpc.proxy().listNamespaces(userpass).contains("testns2"));
  }

  @Test
  public void systemDefault() throws TException {
    assertEquals(tpc.proxy().systemNamespace(), Namespace.ACCUMULO);
    assertEquals(tpc.proxy().defaultNamespace(), Namespace.DEFAULT);
  }

  @Test
  public void namespaceProperties() throws TException {
    tpc.proxy().setNamespaceProperty(userpass, testnamespace, "test.property1", "wharrrgarbl");
    assertEquals(tpc.proxy().getNamespaceProperties(userpass, testnamespace).get("test.property1"), "wharrrgarbl");
    tpc.proxy().removeNamespaceProperty(userpass, testnamespace, "test.property1");
    assertNull(tpc.proxy().getNamespaceProperties(userpass, testnamespace).get("test.property1"));
  }

  @Ignore("MockInstance doesn't return expected results for this function.")
  @Test
  public void namespaceIds() throws TException {
    assertTrue(tpc.proxy().namespaceIdMap(userpass).containsKey("accumulo"));
    assertEquals(tpc.proxy().namespaceIdMap(userpass).get("accumulo"), "+accumulo");
  }

  @Test
  public void namespaceIterators() throws TException {
    IteratorSetting setting = new IteratorSetting(40, "DebugTheThings", "org.apache.accumulo.core.iterators.DebugIterator", new HashMap<String,String>());
    Set<IteratorScope> scopes = new HashSet<>();
    scopes.add(IteratorScope.SCAN);
    tpc.proxy().attachNamespaceIterator(userpass, testnamespace, setting, scopes);
    assertEquals(setting, tpc.proxy().getNamespaceIteratorSetting(userpass, testnamespace, "DebugTheThings", IteratorScope.SCAN));
    assertTrue(tpc.proxy().listNamespaceIterators(userpass, testnamespace).containsKey("DebugTheThings"));
    Set<IteratorScope> scopes2 = new HashSet<>();
    scopes2.add(IteratorScope.MINC);
    tpc.proxy().checkNamespaceIteratorConflicts(userpass, testnamespace, setting, scopes2);
    tpc.proxy().removeNamespaceIterator(userpass, testnamespace, "DebugTheThings", scopes);
    assertFalse(tpc.proxy().listNamespaceIterators(userpass, testnamespace).containsKey("DebugTheThings"));
  }

  @Test(expected = AccumuloException.class)
  public void namespaceIteratorConflict() throws TException {
    IteratorSetting setting = new IteratorSetting(40, "DebugTheThings", "org.apache.accumulo.core.iterators.DebugIterator", new HashMap<String,String>());
    Set<IteratorScope> scopes = new HashSet<>();
    scopes.add(IteratorScope.SCAN);
    tpc.proxy().attachNamespaceIterator(userpass, testnamespace, setting, scopes);
    tpc.proxy().checkNamespaceIteratorConflicts(userpass, testnamespace, setting, scopes);
  }

  @Test
  public void namespaceConstraints() throws TException {
    int constraintId = tpc.proxy().addNamespaceConstraint(userpass, testnamespace, "org.apache.accumulo.test.constraints.MaxMutationSize");
    assertTrue(tpc.proxy().listNamespaceConstraints(userpass, testnamespace).containsKey("org.apache.accumulo.test.constraints.MaxMutationSize"));
    assertEquals(constraintId, (int) tpc.proxy().listNamespaceConstraints(userpass, testnamespace).get("org.apache.accumulo.test.constraints.MaxMutationSize"));
    tpc.proxy().removeNamespaceConstraint(userpass, testnamespace, constraintId);
    assertFalse(tpc.proxy().listNamespaceConstraints(userpass, testnamespace).containsKey("org.apache.accumulo.test.constraints.MaxMutationSize"));
  }

  @Test
  public void classLoad() throws TException {
    assertTrue(tpc.proxy().testNamespaceClassLoad(userpass, testnamespace, "org.apache.accumulo.core.iterators.user.VersioningIterator",
        "org.apache.accumulo.core.iterators.SortedKeyValueIterator"));
    assertFalse(tpc.proxy().testNamespaceClassLoad(userpass, testnamespace, "org.apache.accumulo.core.iterators.user.VersioningIterator", "dummy"));
  }
}

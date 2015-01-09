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
package org.apache.accumulo.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.proxy.thrift.ColumnUpdate;
import org.apache.accumulo.proxy.thrift.TimeType;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestProxyTableOperations {

  protected static TServer proxy;
  protected static Thread thread;
  protected static TestProxyClient tpc;
  protected static ByteBuffer userpass;
  protected static final int port = 10195;
  protected static final String testtable = "testtable";

  @SuppressWarnings("serial")
  @BeforeClass
  public static void setup() throws Exception {
    Properties prop = new Properties();
    prop.setProperty("useMockInstance", "true");
    prop.put("tokenClass", PasswordToken.class.getName());

    proxy = Proxy.createProxyServer(Class.forName("org.apache.accumulo.proxy.thrift.AccumuloProxy"), Class.forName("org.apache.accumulo.proxy.ProxyServer"),
        port, TCompactProtocol.Factory.class, prop);
    thread = new Thread() {
      @Override
      public void run() {
        proxy.serve();
      }
    };
    thread.start();
    tpc = new TestProxyClient("localhost", port);
    userpass = tpc.proxy().login("root", new TreeMap<String,String>() {
      {
        put("password", "");
      }
    });
  }

  @AfterClass
  public static void tearDown() throws InterruptedException {
    proxy.stop();
    thread.join();
  }

  @Before
  public void makeTestTable() throws Exception {
    tpc.proxy().createTable(userpass, testtable, true, TimeType.MILLIS);
  }

  @After
  public void deleteTestTable() throws Exception {
    tpc.proxy().deleteTable(userpass, testtable);
  }

  @Test
  public void createExistsDelete() throws TException {
    assertFalse(tpc.proxy().tableExists(userpass, "testtable2"));
    tpc.proxy().createTable(userpass, "testtable2", true, TimeType.MILLIS);
    assertTrue(tpc.proxy().tableExists(userpass, "testtable2"));
    tpc.proxy().deleteTable(userpass, "testtable2");
    assertFalse(tpc.proxy().tableExists(userpass, "testtable2"));
  }

  @Test
  public void listRename() throws TException {
    assertFalse(tpc.proxy().tableExists(userpass, "testtable2"));
    tpc.proxy().renameTable(userpass, testtable, "testtable2");
    assertTrue(tpc.proxy().tableExists(userpass, "testtable2"));
    tpc.proxy().renameTable(userpass, "testtable2", testtable);
    assertTrue(tpc.proxy().listTables(userpass).contains("testtable"));

  }

  // This test does not yet function because the backing Mock instance does not yet support merging
  @Test
  public void merge() throws TException {
    Set<ByteBuffer> splits = new HashSet<ByteBuffer>();
    splits.add(ByteBuffer.wrap("a".getBytes()));
    splits.add(ByteBuffer.wrap("c".getBytes()));
    splits.add(ByteBuffer.wrap("z".getBytes()));
    tpc.proxy().addSplits(userpass, testtable, splits);

    tpc.proxy().mergeTablets(userpass, testtable, ByteBuffer.wrap("b".getBytes()), ByteBuffer.wrap("d".getBytes()));

    splits.remove(ByteBuffer.wrap("c".getBytes()));

    List<ByteBuffer> tableSplits = tpc.proxy().listSplits(userpass, testtable, 10);

    for (ByteBuffer split : tableSplits)
      assertTrue(splits.contains(split));
    assertTrue(tableSplits.size() == splits.size());

  }

  @Test
  public void splits() throws TException {
    Set<ByteBuffer> splits = new HashSet<ByteBuffer>();
    splits.add(ByteBuffer.wrap("a".getBytes()));
    splits.add(ByteBuffer.wrap("b".getBytes()));
    splits.add(ByteBuffer.wrap("z".getBytes()));
    tpc.proxy().addSplits(userpass, testtable, splits);

    List<ByteBuffer> tableSplits = tpc.proxy().listSplits(userpass, testtable, 10);

    for (ByteBuffer split : tableSplits)
      assertTrue(splits.contains(split));
    assertTrue(tableSplits.size() == splits.size());
  }

  @Test
  public void constraints() throws TException {
    int cid = tpc.proxy().addConstraint(userpass, testtable, "org.apache.accumulo.TestConstraint");
    Map<String,Integer> constraints = tpc.proxy().listConstraints(userpass, testtable);
    assertEquals((int) constraints.get("org.apache.accumulo.TestConstraint"), cid);
    tpc.proxy().removeConstraint(userpass, testtable, cid);
    constraints = tpc.proxy().listConstraints(userpass, testtable);
    assertNull(constraints.get("org.apache.accumulo.TestConstraint"));
  }

  @Test
  public void localityGroups() throws TException {
    Map<String,Set<String>> groups = new HashMap<String,Set<String>>();
    Set<String> group1 = new HashSet<String>();
    group1.add("cf1");
    groups.put("group1", group1);
    Set<String> group2 = new HashSet<String>();
    group2.add("cf2");
    group2.add("cf3");
    groups.put("group2", group2);
    tpc.proxy().setLocalityGroups(userpass, testtable, groups);

    Map<String,Set<String>> actualGroups = tpc.proxy().getLocalityGroups(userpass, testtable);

    assertEquals(groups.size(), actualGroups.size());
    for (String groupName : groups.keySet()) {
      assertTrue(actualGroups.containsKey(groupName));
      assertEquals(groups.get(groupName).size(), actualGroups.get(groupName).size());
      for (String cf : groups.get(groupName)) {
        assertTrue(actualGroups.get(groupName).contains(cf));
      }
    }
  }

  @Test
  public void tableProperties() throws TException {
    tpc.proxy().setTableProperty(userpass, testtable, "test.property1", "wharrrgarbl");
    assertEquals(tpc.proxy().getTableProperties(userpass, testtable).get("test.property1"), "wharrrgarbl");
    tpc.proxy().removeTableProperty(userpass, testtable, "test.property1");
    assertNull(tpc.proxy().getTableProperties(userpass, testtable).get("test.property1"));
  }

  private static void addMutation(Map<ByteBuffer,List<ColumnUpdate>> mutations, String row, String cf, String cq, String value) {
    ColumnUpdate update = new ColumnUpdate(ByteBuffer.wrap(cf.getBytes()), ByteBuffer.wrap(cq.getBytes()));
    update.setValue(value.getBytes());
    mutations.put(ByteBuffer.wrap(row.getBytes()), Collections.singletonList(update));
  }

  @Test
  public void tableOperationsRowMethods() throws TException {
    Map<ByteBuffer,List<ColumnUpdate>> mutations = new HashMap<ByteBuffer,List<ColumnUpdate>>();
    for (int i = 0; i < 10; i++) {
      addMutation(mutations, "" + i, "cf", "cq", "");
    }
    tpc.proxy().updateAndFlush(userpass, testtable, mutations);

    assertEquals(tpc.proxy().getMaxRow(userpass, testtable, null, null, true, null, true), ByteBuffer.wrap("9".getBytes()));

    tpc.proxy().deleteRows(userpass, testtable, ByteBuffer.wrap("51".getBytes()), ByteBuffer.wrap("99".getBytes()));
    assertEquals(tpc.proxy().getMaxRow(userpass, testtable, null, null, true, null, true), ByteBuffer.wrap("5".getBytes()));
  }

}

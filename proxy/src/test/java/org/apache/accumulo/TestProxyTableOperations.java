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
package org.apache.accumulo;

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

import org.apache.accumulo.proxy.Proxy;
import org.apache.accumulo.proxy.TestProxyClient;
import org.apache.accumulo.proxy.thrift.PColumnUpdate;
import org.apache.accumulo.proxy.thrift.UserPass;
import org.apache.thrift.TException;
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
  protected static UserPass userpass;
  protected static final int port = 10195;
  protected static final String testtable = "testtable";
  
  @BeforeClass
  public static void setup() throws Exception {
    Properties prop = new Properties();
    prop.setProperty("org.apache.accumulo.proxy.ProxyServer.useMockInstance", "true");
    
    proxy = Proxy.createProxyServer(Class.forName("org.apache.accumulo.proxy.thrift.AccumuloProxy"),
        Class.forName("org.apache.accumulo.proxy.ProxyServer"), port, prop);
    thread = new Thread() {
      @Override
      public void run() {
        proxy.serve();
      }
    };
    thread.start();
    tpc = new TestProxyClient("localhost", port);
    userpass = new UserPass("root", ByteBuffer.wrap("".getBytes()));
  }
  
  @AfterClass
  public static void tearDown() throws InterruptedException {
    proxy.stop();
    thread.join();
  }
  
  @Before
  public void makeTestTable() throws Exception {
    tpc.proxy().tableOperations_create(userpass, testtable);
  }
  
  @After
  public void deleteTestTable() throws Exception {
    tpc.proxy().tableOperations_delete(userpass, testtable);
  }
  
  @Test
  public void ping() throws Exception {
    tpc.proxy().ping(userpass);
  }
  
  @Test
  public void createExistsDelete() throws TException {
    assertFalse(tpc.proxy().tableOperations_exists(userpass, "testtable2"));
    tpc.proxy().tableOperations_create(userpass, "testtable2");
    assertTrue(tpc.proxy().tableOperations_exists(userpass, "testtable2"));
    tpc.proxy().tableOperations_delete(userpass, "testtable2");
    assertFalse(tpc.proxy().tableOperations_exists(userpass, "testtable2"));
  }
  
  @Test
  public void listRename() throws TException {
    assertFalse(tpc.proxy().tableOperations_exists(userpass, "testtable2"));
    tpc.proxy().tableOperations_rename(userpass, testtable, "testtable2");
    assertTrue(tpc.proxy().tableOperations_exists(userpass, "testtable2"));
    tpc.proxy().tableOperations_rename(userpass, "testtable2", testtable);
    assertTrue(tpc.proxy().tableOperations_list(userpass).contains("testtable"));
    
  }
  
  // This test does not yet function because the backing Mock instance does not yet support merging
  // TODO: add back in as a test when Mock is improved
  // @Test
  public void merge() throws TException {
    Set<String> splits = new HashSet<String>();
    splits.add("a");
    splits.add("c");
    splits.add("z");
    tpc.proxy().tableOperations_addSplits(userpass, testtable, splits);
    
    tpc.proxy().tableOperations_merge(userpass, testtable, "b", "d");
    
    splits.remove("c");
    
    List<String> tableSplits = tpc.proxy().tableOperations_getSplits(userpass, testtable, 10);
    
    for (String split : tableSplits)
      assertTrue(splits.contains(split));
    assertTrue(tableSplits.size() == splits.size());
    
  }
  
  @Test
  public void splits() throws TException {
    Set<String> splits = new HashSet<String>();
    splits.add("a");
    splits.add("b");
    splits.add("z");
    tpc.proxy().tableOperations_addSplits(userpass, testtable, splits);
    
    List<String> tableSplits = tpc.proxy().tableOperations_getSplits(userpass, testtable, 10);
    
    for (String split : tableSplits)
      assertTrue(splits.contains(split));
    assertTrue(tableSplits.size() == splits.size());
  }
  
  @Test
  public void constraints() throws TException {
    int cid = tpc.proxy().tableOperations_addConstraint(userpass, testtable, "org.apache.accumulo.TestConstraint");
    Map<String,Integer> constraints = tpc.proxy().tableOperations_listConstraints(userpass, testtable);
    assertEquals((int) constraints.get("org.apache.accumulo.TestConstraint"), cid);
    tpc.proxy().tableOperations_removeConstraint(userpass, testtable, cid);
    constraints = tpc.proxy().tableOperations_listConstraints(userpass, testtable);
    assertNull(constraints.get("org.apache.accumulo.TestConstraint"));
  }
  
  // This test does not yet function because the backing Mock instance does not yet support locality groups
  // TODO: add back in as a test when Mock is improved
  // @Test
  public void localityGroups() throws TException {
    Map<String,Set<String>> groups = new HashMap<String,Set<String>>();
    Set<String> group1 = new HashSet<String>();
    group1.add("cf1");
    groups.put("group1", group1);
    Set<String> group2 = new HashSet<String>();
    group2.add("cf2");
    group2.add("cf3");
    groups.put("group2", group2);
    tpc.proxy().tableOperations_setLocalityGroups(userpass, testtable, groups);
    
    Map<String,Set<String>> actualGroups = tpc.proxy().tableOperations_getLocalityGroups(userpass, testtable);
    
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
    tpc.proxy().tableOperations_setProperty(userpass, testtable, "test.property1", "wharrrgarbl");
    assertEquals(tpc.proxy().tableOperations_getProperties(userpass, testtable).get("test.property1"), "wharrrgarbl");
    tpc.proxy().tableOperations_removeProperty(userpass, testtable, "test.property1");
    assertNull(tpc.proxy().tableOperations_getProperties(userpass, testtable).get("test.property1"));
  }
  
  private static void addMutation(Map<ByteBuffer,List<PColumnUpdate>> mutations, String row, String cf, String cq, String value) {
    PColumnUpdate update = new PColumnUpdate(ByteBuffer.wrap(cf.getBytes()), ByteBuffer.wrap(cq.getBytes()), ByteBuffer.wrap(value.getBytes()));
    mutations.put(ByteBuffer.wrap(row.getBytes()), Collections.singletonList(update));
  }
  
  @Test
  public void tableOperationsRowMethods() throws TException {
    List<ByteBuffer> auths = tpc.proxy().securityOperations_getUserAuthorizations(userpass, "root");
    // System.out.println(auths);
    Map<ByteBuffer,List<PColumnUpdate>> mutations = new HashMap<ByteBuffer,List<PColumnUpdate>>();
    for (int i = 0; i < 10; i++) {
      addMutation(mutations, "" + i, "cf", "cq", "");
    }
    tpc.proxy().updateAndFlush(userpass, testtable, mutations, null);
    
    assertEquals(tpc.proxy().tableOperations_getMaxRow(userpass, testtable, auths, null, true, null, true), "9");
    
    // TODO: Uncomment when the Mock isn't broken
    // tpc.proxy().tableOperations_deleteRows(userpass,testtable,"51","99");
    // assertEquals(tpc.proxy().tableOperations_getMaxRow(userpass, testtable, auths, null, true, null, true),"5");
    
  }
  
  /*
   * @Test(expected = TException.class) public void peekTest() { }
   */
}

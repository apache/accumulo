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
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.proxy.Proxy;
import org.apache.accumulo.proxy.TestProxyClient;
import org.apache.accumulo.proxy.thrift.PSystemPermission;
import org.apache.accumulo.proxy.thrift.PTablePermission;
import org.apache.accumulo.proxy.thrift.PTimeType;
import org.apache.accumulo.proxy.thrift.UserPass;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestProxySecurityOperations {
  protected static TServer proxy;
  protected static Thread thread;
  protected static TestProxyClient tpc;
  protected static UserPass userpass;
  protected static final int port = 10196;
  protected static final String testtable = "testtable";
  protected static final String testuser = "VonJines";
  protected static final ByteBuffer testpw = ByteBuffer.wrap("fiveones".getBytes());
  
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
  public void makeTestTableAndUser() throws Exception {
    tpc.proxy().tableOperations_create(userpass, testtable, true, PTimeType.MILLIS);
    tpc.proxy().securityOperations_createUser(userpass, testuser, testpw);
  }
  
  @After
  public void deleteTestTable() throws Exception {
    tpc.proxy().tableOperations_delete(userpass, testtable);
    tpc.proxy().securityOperations_dropUser(userpass, testuser);
  }
  
  @Test
  public void create() throws TException {
    tpc.proxy().securityOperations_createUser(userpass, testuser + "2", testpw);
    assertTrue(tpc.proxy().securityOperations_listUsers(userpass).contains(testuser + "2"));
    tpc.proxy().securityOperations_dropUser(userpass, testuser + "2");
    assertTrue(!tpc.proxy().securityOperations_listUsers(userpass).contains(testuser + "2"));
  }
  
  @Test
  public void authenticate() throws TException {
    assertTrue(tpc.proxy().securityOperations_authenticateUser(userpass, testuser, testpw));
    assertFalse(tpc.proxy().securityOperations_authenticateUser(userpass, "EvilUser", testpw));
    
    tpc.proxy().securityOperations_changeUserPassword(userpass, testuser, ByteBuffer.wrap("newpass".getBytes()));
    assertFalse(tpc.proxy().securityOperations_authenticateUser(userpass, testuser, testpw));
    assertTrue(tpc.proxy().securityOperations_authenticateUser(userpass, testuser, ByteBuffer.wrap("newpass".getBytes())));
    
  }
  
  @Test
  public void tablePermissions() throws TException {
    tpc.proxy().securityOperations_grantTablePermission(userpass, testuser, testtable, PTablePermission.ALTER_TABLE);
    assertTrue(tpc.proxy().securityOperations_hasTablePermission(userpass, testuser, testtable, PTablePermission.ALTER_TABLE));
    
    tpc.proxy().securityOperations_revokeTablePermission(userpass, testuser, testtable, PTablePermission.ALTER_TABLE);
    assertFalse(tpc.proxy().securityOperations_hasTablePermission(userpass, testuser, testtable, PTablePermission.ALTER_TABLE));
    
  }
  
  @Test
  public void systemPermissions() throws TException {
    tpc.proxy().securityOperations_grantSystemPermission(userpass, testuser, PSystemPermission.ALTER_USER);
    assertTrue(tpc.proxy().securityOperations_hasSystemPermission(userpass, testuser, PSystemPermission.ALTER_USER));
    
    tpc.proxy().securityOperations_revokeSystemPermission(userpass, testuser, PSystemPermission.ALTER_USER);
    assertFalse(tpc.proxy().securityOperations_hasSystemPermission(userpass, testuser, PSystemPermission.ALTER_USER));
    
  }
  
  @Test
  public void auths() throws TException {
    HashSet<ByteBuffer> newauths = new HashSet<ByteBuffer>();
    newauths.add(ByteBuffer.wrap("BBR".getBytes()));
    newauths.add(ByteBuffer.wrap("Barney".getBytes()));
    tpc.proxy().securityOperations_changeUserAuthorizations(userpass, testuser, newauths);
    List<ByteBuffer> actualauths = tpc.proxy().securityOperations_getUserAuthorizations(userpass, testuser);
    assertEquals(actualauths.size(), newauths.size());
    
    for (ByteBuffer auth : actualauths) {
      assertTrue(newauths.contains(auth));
    }
  }
  
}

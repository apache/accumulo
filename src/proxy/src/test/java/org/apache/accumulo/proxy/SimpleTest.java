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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.proxy.thrift.AccumuloProxy.Client;
import org.apache.accumulo.proxy.thrift.*;
import org.apache.accumulo.server.test.functional.SlowIterator;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Call every method on the proxy and try to verify that it works.
 */
public class SimpleTest {
  
  public static final String TABLE_TEST = "test";

  private static String secret = "";
  private static Random random = new Random();
  private static TServer proxyServer;
  private static Thread thread;
  private static int proxyPort;
  private static org.apache.accumulo.proxy.thrift.AccumuloProxy.Client client;
  private static String principal = "root";
  @SuppressWarnings("serial")
  private static Map<String, String> properties = new TreeMap<String, String>() {{ put("password",secret);}}; 
  private static ByteBuffer creds = null;

  private static Class<? extends TProtocolFactory> protocolClass;

  static Class<? extends TProtocolFactory> getRandomProtocol() {
    List<Class<? extends TProtocolFactory>> protocolFactories = new ArrayList<Class<? extends TProtocolFactory>>();
    protocolFactories.add(org.apache.thrift.protocol.TCompactProtocol.Factory.class);
    
    Random rand = new Random();
    return protocolFactories.get(rand.nextInt(protocolFactories.size()));
  }

  @BeforeClass
  public static void setupMiniCluster() throws Exception {

    Properties props = new Properties();
    props.put("org.apache.accumulo.proxy.ProxyServer.useMockInstance", "true");

    protocolClass = getRandomProtocol();
    System.out.println(protocolClass.getName());

    proxyPort = 40000 + random.nextInt(20000);
    proxyServer = Proxy.createProxyServer(org.apache.accumulo.proxy.thrift.AccumuloProxy.class, org.apache.accumulo.proxy.ProxyServer.class, proxyPort,
        protocolClass, props);
    thread = new Thread() {
      @Override
      public void run() {
        proxyServer.serve();
      }
    };
    thread.start();
    while (!proxyServer.isServing())
      UtilWaitThread.sleep(100);
    client = new TestProxyClient("localhost", proxyPort, protocolClass.newInstance()).proxy();
    creds = client.login(principal, properties);
  }

  @Test
  public void testInstanceOperations() throws Exception {
    // get something we know is in the site config
    Map<String,String> cfg = client.getSiteConfiguration(creds);

    // set a property in zookeeper
    client.setProperty(creds, "table.split.threshold", "500M");
    
    // check that we can read it
    for (int i = 0; i < 5; i++) {
      cfg = client.getSystemConfiguration(creds);
      if ("500M".equals(cfg.get("table.split.threshold")))
          break;
      Thread.sleep(200);
    }
    assertEquals("500M", cfg.get("table.split.threshold"));

    // unset the setting, check that it's not what it was
    client.removeProperty(creds, "table.split.threshold");
    for (int i = 0; i < 5; i++) {
      cfg = client.getSystemConfiguration(creds);
      if (!"500M".equals(cfg.get("table.split.threshold")))
          break;
      Thread.sleep(200);
    }
    assertTrue("500M" != cfg.get("table.split.threshold"));

    // try to load some classes via the proxy
    assertTrue(client.testClassLoad(creds, DevNull.class.getName(), SortedKeyValueIterator.class.getName()));
    assertFalse(client.testClassLoad(creds, "foo.bar", SortedKeyValueIterator.class.getName()));

    // create a table that's very slow, so we can look for scans/compactions
    client.createTable(creds, "slow", true, TimeType.MILLIS);
    IteratorSetting setting = new IteratorSetting(100, "slow", SlowIterator.class.getName(), Collections.singletonMap("sleepTime", "200"));
    client.attachIterator(creds, "slow", setting, EnumSet.allOf(IteratorScope.class));
    client.updateAndFlush(creds, "slow", mutation("row", "cf", "cq", "value"));
    client.updateAndFlush(creds, "slow", mutation("row2", "cf", "cq", "value"));
    client.updateAndFlush(creds, "slow", mutation("row3", "cf", "cq", "value"));
    client.updateAndFlush(creds, "slow", mutation("row4", "cf", "cq", "value"));
    
    // scan
    Thread t = new Thread() {
      @Override
      public void run() {
        String scanner;
        try {
          Client client2 = new TestProxyClient("localhost", proxyPort, protocolClass.newInstance()).proxy();
          scanner = client2.createScanner(creds, "slow", null);
          client2.nextK(scanner, 10);
          client2.closeScanner(scanner);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    t.start();
  }
  
  @Test
  public void testSecurityOperations() throws Exception {
    // check password
    assertTrue(client.authenticateUser(creds, "root", s2pp(secret)));
    assertFalse(client.authenticateUser(creds, "root", s2pp("fail")));

    // create a user
    client.createLocalUser(creds, "stooge", s2bb("password"));
    // change auths
    Set<String> users = client.listLocalUsers(creds);
    assertEquals(new HashSet<String>(Arrays.asList("root", "stooge")), users);
    HashSet<ByteBuffer> auths = new HashSet<ByteBuffer>(Arrays.asList(s2bb("A"),s2bb("B")));
    client.changeUserAuthorizations(creds, "stooge", auths);
    List<ByteBuffer> update = client.getUserAuthorizations(creds, "stooge");
    assertEquals(auths, new HashSet<ByteBuffer>(update));
    
    // change password
    client.changeLocalUserPassword(creds, "stooge", s2bb(""));
    assertTrue(client.authenticateUser(creds, "stooge", s2pp("")));
    
    // check permission failure
    @SuppressWarnings("serial")
    ByteBuffer stooge = client.login("stooge", new TreeMap<String,String>() {{put("password",""); }});
    
    // grant permissions and test
    assertFalse(client.hasSystemPermission(creds, "stooge", SystemPermission.CREATE_TABLE));
    client.grantSystemPermission(creds, "stooge", SystemPermission.CREATE_TABLE);
    assertTrue(client.hasSystemPermission(creds, "stooge", SystemPermission.CREATE_TABLE));
    client.createTable(stooge, "success", true, TimeType.MILLIS);
    client.listTables(creds).contains("succcess");
    
    // revoke permissions
    client.revokeSystemPermission(creds, "stooge", SystemPermission.CREATE_TABLE);
    assertFalse(client.hasSystemPermission(creds, "stooge", SystemPermission.CREATE_TABLE));

    // create a table to test table permissions
    client.createTable(creds, TABLE_TEST, true, TimeType.MILLIS);

    // grant
    assertFalse(client.hasTablePermission(creds, "stooge", TABLE_TEST, TablePermission.READ));
    client.grantTablePermission(creds, "stooge", TABLE_TEST, TablePermission.READ);
    assertTrue(client.hasTablePermission(creds, "stooge", TABLE_TEST, TablePermission.READ));
    String scanner = client.createScanner(stooge, TABLE_TEST, null);
    client.nextK(scanner, 10);
    client.closeScanner(scanner);

    // delete user
    client.dropLocalUser(creds, "stooge");
    users = client.listLocalUsers(creds);
    assertEquals(1, users.size());
    
  }

  @Test
  public void testBatchWriter() throws Exception {
      if (client.tableExists(creds, TABLE_TEST))
          client.deleteTable(creds, TABLE_TEST);

      client.createTable(creds, TABLE_TEST, true, TimeType.MILLIS);

      client.removeConstraint(creds, TABLE_TEST, 1);

      WriterOptions writerOptions = new WriterOptions();
      writerOptions.setLatencyMs(10000);
      writerOptions.setMaxMemory(3000);
      writerOptions.setThreads(1);
      writerOptions.setTimeoutMs(100000);

      String batchWriter = client.createWriter(creds, TABLE_TEST, writerOptions);

      client.update(batchWriter, mutation("row1", "cf", "cq", "x"));
      client.flush(batchWriter);
      client.closeWriter(batchWriter);

      String scanner = client.createScanner(creds, TABLE_TEST, null);
      ScanResult more = client.nextK(scanner, 2);
      assertEquals(1, more.getResults().size());
      client.closeScanner(scanner);

      client.deleteTable(creds, TABLE_TEST);
  }
  
  @Test
  public void testTableOperations() throws Exception {
    if (client.tableExists(creds, TABLE_TEST))
      client.deleteTable(creds, TABLE_TEST);
    client.createTable(creds, TABLE_TEST, true, TimeType.MILLIS);

    client.updateAndFlush(creds, TABLE_TEST, mutation("row1", "cf", "cq", "x"));
    String scanner = client.createScanner(creds, TABLE_TEST, null);
    ScanResult more = client.nextK(scanner, 2);
    client.closeScanner(scanner);
    assertFalse(more.isMore());
    assertEquals(1, more.getResults().size());
    assertEquals(s2bb("x"), more.getResults().get(0).value);
    // iterators
    client.deleteTable(creds, TABLE_TEST);
    client.createTable(creds, TABLE_TEST, true, TimeType.MILLIS);
    HashMap<String, String> options = new HashMap<String, String>();
    options.put("type", "STRING");
    options.put("columns", "cf");
    IteratorSetting setting = new IteratorSetting(10, TABLE_TEST, SummingCombiner.class.getName(), options);
    client.attachIterator(creds, TABLE_TEST, setting, EnumSet.allOf(IteratorScope.class));
    for (int i = 0; i < 10; i++) {
      client.updateAndFlush(creds, TABLE_TEST, mutation("row1", "cf", "cq", "1"));
    }
    scanner = client.createScanner(creds, TABLE_TEST, null);
    more = client.nextK(scanner, 2);
    client.closeScanner(scanner);
    assertEquals("10", new String(more.getResults().get(0).getValue()));
    try {
      client.checkIteratorConflicts(creds, TABLE_TEST, setting, EnumSet.allOf(IteratorScope.class));
      fail("checkIteratorConflicts did not throw and exception");
    } catch (Exception ex) {
    }
    client.removeIterator(creds, TABLE_TEST, "test", EnumSet.allOf(IteratorScope.class));
    for (int i = 0; i < 10; i++) {
      client.updateAndFlush(creds, TABLE_TEST, mutation("row"+i, "cf", "cq", ""+i));
    }
    scanner = client.createScanner(creds, TABLE_TEST, null);
    more = client.nextK(scanner, 100);
    client.closeScanner(scanner);
    assertEquals(10, more.getResults().size());

  }
  
  // scan !METADATA table for file entries for the given table
  private int countFiles(String table) throws Exception {
    Map<String,String> tableIdMap = client.tableIdMap(creds);
    String tableId = tableIdMap.get(table);
    Key start = new Key();
    start.row = s2bb(tableId + ";");
    Key end = new Key();
    end.row = s2bb(tableId + "<");
    end = client.getFollowing(end, PartialKey.ROW);
    ScanOptions opt = new ScanOptions();
    opt.range = new Range(start, true, end, false);
    opt.columns = Collections.singletonList(new ScanColumn(s2bb("file")));
    String scanner = client.createScanner(creds, Constants.METADATA_TABLE_NAME, opt);
    int result = 0;
    while (true) {
      ScanResult more = client.nextK(scanner, 100);
      result += more.getResults().size();
      if (!more.more)
        break;
    }
    return result;
  }

  private Map<ByteBuffer,List<ColumnUpdate>> mutation(String row, String cf, String cq, String value) {
    ColumnUpdate upd = new ColumnUpdate(s2bb(cf), s2bb(cq));
    upd.setValue(value.getBytes());
    return Collections.singletonMap(s2bb(row), Collections.singletonList(upd));
  }

  private ByteBuffer s2bb(String cf) {
    return ByteBuffer.wrap(cf.getBytes());
  }

  private Map<String, String> s2pp(String cf) {
    Map<String, String> toRet = new TreeMap<String, String>();
    toRet.put("password", cf);
    return toRet;
  }
}

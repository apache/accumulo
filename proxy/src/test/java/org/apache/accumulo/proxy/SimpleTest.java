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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.examples.simple.constraints.NumericValueConstraint;
import org.apache.accumulo.proxy.thrift.AccumuloProxy.Client;
import org.apache.accumulo.proxy.thrift.ActiveCompaction;
import org.apache.accumulo.proxy.thrift.ActiveScan;
import org.apache.accumulo.proxy.thrift.ColumnUpdate;
import org.apache.accumulo.proxy.thrift.CompactionReason;
import org.apache.accumulo.proxy.thrift.CompactionType;
import org.apache.accumulo.proxy.thrift.IteratorScope;
import org.apache.accumulo.proxy.thrift.IteratorSetting;
import org.apache.accumulo.proxy.thrift.Key;
import org.apache.accumulo.proxy.thrift.PartialKey;
import org.apache.accumulo.proxy.thrift.Range;
import org.apache.accumulo.proxy.thrift.ScanColumn;
import org.apache.accumulo.proxy.thrift.ScanOptions;
import org.apache.accumulo.proxy.thrift.ScanResult;
import org.apache.accumulo.proxy.thrift.ScanState;
import org.apache.accumulo.proxy.thrift.ScanType;
import org.apache.accumulo.proxy.thrift.SystemPermission;
import org.apache.accumulo.proxy.thrift.TablePermission;
import org.apache.accumulo.proxy.thrift.TimeType;
import org.apache.accumulo.test.MiniAccumuloCluster;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Call every method on the proxy and try to verify that it works.
 */
public class SimpleTest {
  
  public static TemporaryFolder folder = new TemporaryFolder();
  
  private static MiniAccumuloCluster accumulo;
  private static String secret = "superSecret";
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
    protocolFactories.add(org.apache.thrift.protocol.TJSONProtocol.Factory.class);
    protocolFactories.add(org.apache.thrift.protocol.TBinaryProtocol.Factory.class);
    protocolFactories.add(org.apache.thrift.protocol.TTupleProtocol.Factory.class);
    protocolFactories.add(org.apache.thrift.protocol.TCompactProtocol.Factory.class);
    
    Random rand = new Random();
    return protocolFactories.get(rand.nextInt(protocolFactories.size()));
  }

  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    folder.create();
    accumulo = new MiniAccumuloCluster(folder.getRoot(), secret);
    accumulo.start();
  
    Properties props = new Properties();
    props.put("org.apache.accumulo.proxy.ProxyServer.instancename", accumulo.getInstanceName());
    props.put("org.apache.accumulo.proxy.ProxyServer.zookeepers", accumulo.getZooKeepers());
    
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

  @Test(timeout = 10000)
  public void testInstanceOperations() throws Exception {
    int tservers = 0;
    for (String tserver : client.getTabletServers(creds)) {
      client.pingTabletServer(creds, tserver);
      tservers++;
    }
    assertTrue(tservers > 0);
    
    // get something we know is in the site config
    Map<String,String> cfg = client.getSiteConfiguration(creds);
    assertTrue(cfg.get("instance.dfs.dir").startsWith(folder.getRoot().toString()));
    
    // set a property in zookeeper
    client.setProperty(creds, "table.split.threshold", "500M");
    
    // check that we can read it
    cfg = client.getSystemConfiguration(creds);
    assertEquals("500M", cfg.get("table.split.threshold"));
    
    // unset the setting, check that it's not what it was
    client.removeProperty(creds, "table.split.threshold");
    cfg = client.getSystemConfiguration(creds);
    assertNotEquals("500M", cfg.get("table.split.threshold"));
    
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
    // look for the scan
    List<ActiveScan> scans = Collections.emptyList();
    loop:
    for (int i = 0; i < 100; i++) {
      for (String tserver: client.getTabletServers(creds)) {
       scans = client.getActiveScans(creds, tserver);
       if (!scans.isEmpty())
         break loop;
       UtilWaitThread.sleep(10);
      }
    }
    t.join();
    assertFalse(scans.isEmpty());
    ActiveScan scan = scans.get(0);
    assertEquals("root", scan.getUser());
    assertEquals(ScanState.RUNNING, scan.getState());
    assertEquals(ScanType.SINGLE, scan.getType());
    assertEquals("slow", scan.getTable());
    Map<String,String> map = client.tableIdMap(creds);
    assertEquals(map.get("slow"), scan.getExtent().tableId);
    assertTrue(scan.getExtent().endRow == null);
    assertTrue(scan.getExtent().prevEndRow == null);
    
    // start a compaction
    t = new Thread() {
      @Override
      public void run() {
        try {
          Client client2 = new TestProxyClient("localhost", proxyPort, protocolClass.newInstance()).proxy();
          client2.compactTable(creds, "slow", null, null, null, true, true);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    t.start();
    
    // try to catch it in the act
    List<ActiveCompaction> compactions = Collections.emptyList();
    loop2:
    for (int i = 0; i < 100; i++) {
      for (String tserver: client.getTabletServers(creds)) {
        compactions = client.getActiveCompactions(creds, tserver);
        if (!compactions.isEmpty())
          break loop2;
      }
      UtilWaitThread.sleep(10);
    }
    t.join();
    // verify the compaction information
    assertFalse(compactions.isEmpty());
    ActiveCompaction c = compactions.get(0);
    assertEquals(map.get("slow"), c.getExtent().tableId);
    assertTrue(c.inputFiles.isEmpty());
    assertEquals(CompactionType.MINOR, c.getType());
    assertEquals(CompactionReason.USER, c.getReason());
    assertEquals("", c.localityGroup);
    assertTrue(c.outputFile.contains("default_tablet"));
  }
  
  @Test
  public void testSecurityOperations() throws Exception {
    // check password
    assertTrue(client.authenticateUser(creds, "root", s2pp(secret)));
    assertFalse(client.authenticateUser(creds, "root", s2pp("")));

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
    
    try {
      client.createTable(stooge, "fail", true, TimeType.MILLIS);
      fail("should not create the table");
    } catch (TException ex) {
      assertFalse(client.listTables(creds).contains("fail"));
    }
    // grant permissions and test
    assertFalse(client.hasSystemPermission(creds, "stooge", SystemPermission.CREATE_TABLE));
    client.grantSystemPermission(creds, "stooge", SystemPermission.CREATE_TABLE);
    assertTrue(client.hasSystemPermission(creds, "stooge", SystemPermission.CREATE_TABLE));
    client.createTable(stooge, "success", true, TimeType.MILLIS);
    client.listTables(creds).contains("succcess");
    
    // revoke permissions
    client.revokeSystemPermission(creds, "stooge", SystemPermission.CREATE_TABLE);
    assertFalse(client.hasSystemPermission(creds, "stooge", SystemPermission.CREATE_TABLE));
    try {
      client.createTable(stooge, "fail", true, TimeType.MILLIS);
      fail("should not create the table");
    } catch (TException ex) {
      assertFalse(client.listTables(creds).contains("fail"));
    }
    // create a table to test table permissions
    client.createTable(creds, "test", true, TimeType.MILLIS);
    // denied!
    try {
      String scanner = client.createScanner(stooge, "test", null);
      client.nextK(scanner, 100);
      fail("stooge should not read table test");
    } catch (TException ex) {
    }
    // grant
    assertFalse(client.hasTablePermission(creds, "stooge", "test", TablePermission.READ));
    client.grantTablePermission(creds, "stooge", "test", TablePermission.READ);
    assertTrue(client.hasTablePermission(creds, "stooge", "test", TablePermission.READ));
    String scanner = client.createScanner(stooge, "test", null);
    client.nextK(scanner, 10);
    client.closeScanner(scanner);
    // revoke
    client.revokeTablePermission(creds, "stooge", "test", TablePermission.READ);
    assertFalse(client.hasTablePermission(creds, "stooge", "test", TablePermission.READ));
    try {
      scanner = client.createScanner(stooge, "test", null);
      client.nextK(scanner, 100);
      fail("stooge should not read table test");
    } catch (TException ex) {
    }
    
    // delete user
    client.dropLocalUser(creds, "stooge");
    users = client.listLocalUsers(creds);
    assertEquals(1, users.size());
    
  }
  
  @Test
  public void testTableOperations() throws Exception {
    if (client.tableExists(creds, "test"))
      client.deleteTable(creds, "test");
    client.createTable(creds, "test", true, TimeType.MILLIS);
    // constraints
    client.addConstraint(creds, "test", NumericValueConstraint.class.getName());
    client.updateAndFlush(creds, "test", mutation("row1", "cf", "cq", "123"));
    try {
      client.updateAndFlush(creds, "test", mutation("row1", "cf", "cq", "x"));
      fail("constraint did not fire");
    } catch (Exception ex) {
    }
    client.removeConstraint(creds, "test", 1);
    client.updateAndFlush(creds, "test", mutation("row1", "cf", "cq", "x"));
    String scanner = client.createScanner(creds, "test", null);
    ScanResult more = client.nextK(scanner, 2);
    client.closeScanner(scanner);
    assertFalse(more.isMore());
    assertEquals(1, more.getResults().size());
    assertEquals(s2bb("x"), more.getResults().get(0).value);
    // splits, merge
    client.addSplits(creds, "test", new HashSet<ByteBuffer>(Arrays.asList(s2bb("a"), s2bb("m"), s2bb("z"))));
    List<ByteBuffer> splits = client.listSplits(creds, "test", 1);
    assertEquals(Arrays.asList(s2bb("m")), splits);
    client.mergeTablets(creds, "test", null, s2bb("m"));
    splits = client.listSplits(creds, "test", 10);
    assertEquals(Arrays.asList(s2bb("m"), s2bb("z")), splits);
    client.mergeTablets(creds, "test", null, null);
    splits = client.listSplits(creds, "test", 10);
    List<ByteBuffer> empty = Collections.emptyList();
    assertEquals(empty, splits);
    // iterators
    client.deleteTable(creds, "test");
    client.createTable(creds, "test", true, TimeType.MILLIS);
    HashMap<String, String> options = new HashMap<String, String>();
    options.put("type", "STRING");
    options.put("columns", "cf");
    IteratorSetting setting = new IteratorSetting(10, "test", SummingCombiner.class.getName(), options);
    client.attachIterator(creds, "test", setting, EnumSet.allOf(IteratorScope.class));
    for (int i = 0; i < 10; i++) {
      client.updateAndFlush(creds, "test", mutation("row1", "cf", "cq", "1"));
    }
    scanner = client.createScanner(creds, "test", null);
    more = client.nextK(scanner, 2);
    client.closeScanner(scanner);
    assertEquals("10", new String(more.getResults().get(0).getValue()));
    try {
      client.checkIteratorConflicts(creds, "test", setting, EnumSet.allOf(IteratorScope.class));
      fail("checkIteratorConflicts did not throw and exception");
    } catch (Exception ex) {
    }
    client.deleteRows(creds, "test", null, null);
    client.removeIterator(creds, "test", "test", EnumSet.allOf(IteratorScope.class));
    for (int i = 0; i < 10; i++) {
      client.updateAndFlush(creds, "test", mutation("row"+i, "cf", "cq", ""+i));
      client.flushTable(creds, "test", null, null, true);
    }
    scanner = client.createScanner(creds, "test", null);
    more = client.nextK(scanner, 100);
    client.closeScanner(scanner);
    assertEquals(10, more.getResults().size());
    // clone
    client.cloneTable(creds, "test", "test2", true, null, null);
    scanner = client.createScanner(creds, "test2", null);
    more = client.nextK(scanner, 100);
    client.closeScanner(scanner);
    assertEquals(10, more.getResults().size());
    client.deleteTable(creds, "test2");
    
    // don't know how to test this, call it just for fun
    client.clearLocatorCache(creds, "test");
    
    // compact
    assertTrue(countFiles("test") > 1);
    client.compactTable(creds, "test", null, null, null, true, true);
    assertEquals(1, countFiles("test"));
    
    // export/import
    String dir = folder.getRoot() + "/test";
    String destDir = folder.getRoot() + "/test_dest";
    client.offlineTable(creds, "test");
    client.exportTable(creds, "test", dir);
    // copy files to a new location
    FileSystem fs = FileSystem.get(new Configuration());
    FSDataInputStream is = fs.open(new Path(dir + "/distcp.txt"));
    BufferedReader r = new BufferedReader(new InputStreamReader(is));
    while (true) {
      String line = r.readLine();
      if (line == null)
        break;
      Path srcPath = new Path(line);
      FileUtils.copyFile(new File(srcPath.toUri().getPath()), new File(destDir, srcPath.getName()));
    }
    client.deleteTable(creds, "test");
    client.importTable(creds, "testify", destDir);
    scanner = client.createScanner(creds, "testify", null);
    more = client.nextK(scanner, 100);
    client.closeScanner(scanner);
    assertEquals(10, more.results.size());
    
    // Locality groups
    client.createTable(creds, "test", true, TimeType.MILLIS);
    Map<String, Set<String>> groups = new HashMap<String, Set<String>>();
    groups.put("group1", Collections.singleton("cf1"));
    groups.put("group2", Collections.singleton("cf2"));
    client.setLocalityGroups(creds, "test", groups);
    assertEquals(groups, client.getLocalityGroups(creds, "test"));
    // table properties
    Map<String,String> orig = client.getTableProperties(creds, "test");
    client.setTableProperty(creds, "test", "table.split.threshold", "500M");
    Map<String,String> update = client.getTableProperties(creds, "test");
    assertEquals(update.get("table.split.threshold"), "500M");
    client.removeTableProperty(creds, "test", "table.split.threshold");
    update = client.getTableProperties(creds, "test");
    assertEquals(orig, update);
    // rename table
    Map<String,String> tables = client.tableIdMap(creds);
    client.renameTable(creds, "test", "bar");
    Map<String,String> tables2 = client.tableIdMap(creds);
    assertEquals(tables.get("test"), tables2.get("bar"));
    // table exists
    assertTrue(client.tableExists(creds, "bar"));
    assertFalse(client.tableExists(creds, "test"));
    // bulk import
    String filename = dir + "/bulk/import/rfile.rf";
    FileSKVWriter writer = FileOperations.getInstance().openWriter(filename, fs, fs.getConf(), DefaultConfiguration.getInstance());
    writer.startDefaultLocalityGroup();
    writer.append(new org.apache.accumulo.core.data.Key(new Text("a"), new Text("b"), new Text("c")), new Value("value".getBytes()));
    writer.close();
    fs.mkdirs(new Path(dir + "/bulk/fail"));
    client.importDirectory(creds, "bar", dir + "/bulk/import", dir + "/bulk/fail", true);
    scanner = client.createScanner(creds, "bar", null);
    more = client.nextK(scanner, 100);
    client.closeScanner(scanner);
    assertEquals(1, more.results.size());
    ByteBuffer maxRow = client.getMaxRow(creds, "bar", null, null, false, null, false);
    assertEquals(s2bb("a"), maxRow);
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

  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
    // folder.delete();
  }
  
}

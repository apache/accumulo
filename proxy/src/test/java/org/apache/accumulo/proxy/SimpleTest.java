package org.apache.accumulo.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.proxy.thrift.AccumuloProxy.Client;
import org.apache.accumulo.proxy.thrift.ActiveCompaction;
import org.apache.accumulo.proxy.thrift.ActiveScan;
import org.apache.accumulo.proxy.thrift.ColumnUpdate;
import org.apache.accumulo.proxy.thrift.CompactionReason;
import org.apache.accumulo.proxy.thrift.CompactionType;
import org.apache.accumulo.proxy.thrift.IteratorScope;
import org.apache.accumulo.proxy.thrift.IteratorSetting;
import org.apache.accumulo.proxy.thrift.ScanState;
import org.apache.accumulo.proxy.thrift.ScanType;
import org.apache.accumulo.proxy.thrift.SystemPermission;
import org.apache.accumulo.proxy.thrift.TablePermission;
import org.apache.accumulo.proxy.thrift.TimeType;
import org.apache.accumulo.proxy.thrift.UserPass;
import org.apache.accumulo.server.test.functional.SlowIterator;
import org.apache.accumulo.test.MiniAccumuloCluster;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * An example unit test that shows how to use MiniAccumuloCluster in a unit test
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
  private static UserPass creds = new UserPass("root", ByteBuffer.wrap(secret.getBytes()));

  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    folder.create();
    accumulo = new MiniAccumuloCluster(folder.getRoot(), secret);
    accumulo.start();
  
    Properties props = new Properties();
    props.put("org.apache.accumulo.proxy.ProxyServer.instancename", accumulo.getInstanceName());
    props.put("org.apache.accumulo.proxy.ProxyServer.zookeepers", accumulo.getZookeepers());
    
    proxyPort = 40000 + random.nextInt(20000);
    proxyServer = Proxy.createProxyServer(
        org.apache.accumulo.proxy.thrift.AccumuloProxy.class,
        org.apache.accumulo.proxy.ProxyServer.class, 
        proxyPort, 
        props);
    thread = new Thread() {
      @Override
      public void run() {
        proxyServer.serve();
      }
    };
    thread.start();
    while (!proxyServer.isServing())
      UtilWaitThread.sleep(100);
    client = new TestProxyClient("localhost", proxyPort).proxy();
  }

  //@Test(timeout = 10000)
  public void testPing() throws Exception {
    client.ping(creds);
  }
  
  //@Test(timeout = 10000)
  public void testInstanceOperations() throws Exception {
    int tservers = 0;
    for (String tserver : client.getTabletServers(creds)) {
      client.pingTabletServer(creds, tserver);
      tservers++;
    }
    assertTrue(tservers > 0);
    
    // get something we know is in the site config
    Map<String,String> cfg = client.getSiteConfiguration(creds);
    assertTrue(cfg.get("instance.dfs.dir").startsWith("/tmp/junit"));
    
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
    IteratorSetting setting = new IteratorSetting(100, "slow", SlowIterator.class.getName(), Collections.singletonMap("sleepTime", "100"));
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
          Client client2 = new TestProxyClient("localhost", proxyPort).proxy();
          scanner = client2.createScanner(creds, "slow", null, null, null);
          client2.scanner_next_k(scanner, 10);
          client2.close_scanner(scanner);
        } catch (TException e) {
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
          Client client2 = new TestProxyClient("localhost", proxyPort).proxy();
          client2.compactTable(creds, "slow", null, null, null, true, true);
        } catch (TException e) {
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
    assertTrue(client.authenticateUser(creds, "root", s2bb(secret)));
    assertFalse(client.authenticateUser(creds, "root", s2bb("")));

    // create a user
    client.createUser(creds, "stooge", s2bb("password"));
    // change auths
    Set<String> users = client.listUsers(creds);
    assertEquals(new HashSet<String>(Arrays.asList("root", "stooge")), users);
    HashSet<ByteBuffer> auths = new HashSet<ByteBuffer>(Arrays.asList(s2bb("A"),s2bb("B")));
    client.changeUserAuthorizations(creds, "stooge", auths);
    List<ByteBuffer> update = client.getUserAuthorizations(creds, "stooge");
    assertEquals(auths, new HashSet<ByteBuffer>(update));
    
    // change password
    client.changeUserPassword(creds, "stooge", s2bb(""));
    assertTrue(client.authenticateUser(creds, "stooge", s2bb("")));
    
    // check permission failure
    UserPass stooge = new UserPass("stooge", s2bb(""));
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
      String scanner = client.createScanner(stooge, "test", null, null, null);
      client.scanner_next_k(scanner, 100);
      fail("stooge should not read table test");
    } catch (TException ex) {
    }
    // grant
    assertFalse(client.hasTablePermission(creds, "stooge", "test", TablePermission.READ));
    client.grantTablePermission(creds, "stooge", "test", TablePermission.READ);
    assertTrue(client.hasTablePermission(creds, "stooge", "test", TablePermission.READ));
    String scanner = client.createScanner(stooge, "test", null, null, null);
    client.scanner_next_k(scanner, 10);
    client.close_scanner(scanner);
    // revoke
    client.revokeTablePermission(creds, "stooge", "test", TablePermission.READ);
    assertFalse(client.hasTablePermission(creds, "stooge", "test", TablePermission.READ));
    try {
      scanner = client.createScanner(stooge, "test", null, null, null);
      client.scanner_next_k(scanner, 100);
      fail("stooge should not read table test");
    } catch (TException ex) {
    }
    
    // delete user
    client.dropUser(creds, "stooge");
    users = client.listUsers(creds);
    assertEquals(1, users.size());
    
  }
  
  private Map<ByteBuffer,List<ColumnUpdate>> mutation(String row, String cf, String cq, String value) {
    ColumnUpdate upd = new ColumnUpdate(s2bb(cf), s2bb(cq));
    upd.setValue(value.getBytes());
    return Collections.singletonMap(s2bb(row), Collections.singletonList(upd));
  }

  private ByteBuffer s2bb(String cf) {
    return ByteBuffer.wrap(cf.getBytes());
  }

  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
    folder.delete();
  }
  
}

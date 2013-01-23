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
import org.apache.accumulo.proxy.thrift.PActiveCompaction;
import org.apache.accumulo.proxy.thrift.PActiveScan;
import org.apache.accumulo.proxy.thrift.PColumnUpdate;
import org.apache.accumulo.proxy.thrift.PCompactionReason;
import org.apache.accumulo.proxy.thrift.PCompactionType;
import org.apache.accumulo.proxy.thrift.PIteratorScope;
import org.apache.accumulo.proxy.thrift.PIteratorSetting;
import org.apache.accumulo.proxy.thrift.PScanState;
import org.apache.accumulo.proxy.thrift.PScanType;
import org.apache.accumulo.proxy.thrift.PSystemPermission;
import org.apache.accumulo.proxy.thrift.PTablePermission;
import org.apache.accumulo.proxy.thrift.PTimeType;
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
    for (String tserver : client.instanceOperations_getTabletServers(creds)) {
      client.instanceOperations_pingTabletServer(creds, tserver);
      tservers++;
    }
    assertTrue(tservers > 0);
    
    // get something we know is in the site config
    Map<String,String> cfg = client.instanceOperations_getSiteConfiguration(creds);
    assertTrue(cfg.get("instance.dfs.dir").startsWith("/tmp/junit"));
    
    // set a property in zookeeper
    client.instanceOperations_setProperty(creds, "table.split.threshold", "500M");
    
    // check that we can read it
    cfg = client.instanceOperations_getSystemConfiguration(creds);
    assertEquals("500M", cfg.get("table.split.threshold"));
    
    // unset the setting, check that it's not what it was
    client.instanceOperations_removeProperty(creds, "table.split.threshold");
    cfg = client.instanceOperations_getSystemConfiguration(creds);
    assertNotEquals("500M", cfg.get("table.split.threshold"));
    
    // try to load some classes via the proxy
    assertTrue(client.instanceOperations_testClassLoad(creds, DevNull.class.getName(), SortedKeyValueIterator.class.getName()));
    assertFalse(client.instanceOperations_testClassLoad(creds, "foo.bar", SortedKeyValueIterator.class.getName()));

    // create a table that's very slow, so we can look for scans/compactions
    client.tableOperations_create(creds, "slow", true, PTimeType.MILLIS);
    PIteratorSetting setting = new PIteratorSetting(100, "slow", SlowIterator.class.getName(), Collections.singletonMap("sleepTime", "100"));
    client.tableOperations_attachIterator(creds, "slow", setting, EnumSet.allOf(PIteratorScope.class));
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
    List<PActiveScan> scans = Collections.emptyList();
    loop:
    for (int i = 0; i < 100; i++) {
      for (String tserver: client.instanceOperations_getTabletServers(creds)) {
       scans = client.instanceOperations_getActiveScans(creds, tserver);
       if (!scans.isEmpty())
         break loop;
       UtilWaitThread.sleep(10);
      }
    }
    t.join();
    assertFalse(scans.isEmpty());
    PActiveScan scan = scans.get(0);
    assertEquals("root", scan.getUser());
    assertEquals(PScanState.RUNNING, scan.getState());
    assertEquals(PScanType.SINGLE, scan.getType());
    assertEquals("slow", scan.getTable());
    Map<String,String> map = client.tableOperations_tableIdMap(creds);
    assertEquals(map.get("slow"), scan.getExtent().tableId);
    assertTrue(scan.getExtent().endRow == null);
    assertTrue(scan.getExtent().prevEndRow == null);
    
    // start a compaction
    t = new Thread() {
      @Override
      public void run() {
        try {
          Client client2 = new TestProxyClient("localhost", proxyPort).proxy();
          client2.tableOperations_compact(creds, "slow", null, null, null, true, true);
        } catch (TException e) {
          throw new RuntimeException(e);
        }
      }
    };
    t.start();
    
    // try to catch it in the act
    List<PActiveCompaction> compactions = Collections.emptyList();
    loop2:
    for (int i = 0; i < 100; i++) {
      for (String tserver: client.instanceOperations_getTabletServers(creds)) {
        compactions = client.instanceOperations_getActiveCompactions(creds, tserver);
        if (!compactions.isEmpty())
          break loop2;
      }
      UtilWaitThread.sleep(10);
    }
    t.join();
    // verify the compaction information
    assertFalse(compactions.isEmpty());
    PActiveCompaction c = compactions.get(0);
    assertEquals(map.get("slow"), c.getExtent().tableId);
    assertTrue(c.inputFiles.isEmpty());
    assertEquals(PCompactionType.MINOR, c.getType());
    assertEquals(PCompactionReason.USER, c.getReason());
    assertEquals("", c.localityGroup);
    assertTrue(c.outputFile.contains("default_tablet"));
  }
  
  @Test
  public void testSecurityOperations() throws Exception {
    // check password
    assertTrue(client.securityOperations_authenticateUser(creds, "root", s2bb(secret)));
    assertFalse(client.securityOperations_authenticateUser(creds, "root", s2bb("")));

    // create a user
    client.securityOperations_createUser(creds, "stooge", s2bb("password"));
    // change auths
    Set<String> users = client.securityOperations_listUsers(creds);
    assertEquals(new HashSet<String>(Arrays.asList("root", "stooge")), users);
    HashSet<ByteBuffer> auths = new HashSet<ByteBuffer>(Arrays.asList(s2bb("A"),s2bb("B")));
    client.securityOperations_changeUserAuthorizations(creds, "stooge", auths);
    List<ByteBuffer> update = client.securityOperations_getUserAuthorizations(creds, "stooge");
    assertEquals(auths, new HashSet<ByteBuffer>(update));
    
    // change password
    client.securityOperations_changeUserPassword(creds, "stooge", s2bb(""));
    assertTrue(client.securityOperations_authenticateUser(creds, "stooge", s2bb("")));
    
    // check permission failure
    UserPass stooge = new UserPass("stooge", s2bb(""));
    try {
      client.tableOperations_create(stooge, "fail", true, PTimeType.MILLIS);
      fail("should not create the table");
    } catch (TException ex) {
      assertFalse(client.tableOperations_list(creds).contains("fail"));
    }
    // grant permissions and test
    assertFalse(client.securityOperations_hasSystemPermission(creds, "stooge", PSystemPermission.CREATE_TABLE));
    client.securityOperations_grantSystemPermission(creds, "stooge", PSystemPermission.CREATE_TABLE);
    assertTrue(client.securityOperations_hasSystemPermission(creds, "stooge", PSystemPermission.CREATE_TABLE));
    client.tableOperations_create(stooge, "success", true, PTimeType.MILLIS);
    client.tableOperations_list(creds).contains("succcess");
    
    // revoke permissions
    client.securityOperations_revokeSystemPermission(creds, "stooge", PSystemPermission.CREATE_TABLE);
    assertFalse(client.securityOperations_hasSystemPermission(creds, "stooge", PSystemPermission.CREATE_TABLE));
    try {
      client.tableOperations_create(stooge, "fail", true, PTimeType.MILLIS);
      fail("should not create the table");
    } catch (TException ex) {
      assertFalse(client.tableOperations_list(creds).contains("fail"));
    }
    // create a table to test table permissions
    client.tableOperations_create(creds, "test", true, PTimeType.MILLIS);
    // denied!
    try {
      String scanner = client.createScanner(stooge, "test", null, null, null);
      client.scanner_next_k(scanner, 100);
      fail("stooge should not read table test");
    } catch (TException ex) {
    }
    // grant
    assertFalse(client.securityOperations_hasTablePermission(creds, "stooge", "test", PTablePermission.READ));
    client.securityOperations_grantTablePermission(creds, "stooge", "test", PTablePermission.READ);
    assertTrue(client.securityOperations_hasTablePermission(creds, "stooge", "test", PTablePermission.READ));
    String scanner = client.createScanner(stooge, "test", null, null, null);
    client.scanner_next_k(scanner, 10);
    client.close_scanner(scanner);
    // revoke
    client.securityOperations_revokeTablePermission(creds, "stooge", "test", PTablePermission.READ);
    assertFalse(client.securityOperations_hasTablePermission(creds, "stooge", "test", PTablePermission.READ));
    try {
      scanner = client.createScanner(stooge, "test", null, null, null);
      client.scanner_next_k(scanner, 100);
      fail("stooge should not read table test");
    } catch (TException ex) {
    }
    
    // delete user
    client.securityOperations_dropUser(creds, "stooge");
    users = client.securityOperations_listUsers(creds);
    assertEquals(1, users.size());
    
  }
  
  private Map<ByteBuffer,List<PColumnUpdate>> mutation(String row, String cf, String cq, String value) {
    PColumnUpdate upd = new PColumnUpdate(s2bb(cf), s2bb(cq));
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

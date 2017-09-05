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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.UGIAssumingTransport;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloITBase;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.accumulo.harness.TestingKdc;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.proxy.Proxy;
import org.apache.accumulo.proxy.ProxyServer;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.proxy.thrift.AccumuloProxy.Client;
import org.apache.accumulo.proxy.thrift.AccumuloSecurityException;
import org.apache.accumulo.proxy.thrift.ColumnUpdate;
import org.apache.accumulo.proxy.thrift.Key;
import org.apache.accumulo.proxy.thrift.KeyValue;
import org.apache.accumulo.proxy.thrift.ScanOptions;
import org.apache.accumulo.proxy.thrift.ScanResult;
import org.apache.accumulo.proxy.thrift.TimeType;
import org.apache.accumulo.proxy.thrift.WriterOptions;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

/**
 * Tests impersonation of clients over Kerberos+SASL. "Proxy" may be referring to the Accumulo Proxy service or it may be referring to the notion of a username
 * overriding the real username of the actual credentials used in the system. Beware of the context of the word "proxy".
 */
@Category(MiniClusterOnlyTests.class)
public class KerberosProxyIT extends AccumuloITBase {
  private static final Logger log = LoggerFactory.getLogger(KerberosProxyIT.class);
  private static final String PROXIED_USER1 = "proxied_user1", PROXIED_USER2 = "proxied_user2", PROXIED_USER3 = "proxied_user3";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static TestingKdc kdc;
  private static String krbEnabledForITs = null;
  private static File proxyKeytab;
  private static String hostname, proxyPrimary, proxyPrincipal;

  @Override
  protected int defaultTimeoutSeconds() {
    return 60 * 5;
  }

  @BeforeClass
  public static void startKdc() throws Exception {
    kdc = new TestingKdc();
    kdc.start();
    krbEnabledForITs = System.getProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION);
    if (null == krbEnabledForITs || !Boolean.parseBoolean(krbEnabledForITs)) {
      System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, "true");
    }

    // Create a principal+keytab for the proxy
    proxyKeytab = new File(kdc.getKeytabDir(), "proxy.keytab");
    hostname = InetAddress.getLocalHost().getCanonicalHostName();
    // Set the primary because the client needs to know it
    proxyPrimary = "proxy";
    // Qualify with an instance
    proxyPrincipal = proxyPrimary + "/" + hostname;
    kdc.createPrincipal(proxyKeytab, proxyPrincipal);
    // Tack on the realm too
    proxyPrincipal = kdc.qualifyUser(proxyPrincipal);
  }

  @AfterClass
  public static void stopKdc() throws Exception {
    if (null != kdc) {
      kdc.stop();
    }
    if (null != krbEnabledForITs) {
      System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, krbEnabledForITs);
    }
    UserGroupInformation.setConfiguration(new Configuration(false));
  }

  private MiniAccumuloClusterImpl mac;
  private Process proxyProcess;
  private int proxyPort;

  @Before
  public void startMac() throws Exception {
    MiniClusterHarness harness = new MiniClusterHarness();
    mac = harness.create(getClass().getName(), testName.getMethodName(), new PasswordToken("unused"), new MiniClusterConfigurationCallback() {

      @Override
      public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
        cfg.setNumTservers(1);
        Map<String,String> siteCfg = cfg.getSiteConfig();
        // Allow the proxy to impersonate the "root" Accumulo user and our one special user.
        siteCfg.put(Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION.getKey(),
            proxyPrincipal + ":" + kdc.getRootUser().getPrincipal() + "," + kdc.qualifyUser(PROXIED_USER1) + "," + kdc.qualifyUser(PROXIED_USER2));
        siteCfg.put(Property.INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION.getKey(), "*");
        cfg.setSiteConfig(siteCfg);
      }

    }, kdc);

    mac.start();
    MiniAccumuloConfigImpl cfg = mac.getConfig();

    // Generate Proxy configuration and start the proxy
    proxyProcess = startProxy(cfg);

    // Enabled kerberos auth
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);

    boolean success = false;
    ClusterUser rootUser = kdc.getRootUser();
    // Rely on the junit timeout rule
    while (!success) {
      UserGroupInformation ugi;
      try {
        ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
      } catch (IOException ex) {
        log.info("Login as root is failing", ex);
        Thread.sleep(3000);
        continue;
      }

      TSocket socket = new TSocket(hostname, proxyPort);
      log.info("Connecting to proxy with server primary '{}' running on {}", proxyPrimary, hostname);
      TSaslClientTransport transport = new TSaslClientTransport("GSSAPI", null, proxyPrimary, hostname, Collections.singletonMap("javax.security.sasl.qop",
          "auth"), null, socket);

      final UGIAssumingTransport ugiTransport = new UGIAssumingTransport(transport, ugi);

      try {
        // UGI transport will perform the doAs for us
        ugiTransport.open();
        success = true;
      } catch (TTransportException e) {
        Throwable cause = e.getCause();
        if (null != cause && cause instanceof ConnectException) {
          log.info("Proxy not yet up, waiting");
          Thread.sleep(3000);
          proxyProcess = checkProxyAndRestart(proxyProcess, cfg);
          continue;
        }
      } finally {
        if (null != ugiTransport) {
          ugiTransport.close();
        }
      }
    }

    assertTrue("Failed to connect to the proxy repeatedly", success);
  }

  /**
   * Starts the thrift proxy using the given MAConfig.
   *
   * @param cfg
   *          configuration for MAC
   * @return Process for the thrift proxy
   */
  private Process startProxy(MiniAccumuloConfigImpl cfg) throws IOException {
    File proxyPropertiesFile = generateNewProxyConfiguration(cfg);
    return mac.exec(Proxy.class, "-p", proxyPropertiesFile.getCanonicalPath());
  }

  /**
   * Generates a proxy configuration file for the MAC instance. Implicitly updates {@link #proxyPort} when choosing the port the proxy will listen on.
   *
   * @param cfg
   *          The MAC configuration
   * @return The proxy's configuration file
   */
  private File generateNewProxyConfiguration(MiniAccumuloConfigImpl cfg) throws IOException {
    // Chooses a new port for the proxy as side-effect
    proxyPort = PortUtils.getRandomFreePort();

    // Proxy configuration
    File proxyPropertiesFile = new File(cfg.getConfDir(), "proxy.properties");
    if (proxyPropertiesFile.exists()) {
      assertTrue("Failed to delete proxy.properties file", proxyPropertiesFile.delete());
    }
    Properties proxyProperties = new Properties();
    proxyProperties.setProperty("useMockInstance", "false");
    proxyProperties.setProperty("useMiniAccumulo", "false");
    proxyProperties.setProperty("protocolFactory", TCompactProtocol.Factory.class.getName());
    proxyProperties.setProperty("tokenClass", KerberosToken.class.getName());
    proxyProperties.setProperty("port", Integer.toString(proxyPort));
    proxyProperties.setProperty("maxFrameSize", "16M");
    proxyProperties.setProperty("instance", mac.getInstanceName());
    proxyProperties.setProperty("zookeepers", mac.getZooKeepers());
    proxyProperties.setProperty("thriftServerType", "sasl");
    proxyProperties.setProperty("kerberosPrincipal", proxyPrincipal);
    proxyProperties.setProperty("kerberosKeytab", proxyKeytab.getCanonicalPath());

    // Write out the proxy.properties file
    FileWriter writer = new FileWriter(proxyPropertiesFile);
    proxyProperties.store(writer, "Configuration for Accumulo proxy");
    writer.close();

    log.info("Created configuration for proxy listening on {}", proxyPort);

    return proxyPropertiesFile;
  }

  /**
   * Restarts the thrift proxy if the previous instance is no longer running. If the proxy is still running, this method does nothing.
   *
   * @param proxy
   *          The thrift proxy process
   * @param cfg
   *          The MAC configuration
   * @return The process for the Proxy, either the previous instance or a new instance.
   */
  private Process checkProxyAndRestart(Process proxy, MiniAccumuloConfigImpl cfg) throws IOException {
    try {
      // Get the return code
      proxy.exitValue();
    } catch (IllegalThreadStateException e) {
      log.info("Proxy is still running");
      // OK, process is still running, don't restart
      return proxy;
    }

    log.info("Restarting proxy because it is no longer alive");

    // We got a return code which means the proxy exited. We'll assume this is because it failed
    // to bind the port due to the known race condition between choosing a port and having the
    // proxy bind it.
    return startProxy(cfg);
  }

  @After
  public void stopMac() throws Exception {
    if (null != proxyProcess) {
      log.info("Destroying proxy process");
      proxyProcess.destroy();
      log.info("Waiting for proxy termination");
      proxyProcess.waitFor();
      log.info("Proxy terminated");
    }
    if (null != mac) {
      mac.stop();
    }
  }

  @Test
  public void testProxyClient() throws Exception {
    ClusterUser rootUser = kdc.getRootUser();
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());

    TSocket socket = new TSocket(hostname, proxyPort);
    log.info("Connecting to proxy with server primary '{}' running on {}", proxyPrimary, hostname);
    TSaslClientTransport transport = new TSaslClientTransport("GSSAPI", null, proxyPrimary, hostname, Collections.singletonMap("javax.security.sasl.qop",
        "auth"), null, socket);

    final UGIAssumingTransport ugiTransport = new UGIAssumingTransport(transport, ugi);

    // UGI transport will perform the doAs for us
    ugiTransport.open();

    AccumuloProxy.Client.Factory factory = new AccumuloProxy.Client.Factory();
    Client client = factory.getClient(new TCompactProtocol(ugiTransport), new TCompactProtocol(ugiTransport));

    // Will fail if the proxy can impersonate the client
    ByteBuffer login = client.login(rootUser.getPrincipal(), Collections.<String,String> emptyMap());

    // For all of the below actions, the proxy user doesn't have permission to do any of them, but the client user does.
    // The fact that any of them actually run tells us that impersonation is working.

    // Create a table
    String table = "table";
    if (!client.tableExists(login, table)) {
      client.createTable(login, table, true, TimeType.MILLIS);
    }

    // Write two records to the table
    String writer = client.createWriter(login, table, new WriterOptions());
    Map<ByteBuffer,List<ColumnUpdate>> updates = new HashMap<>();
    ColumnUpdate update = new ColumnUpdate(ByteBuffer.wrap("cf1".getBytes(UTF_8)), ByteBuffer.wrap("cq1".getBytes(UTF_8)));
    update.setValue(ByteBuffer.wrap("value1".getBytes(UTF_8)));
    updates.put(ByteBuffer.wrap("row1".getBytes(UTF_8)), Collections.<ColumnUpdate> singletonList(update));
    update = new ColumnUpdate(ByteBuffer.wrap("cf2".getBytes(UTF_8)), ByteBuffer.wrap("cq2".getBytes(UTF_8)));
    update.setValue(ByteBuffer.wrap("value2".getBytes(UTF_8)));
    updates.put(ByteBuffer.wrap("row2".getBytes(UTF_8)), Collections.<ColumnUpdate> singletonList(update));
    client.update(writer, updates);

    // Flush and close the writer
    client.flush(writer);
    client.closeWriter(writer);

    // Open a scanner to the table
    String scanner = client.createScanner(login, table, new ScanOptions());
    ScanResult results = client.nextK(scanner, 10);
    assertEquals(2, results.getResults().size());

    // Check the first key-value
    KeyValue kv = results.getResults().get(0);
    Key k = kv.key;
    ByteBuffer v = kv.value;
    assertEquals(ByteBuffer.wrap("row1".getBytes(UTF_8)), k.row);
    assertEquals(ByteBuffer.wrap("cf1".getBytes(UTF_8)), k.colFamily);
    assertEquals(ByteBuffer.wrap("cq1".getBytes(UTF_8)), k.colQualifier);
    assertEquals(ByteBuffer.wrap(new byte[0]), k.colVisibility);
    assertEquals(ByteBuffer.wrap("value1".getBytes(UTF_8)), v);

    // And then the second
    kv = results.getResults().get(1);
    k = kv.key;
    v = kv.value;
    assertEquals(ByteBuffer.wrap("row2".getBytes(UTF_8)), k.row);
    assertEquals(ByteBuffer.wrap("cf2".getBytes(UTF_8)), k.colFamily);
    assertEquals(ByteBuffer.wrap("cq2".getBytes(UTF_8)), k.colQualifier);
    assertEquals(ByteBuffer.wrap(new byte[0]), k.colVisibility);
    assertEquals(ByteBuffer.wrap("value2".getBytes(UTF_8)), v);

    // Close the scanner
    client.closeScanner(scanner);

    ugiTransport.close();
  }

  @Test
  public void testDisallowedClientForImpersonation() throws Exception {
    String user = testName.getMethodName();
    File keytab = new File(kdc.getKeytabDir(), user + ".keytab");
    kdc.createPrincipal(keytab, user);

    // Login as the new user
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytab.getAbsolutePath());

    log.info("Logged in as {}", ugi);

    // Expect an AccumuloSecurityException
    thrown.expect(AccumuloSecurityException.class);
    // Error msg would look like:
    //
    // org.apache.accumulo.core.client.AccumuloSecurityException: Error BAD_CREDENTIALS for user Principal in credentials object should match kerberos
    // principal.
    // Expected 'proxy/hw10447.local@EXAMPLE.COM' but was 'testDisallowedClientForImpersonation@EXAMPLE.COM' - Username or Password is Invalid)
    thrown.expect(new ThriftExceptionMatchesPattern(".*Error BAD_CREDENTIALS.*"));
    thrown.expect(new ThriftExceptionMatchesPattern(".*Expected '" + proxyPrincipal + "' but was '" + kdc.qualifyUser(user) + "'.*"));

    TSocket socket = new TSocket(hostname, proxyPort);
    log.info("Connecting to proxy with server primary '{}' running on {}", proxyPrimary, hostname);

    // Should fail to open the tran
    TSaslClientTransport transport = new TSaslClientTransport("GSSAPI", null, proxyPrimary, hostname, Collections.singletonMap("javax.security.sasl.qop",
        "auth"), null, socket);

    final UGIAssumingTransport ugiTransport = new UGIAssumingTransport(transport, ugi);

    // UGI transport will perform the doAs for us
    ugiTransport.open();

    AccumuloProxy.Client.Factory factory = new AccumuloProxy.Client.Factory();
    Client client = factory.getClient(new TCompactProtocol(ugiTransport), new TCompactProtocol(ugiTransport));

    // Will fail because the proxy can't impersonate this user (per the site configuration)
    try {
      client.login(kdc.qualifyUser(user), Collections.<String,String> emptyMap());
    } finally {
      if (null != ugiTransport) {
        ugiTransport.close();
      }
    }
  }

  @Test
  public void testMismatchPrincipals() throws Exception {
    ClusterUser rootUser = kdc.getRootUser();
    // Should get an AccumuloSecurityException and the given message
    thrown.expect(AccumuloSecurityException.class);
    thrown.expect(new ThriftExceptionMatchesPattern(ProxyServer.RPC_ACCUMULO_PRINCIPAL_MISMATCH_MSG));

    // Make a new user
    String user = testName.getMethodName();
    File keytab = new File(kdc.getKeytabDir(), user + ".keytab");
    kdc.createPrincipal(keytab, user);

    // Login as the new user
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytab.getAbsolutePath());

    log.info("Logged in as {}", ugi);

    TSocket socket = new TSocket(hostname, proxyPort);
    log.info("Connecting to proxy with server primary '{}' running on {}", proxyPrimary, hostname);

    // Should fail to open the tran
    TSaslClientTransport transport = new TSaslClientTransport("GSSAPI", null, proxyPrimary, hostname, Collections.singletonMap("javax.security.sasl.qop",
        "auth"), null, socket);

    final UGIAssumingTransport ugiTransport = new UGIAssumingTransport(transport, ugi);

    // UGI transport will perform the doAs for us
    ugiTransport.open();

    AccumuloProxy.Client.Factory factory = new AccumuloProxy.Client.Factory();
    Client client = factory.getClient(new TCompactProtocol(ugiTransport), new TCompactProtocol(ugiTransport));

    // The proxy needs to recognize that the requested principal isn't the same as the SASL principal and fail
    // Accumulo should let this through -- we need to rely on the proxy to dump me before talking to accumulo
    try {
      client.login(rootUser.getPrincipal(), Collections.<String,String> emptyMap());
    } finally {
      if (null != ugiTransport) {
        ugiTransport.close();
      }
    }
  }

  @Test
  public void proxiedUserAccessWithoutAccumuloProxy() throws Exception {
    final String tableName = getUniqueNames(1)[0];
    ClusterUser rootUser = kdc.getRootUser();
    final UserGroupInformation rootUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
    final UserGroupInformation realUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(proxyPrincipal, proxyKeytab.getAbsolutePath());
    final String userWithoutCredentials1 = kdc.qualifyUser(PROXIED_USER1);
    final String userWithoutCredentials2 = kdc.qualifyUser(PROXIED_USER2);
    final String userWithoutCredentials3 = kdc.qualifyUser(PROXIED_USER3);
    final UserGroupInformation proxyUser1 = UserGroupInformation.createProxyUser(userWithoutCredentials1, realUgi);
    final UserGroupInformation proxyUser2 = UserGroupInformation.createProxyUser(userWithoutCredentials2, realUgi);
    final UserGroupInformation proxyUser3 = UserGroupInformation.createProxyUser(userWithoutCredentials3, realUgi);

    // Create a table and user, grant permission to our user to read that table.
    rootUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        ZooKeeperInstance inst = new ZooKeeperInstance(mac.getClientConfig());
        Connector conn = inst.getConnector(rootUgi.getUserName(), new KerberosToken());
        conn.tableOperations().create(tableName);
        conn.securityOperations().createLocalUser(userWithoutCredentials1, new PasswordToken("ignored"));
        conn.securityOperations().grantTablePermission(userWithoutCredentials1, tableName, TablePermission.READ);
        conn.securityOperations().createLocalUser(userWithoutCredentials3, new PasswordToken("ignored"));
        conn.securityOperations().grantTablePermission(userWithoutCredentials3, tableName, TablePermission.READ);
        return null;
      }
    });
    realUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        ZooKeeperInstance inst = new ZooKeeperInstance(mac.getClientConfig());
        Connector conn = inst.getConnector(proxyPrincipal, new KerberosToken());
        try {
          Scanner s = conn.createScanner(tableName, Authorizations.EMPTY);
          s.iterator().hasNext();
          Assert.fail("Expected to see an exception");
        } catch (RuntimeException e) {
          int numSecurityExceptionsSeen = Iterables.size(Iterables.filter(Throwables.getCausalChain(e),
              org.apache.accumulo.core.client.AccumuloSecurityException.class));
          assertTrue("Expected to see at least one AccumuloSecurityException, but saw: " + Throwables.getStackTraceAsString(e), numSecurityExceptionsSeen > 0);
        }
        return null;
      }
    });
    // Allowed to be proxied and has read permission
    proxyUser1.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        ZooKeeperInstance inst = new ZooKeeperInstance(mac.getClientConfig());
        Connector conn = inst.getConnector(userWithoutCredentials1, new KerberosToken(userWithoutCredentials1));
        Scanner s = conn.createScanner(tableName, Authorizations.EMPTY);
        assertFalse(s.iterator().hasNext());
        return null;
      }
    });
    // Allowed to be proxied but does not have read permission
    proxyUser2.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        ZooKeeperInstance inst = new ZooKeeperInstance(mac.getClientConfig());
        Connector conn = inst.getConnector(userWithoutCredentials2, new KerberosToken(userWithoutCredentials3));
        try {
          Scanner s = conn.createScanner(tableName, Authorizations.EMPTY);
          s.iterator().hasNext();
          Assert.fail("Expected to see an exception");
        } catch (RuntimeException e) {
          int numSecurityExceptionsSeen = Iterables.size(Iterables.filter(Throwables.getCausalChain(e),
              org.apache.accumulo.core.client.AccumuloSecurityException.class));
          assertTrue("Expected to see at least one AccumuloSecurityException, but saw: " + Throwables.getStackTraceAsString(e), numSecurityExceptionsSeen > 0);
        }
        return null;
      }
    });
    // Has read permission but is not allowed to be proxied
    proxyUser3.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        ZooKeeperInstance inst = new ZooKeeperInstance(mac.getClientConfig());
        try {
          inst.getConnector(userWithoutCredentials3, new KerberosToken(userWithoutCredentials3));
          Assert.fail("Should not be able to create a Connector as this user cannot be proxied");
        } catch (org.apache.accumulo.core.client.AccumuloSecurityException e) {
          // Expected, this user cannot be proxied
        }
        return null;
      }
    });
  }

  private static class ThriftExceptionMatchesPattern extends TypeSafeMatcher<AccumuloSecurityException> {
    private String pattern;

    public ThriftExceptionMatchesPattern(String pattern) {
      this.pattern = pattern;
    }

    @Override
    protected boolean matchesSafely(AccumuloSecurityException item) {
      return item.isSetMsg() && item.msg.matches(pattern);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("matches pattern ").appendValue(pattern);
    }

    @Override
    protected void describeMismatchSafely(AccumuloSecurityException item, Description mismatchDescription) {
      mismatchDescription.appendText("does not match");
    }
  }
}

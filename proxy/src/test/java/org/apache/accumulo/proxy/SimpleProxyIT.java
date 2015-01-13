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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.examples.simple.constraints.NumericValueConstraint;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.proxy.thrift.AccumuloProxy.Client;
import org.apache.accumulo.proxy.thrift.AccumuloSecurityException;
import org.apache.accumulo.proxy.thrift.ActiveCompaction;
import org.apache.accumulo.proxy.thrift.ActiveScan;
import org.apache.accumulo.proxy.thrift.BatchScanOptions;
import org.apache.accumulo.proxy.thrift.Column;
import org.apache.accumulo.proxy.thrift.ColumnUpdate;
import org.apache.accumulo.proxy.thrift.CompactionReason;
import org.apache.accumulo.proxy.thrift.CompactionType;
import org.apache.accumulo.proxy.thrift.Condition;
import org.apache.accumulo.proxy.thrift.ConditionalStatus;
import org.apache.accumulo.proxy.thrift.ConditionalUpdates;
import org.apache.accumulo.proxy.thrift.ConditionalWriterOptions;
import org.apache.accumulo.proxy.thrift.DiskUsage;
import org.apache.accumulo.proxy.thrift.IteratorScope;
import org.apache.accumulo.proxy.thrift.IteratorSetting;
import org.apache.accumulo.proxy.thrift.Key;
import org.apache.accumulo.proxy.thrift.KeyValue;
import org.apache.accumulo.proxy.thrift.MutationsRejectedException;
import org.apache.accumulo.proxy.thrift.PartialKey;
import org.apache.accumulo.proxy.thrift.Range;
import org.apache.accumulo.proxy.thrift.ScanColumn;
import org.apache.accumulo.proxy.thrift.ScanOptions;
import org.apache.accumulo.proxy.thrift.ScanResult;
import org.apache.accumulo.proxy.thrift.ScanState;
import org.apache.accumulo.proxy.thrift.ScanType;
import org.apache.accumulo.proxy.thrift.SystemPermission;
import org.apache.accumulo.proxy.thrift.TableExistsException;
import org.apache.accumulo.proxy.thrift.TableNotFoundException;
import org.apache.accumulo.proxy.thrift.TablePermission;
import org.apache.accumulo.proxy.thrift.TimeType;
import org.apache.accumulo.proxy.thrift.UnknownScanner;
import org.apache.accumulo.proxy.thrift.UnknownWriter;
import org.apache.accumulo.proxy.thrift.WriterOptions;
import org.apache.accumulo.server.util.PortUtils;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

/**
 * Call every method on the proxy and try to verify that it works.
 */
public class SimpleProxyIT {

  public static File macTestFolder = new File(System.getProperty("user.dir") + "/target/" + SimpleProxyIT.class.getName());

  private static MiniAccumuloCluster accumulo;
  private static String secret = "superSecret";
  private static Random random = new Random();
  private static TServer proxyServer;
  private static Thread thread;
  private static int proxyPort;
  private static org.apache.accumulo.proxy.thrift.AccumuloProxy.Client client;
  private static String principal = "root";

  private static Map<String,String> properties = new TreeMap<String,String>() {
    private static final long serialVersionUID = 1L;

    {
      put("password", secret);
    }
  };
  private static ByteBuffer creds = null;

  private static Class<? extends TProtocolFactory> protocolClass;

  static Class<? extends TProtocolFactory> getRandomProtocol() {
    List<Class<? extends TProtocolFactory>> protocolFactories = new ArrayList<Class<? extends TProtocolFactory>>();
    protocolFactories.add(org.apache.thrift.protocol.TJSONProtocol.Factory.class);
    protocolFactories.add(org.apache.thrift.protocol.TBinaryProtocol.Factory.class);
    protocolFactories.add(org.apache.thrift.protocol.TTupleProtocol.Factory.class);
    protocolFactories.add(org.apache.thrift.protocol.TCompactProtocol.Factory.class);

    return protocolFactories.get(random.nextInt(protocolFactories.size()));
  }

  private static final AtomicInteger tableCounter = new AtomicInteger(0);

  private static String makeTableName() {
    return "test" + tableCounter.getAndIncrement();
  }

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder(macTestFolder);

  @Rule
  public TestName testName = new TestName();

  @Rule
  public Timeout testsShouldTimeout() {
    int waitLonger;
    try {
      waitLonger = Integer.parseInt(System.getProperty("timeout.factor"));
    } catch (NumberFormatException e) {
      waitLonger = 1;
    }

    return new Timeout(waitLonger * 60 * 1000);
  }

  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    FileUtils.deleteQuietly(macTestFolder);
    macTestFolder.mkdirs();
    MiniAccumuloConfig config = new MiniAccumuloConfig(macTestFolder, secret).setNumTservers(1);
    accumulo = new MiniAccumuloCluster(config);
    accumulo.start();
    // wait for accumulo to be up and functional
    ZooKeeperInstance zoo = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
    Connector c = zoo.getConnector("root", new PasswordToken(secret.getBytes()));
    for (@SuppressWarnings("unused")
    Entry<org.apache.accumulo.core.data.Key,Value> entry : c.createScanner(MetadataTable.NAME, Authorizations.EMPTY))
      ;

    Properties props = new Properties();
    props.put("instance", accumulo.getConfig().getInstanceName());
    props.put("zookeepers", accumulo.getZooKeepers());
    props.put("tokenClass", PasswordToken.class.getName());

    protocolClass = getRandomProtocol();

    proxyPort = PortUtils.getRandomFreePort();
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

  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    if (null != proxyServer) {
      proxyServer.stop();
      thread.interrupt();
      thread.join(5000);
    }
    accumulo.stop();
    FileUtils.deleteQuietly(macTestFolder);
  }

  @Test
  public void security() throws Exception {
    client.createLocalUser(creds, "user", s2bb(secret));
    ByteBuffer badLogin = client.login("user", properties);
    client.dropLocalUser(creds, "user");
    final String table = makeTableName();
    client.createTable(creds, table, false, TimeType.MILLIS);

    final IteratorSetting setting = new IteratorSetting(100, "slow", SlowIterator.class.getName(), Collections.singletonMap("sleepTime", "200"));

    try {
      client.addConstraint(badLogin, table, NumericValueConstraint.class.getName());
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.addSplits(badLogin, table, Collections.singleton(s2bb("1")));
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.clearLocatorCache(badLogin, table);
      fail("exception not thrown");
    } catch (TException ex) {}
    try {
      client.compactTable(badLogin, table, null, null, null, true, false);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.cancelCompaction(badLogin, table);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.createTable(badLogin, table, false, TimeType.MILLIS);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.deleteTable(badLogin, table);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.deleteRows(badLogin, table, null, null);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.tableExists(badLogin, table);
      fail("exception not thrown");
    } catch (TException ex) {}
    try {
      client.flushTable(badLogin, table, null, null, false);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.getLocalityGroups(badLogin, table);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.getMaxRow(badLogin, table, Collections.<ByteBuffer> emptySet(), null, false, null, false);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.getTableProperties(badLogin, table);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.listSplits(badLogin, table, 10000);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.listTables(badLogin);
      fail("exception not thrown");
    } catch (TException ex) {}
    try {
      client.listConstraints(badLogin, table);
      fail("exception not thrown");
    } catch (TException ex) {}
    try {
      client.mergeTablets(badLogin, table, null, null);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.offlineTable(badLogin, table, false);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.onlineTable(badLogin, table, false);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.removeConstraint(badLogin, table, 0);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.removeTableProperty(badLogin, table, Property.TABLE_FILE_MAX.getKey());
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.renameTable(badLogin, table, "someTableName");
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      Map<String,Set<String>> groups = new HashMap<String,Set<String>>();
      groups.put("group1", Collections.singleton("cf1"));
      groups.put("group2", Collections.singleton("cf2"));
      client.setLocalityGroups(badLogin, table, groups);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.setTableProperty(badLogin, table, Property.TABLE_FILE_MAX.getKey(), "0");
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.tableIdMap(badLogin);
      fail("exception not thrown");
    } catch (TException ex) {}
    try {
      client.getSiteConfiguration(badLogin);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.getSystemConfiguration(badLogin);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.getTabletServers(badLogin);
      fail("exception not thrown");
    } catch (TException ex) {}
    try {
      client.getActiveScans(badLogin, "fake");
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.getActiveCompactions(badLogin, "fakse");
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.removeProperty(badLogin, "table.split.threshold");
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.setProperty(badLogin, "table.split.threshold", "500M");
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.testClassLoad(badLogin, DevNull.class.getName(), SortedKeyValueIterator.class.getName());
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.authenticateUser(badLogin, "root", s2pp(secret));
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      HashSet<ByteBuffer> auths = new HashSet<ByteBuffer>(Arrays.asList(s2bb("A"), s2bb("B")));
      client.changeUserAuthorizations(badLogin, "stooge", auths);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.changeLocalUserPassword(badLogin, "stooge", s2bb(""));
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.createLocalUser(badLogin, "stooge", s2bb("password"));
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.dropLocalUser(badLogin, "stooge");
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.getUserAuthorizations(badLogin, "stooge");
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.grantSystemPermission(badLogin, "stooge", SystemPermission.CREATE_TABLE);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.grantTablePermission(badLogin, "root", table, TablePermission.WRITE);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.hasSystemPermission(badLogin, "stooge", SystemPermission.CREATE_TABLE);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.hasTablePermission(badLogin, "root", table, TablePermission.WRITE);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.listLocalUsers(badLogin);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.revokeSystemPermission(badLogin, "stooge", SystemPermission.CREATE_TABLE);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.revokeTablePermission(badLogin, "root", table, TablePermission.ALTER_TABLE);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.createScanner(badLogin, table, new ScanOptions());
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.createBatchScanner(badLogin, table, new BatchScanOptions());
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.updateAndFlush(badLogin, table, new HashMap<ByteBuffer,List<ColumnUpdate>>());
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.createWriter(badLogin, table, new WriterOptions());
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.attachIterator(badLogin, "slow", setting, EnumSet.allOf(IteratorScope.class));
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.checkIteratorConflicts(badLogin, table, setting, EnumSet.allOf(IteratorScope.class));
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      final String TABLE_TEST = makeTableName();
      client.cloneTable(badLogin, table, TABLE_TEST, false, null, null);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.exportTable(badLogin, table, "/tmp");
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.importTable(badLogin, "testify", "/tmp");
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.getIteratorSetting(badLogin, table, "foo", IteratorScope.SCAN);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.listIterators(badLogin, table);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.removeIterator(badLogin, table, "name", EnumSet.allOf(IteratorScope.class));
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.splitRangeByTablets(badLogin, table, client.getRowRange(ByteBuffer.wrap("row".getBytes())), 10);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      File importDir = tempFolder.newFolder("importDir");
      File failuresDir = tempFolder.newFolder("failuresDir");
      client.importDirectory(badLogin, table, importDir.getAbsolutePath(), failuresDir.getAbsolutePath(), true);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.pingTabletServer(badLogin, "fake");
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.login("badUser", properties);
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.testTableClassLoad(badLogin, table, VersioningIterator.class.getName(), SortedKeyValueIterator.class.getName());
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
    try {
      client.createConditionalWriter(badLogin, table, new ConditionalWriterOptions());
      fail("exception not thrown");
    } catch (AccumuloSecurityException ex) {}
  }

  @Test
  public void tableNotFound() throws Exception {
    final String doesNotExist = "doesNotExists";
    try {
      client.addConstraint(creds, doesNotExist, NumericValueConstraint.class.getName());
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.addSplits(creds, doesNotExist, Collections.<ByteBuffer> emptySet());
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    final IteratorSetting setting = new IteratorSetting(100, "slow", SlowIterator.class.getName(), Collections.singletonMap("sleepTime", "200"));
    try {
      client.attachIterator(creds, doesNotExist, setting, EnumSet.allOf(IteratorScope.class));
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.cancelCompaction(creds, doesNotExist);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.checkIteratorConflicts(creds, doesNotExist, setting, EnumSet.allOf(IteratorScope.class));
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.clearLocatorCache(creds, doesNotExist);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      final String TABLE_TEST = makeTableName();
      client.cloneTable(creds, doesNotExist, TABLE_TEST, false, null, null);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.compactTable(creds, doesNotExist, null, null, null, true, false);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.createBatchScanner(creds, doesNotExist, new BatchScanOptions());
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.createScanner(creds, doesNotExist, new ScanOptions());
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.createWriter(creds, doesNotExist, new WriterOptions());
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.deleteRows(creds, doesNotExist, null, null);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.deleteTable(creds, doesNotExist);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.exportTable(creds, doesNotExist, "/tmp");
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.flushTable(creds, doesNotExist, null, null, false);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.getIteratorSetting(creds, doesNotExist, "foo", IteratorScope.SCAN);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.getLocalityGroups(creds, doesNotExist);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.getMaxRow(creds, doesNotExist, Collections.<ByteBuffer> emptySet(), null, false, null, false);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.getTableProperties(creds, doesNotExist);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.grantTablePermission(creds, "root", doesNotExist, TablePermission.WRITE);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.hasTablePermission(creds, "root", doesNotExist, TablePermission.WRITE);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      File importDir = tempFolder.newFolder("importDir");
      File failuresDir = tempFolder.newFolder("failuresDir");
      client.importDirectory(creds, doesNotExist, importDir.getAbsolutePath(), failuresDir.getAbsolutePath(), true);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.listConstraints(creds, doesNotExist);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.listSplits(creds, doesNotExist, 10000);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.mergeTablets(creds, doesNotExist, null, null);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.offlineTable(creds, doesNotExist, false);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.onlineTable(creds, doesNotExist, false);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.removeConstraint(creds, doesNotExist, 0);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.removeIterator(creds, doesNotExist, "name", EnumSet.allOf(IteratorScope.class));
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.removeTableProperty(creds, doesNotExist, Property.TABLE_FILE_MAX.getKey());
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.renameTable(creds, doesNotExist, "someTableName");
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.revokeTablePermission(creds, "root", doesNotExist, TablePermission.ALTER_TABLE);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.setTableProperty(creds, doesNotExist, Property.TABLE_FILE_MAX.getKey(), "0");
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.splitRangeByTablets(creds, doesNotExist, client.getRowRange(ByteBuffer.wrap("row".getBytes())), 10);
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.updateAndFlush(creds, doesNotExist, new HashMap<ByteBuffer,List<ColumnUpdate>>());
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.getDiskUsage(creds, Collections.singleton(doesNotExist));
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.testTableClassLoad(creds, doesNotExist, VersioningIterator.class.getName(), SortedKeyValueIterator.class.getName());
      fail("exception not thrown");
    } catch (TableNotFoundException ex) {}
    try {
      client.createConditionalWriter(creds, doesNotExist, new ConditionalWriterOptions());
    } catch (TableNotFoundException ex) {}
  }

  @Test
  public void testExists() throws Exception {
    client.createTable(creds, "ett1", false, TimeType.MILLIS);
    client.createTable(creds, "ett2", false, TimeType.MILLIS);
    try {
      client.createTable(creds, "ett1", false, TimeType.MILLIS);
      fail("exception not thrown");
    } catch (TableExistsException tee) {}
    try {
      client.renameTable(creds, "ett1", "ett2");
      fail("exception not thrown");
    } catch (TableExistsException tee) {}
    try {
      client.cloneTable(creds, "ett1", "ett2", false, new HashMap<String,String>(), new HashSet<String>());
      fail("exception not thrown");
    } catch (TableExistsException tee) {}
  }

  @Test
  public void testUnknownScanner() throws Exception {
    final String TABLE_TEST = makeTableName();

    client.createTable(creds, TABLE_TEST, true, TimeType.MILLIS);

    String scanner = client.createScanner(creds, TABLE_TEST, null);
    assertFalse(client.hasNext(scanner));
    client.closeScanner(scanner);

    try {
      client.hasNext(scanner);
      fail("exception not thrown");
    } catch (UnknownScanner us) {}

    try {
      client.closeScanner(scanner);
      fail("exception not thrown");
    } catch (UnknownScanner us) {}

    try {
      client.nextEntry("99999999");
      fail("exception not thrown");
    } catch (UnknownScanner us) {}
    try {
      client.nextK("99999999", 6);
      fail("exception not thrown");
    } catch (UnknownScanner us) {}
    try {
      client.hasNext("99999999");
      fail("exception not thrown");
    } catch (UnknownScanner us) {}
    try {
      client.hasNext(UUID.randomUUID().toString());
      fail("exception not thrown");
    } catch (UnknownScanner us) {}
  }

  @Test
  public void testUnknownWriter() throws Exception {
    final String TABLE_TEST = makeTableName();

    client.createTable(creds, TABLE_TEST, true, TimeType.MILLIS);

    String writer = client.createWriter(creds, TABLE_TEST, null);
    client.update(writer, mutation("row0", "cf", "cq", "value"));
    client.flush(writer);
    client.update(writer, mutation("row2", "cf", "cq", "value2"));
    client.closeWriter(writer);

    // this is a oneway call, so it does not throw exceptions
    client.update(writer, mutation("row2", "cf", "cq", "value2"));

    try {
      client.flush(writer);
      fail("exception not thrown");
    } catch (UnknownWriter uw) {}
    try {
      client.flush("99999");
      fail("exception not thrown");
    } catch (UnknownWriter uw) {}
    try {
      client.flush(UUID.randomUUID().toString());
      fail("exception not thrown");
    } catch (UnknownWriter uw) {}
    try {
      client.closeWriter("99999");
      fail("exception not thrown");
    } catch (UnknownWriter uw) {}
  }

  @Test
  public void testDelete() throws Exception {
    final String TABLE_TEST = makeTableName();

    client.createTable(creds, TABLE_TEST, true, TimeType.MILLIS);
    client.updateAndFlush(creds, TABLE_TEST, mutation("row0", "cf", "cq", "value"));

    assertScan(new String[][] {{"row0", "cf", "cq", "value"}}, TABLE_TEST);

    ColumnUpdate upd = new ColumnUpdate(s2bb("cf"), s2bb("cq"));
    upd.setDeleteCell(false);
    Map<ByteBuffer,List<ColumnUpdate>> notDelete = Collections.singletonMap(s2bb("row0"), Collections.singletonList(upd));
    client.updateAndFlush(creds, TABLE_TEST, notDelete);
    String scanner = client.createScanner(creds, TABLE_TEST, null);
    ScanResult entries = client.nextK(scanner, 10);
    client.closeScanner(scanner);
    assertFalse(entries.more);
    assertEquals(1, entries.results.size());

    upd = new ColumnUpdate(s2bb("cf"), s2bb("cq"));
    upd.setDeleteCell(true);
    Map<ByteBuffer,List<ColumnUpdate>> delete = Collections.singletonMap(s2bb("row0"), Collections.singletonList(upd));

    client.updateAndFlush(creds, TABLE_TEST, delete);

    assertScan(new String[][] {}, TABLE_TEST);
  }

  @Test
  public void testInstanceOperations() throws Exception {
    int tservers = 0;
    for (String tserver : client.getTabletServers(creds)) {
      client.pingTabletServer(creds, tserver);
      tservers++;
    }
    assertTrue(tservers > 0);

    // get something we know is in the site config
    Map<String,String> cfg = client.getSiteConfiguration(creds);
    assertTrue(cfg.get("instance.dfs.dir").startsWith(macTestFolder.getPath()));

    // set a property in zookeeper
    client.setProperty(creds, "table.split.threshold", "500M");

    // check that we can read it
    for (int i = 0; i < 5; i++) {
      cfg = client.getSystemConfiguration(creds);
      if ("500M".equals(cfg.get("table.split.threshold")))
        break;
      UtilWaitThread.sleep(200);
    }
    assertEquals("500M", cfg.get("table.split.threshold"));

    // unset the setting, check that it's not what it was
    client.removeProperty(creds, "table.split.threshold");
    for (int i = 0; i < 5; i++) {
      cfg = client.getSystemConfiguration(creds);
      if (!"500M".equals(cfg.get("table.split.threshold")))
        break;
      UtilWaitThread.sleep(200);
    }
    assertNotEquals("500M", cfg.get("table.split.threshold"));

    // try to load some classes via the proxy
    assertTrue(client.testClassLoad(creds, DevNull.class.getName(), SortedKeyValueIterator.class.getName()));
    assertFalse(client.testClassLoad(creds, "foo.bar", SortedKeyValueIterator.class.getName()));

    // create a table that's very slow, so we can look for scans/compactions
    client.createTable(creds, "slow", true, TimeType.MILLIS);
    IteratorSetting setting = new IteratorSetting(100, "slow", SlowIterator.class.getName(), Collections.singletonMap("sleepTime", "250"));
    client.attachIterator(creds, "slow", setting, EnumSet.allOf(IteratorScope.class));

    // Should take 10 seconds to read every record
    for (int i = 0; i < 40; i++) {
      client.updateAndFlush(creds, "slow", mutation("row" + i, "cf", "cq", "value"));
    }

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

    // look for the scan many times
    List<ActiveScan> scans = new ArrayList<ActiveScan>();
    for (int i = 0; i < 100 && scans.isEmpty(); i++) {
      for (String tserver : client.getTabletServers(creds)) {
        List<ActiveScan> scansForServer = client.getActiveScans(creds, tserver);
        for (ActiveScan scan : scansForServer) {
          if ("root".equals(scan.getUser())) {
            scans.add(scan);
          }
        }

        if (!scans.isEmpty())
          break;
        UtilWaitThread.sleep(100);
      }
    }
    t.join();

    assertFalse(scans.isEmpty());
    boolean found = false;
    Map<String,String> map = null;
    for (int i = 0; i < scans.size() && !found; i++) {
      ActiveScan scan = scans.get(i);
      if ("root".equals(scan.getUser())) {
        assertTrue(ScanState.RUNNING.equals(scan.getState()) || ScanState.QUEUED.equals(scan.getState()));
        assertEquals(ScanType.SINGLE, scan.getType());
        assertEquals("slow", scan.getTable());

        map = client.tableIdMap(creds);
        assertEquals(map.get("slow"), scan.getExtent().tableId);
        assertTrue(scan.getExtent().endRow == null);
        assertTrue(scan.getExtent().prevEndRow == null);
        found = true;
      }
    }

    assertTrue("Could not find a scan against the 'slow' table", found);

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

    final String desiredTableId = map.get("slow");

    // try to catch it in the act
    List<ActiveCompaction> compactions = new ArrayList<ActiveCompaction>();
    for (int i = 0; i < 100 && compactions.isEmpty(); i++) {
      // Iterate over the tservers
      for (String tserver : client.getTabletServers(creds)) {
        // And get the compactions on each
        List<ActiveCompaction> compactionsOnServer = client.getActiveCompactions(creds, tserver);
        for (ActiveCompaction compact : compactionsOnServer) {
          // There might be other compactions occurring (e.g. on METADATA) in which
          // case we want to prune out those that aren't for our slow table
          if (desiredTableId.equals(compact.getExtent().tableId)) {
            compactions.add(compact);
          }
        }

        // If we found a compaction for the table we wanted, so we can stop looking
        if (!compactions.isEmpty())
          break;
      }
      UtilWaitThread.sleep(10);
    }
    t.join();

    // verify the compaction information
    assertFalse(compactions.isEmpty());
    for (ActiveCompaction c : compactions) {
      if (desiredTableId.equals(c.getExtent().tableId)) {
        assertTrue(c.inputFiles.isEmpty());
        assertEquals(CompactionType.MINOR, c.getType());
        assertEquals(CompactionReason.USER, c.getReason());
        assertEquals("", c.localityGroup);
        assertTrue(c.outputFile.contains("default_tablet"));

        return;
      }
    }
    fail("Expection to find running compaction for table 'slow' but did not find one");
  }

  @Test
  public void testSecurityOperations() throws Exception {
    final String TABLE_TEST = makeTableName();

    // check password
    assertTrue(client.authenticateUser(creds, "root", s2pp(secret)));
    assertFalse(client.authenticateUser(creds, "root", s2pp("")));

    // create a user
    client.createLocalUser(creds, "stooge", s2bb("password"));
    // change auths
    Set<String> users = client.listLocalUsers(creds);
    assertEquals(new HashSet<String>(Arrays.asList("root", "stooge")), users);
    HashSet<ByteBuffer> auths = new HashSet<ByteBuffer>(Arrays.asList(s2bb("A"), s2bb("B")));
    client.changeUserAuthorizations(creds, "stooge", auths);
    List<ByteBuffer> update = client.getUserAuthorizations(creds, "stooge");
    assertEquals(auths, new HashSet<ByteBuffer>(update));

    // change password
    client.changeLocalUserPassword(creds, "stooge", s2bb(""));
    assertTrue(client.authenticateUser(creds, "stooge", s2pp("")));

    // check permission failure
    @SuppressWarnings("serial")
    ByteBuffer stooge = client.login("stooge", new TreeMap<String,String>() {
      {
        put("password", "");
      }
    });

    try {
      client.createTable(stooge, "fail", true, TimeType.MILLIS);
      fail("should not create the table");
    } catch (AccumuloSecurityException ex) {
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
    } catch (AccumuloSecurityException ex) {
      assertFalse(client.listTables(creds).contains("fail"));
    }
    // create a table to test table permissions
    client.createTable(creds, TABLE_TEST, true, TimeType.MILLIS);
    // denied!
    try {
      String scanner = client.createScanner(stooge, TABLE_TEST, null);
      client.nextK(scanner, 100);
      fail("stooge should not read table test");
    } catch (AccumuloSecurityException ex) {}
    // grant
    assertFalse(client.hasTablePermission(creds, "stooge", TABLE_TEST, TablePermission.READ));
    client.grantTablePermission(creds, "stooge", TABLE_TEST, TablePermission.READ);
    assertTrue(client.hasTablePermission(creds, "stooge", TABLE_TEST, TablePermission.READ));
    String scanner = client.createScanner(stooge, TABLE_TEST, null);
    client.nextK(scanner, 10);
    client.closeScanner(scanner);
    // revoke
    client.revokeTablePermission(creds, "stooge", TABLE_TEST, TablePermission.READ);
    assertFalse(client.hasTablePermission(creds, "stooge", TABLE_TEST, TablePermission.READ));
    try {
      scanner = client.createScanner(stooge, TABLE_TEST, null);
      client.nextK(scanner, 100);
      fail("stooge should not read table test");
    } catch (AccumuloSecurityException ex) {}

    // delete user
    client.dropLocalUser(creds, "stooge");
    users = client.listLocalUsers(creds);
    assertEquals(1, users.size());

  }

  @Test
  public void testBatchWriter() throws Exception {
    final String TABLE_TEST = makeTableName();

    client.createTable(creds, TABLE_TEST, true, TimeType.MILLIS);
    client.addConstraint(creds, TABLE_TEST, NumericValueConstraint.class.getName());

    WriterOptions writerOptions = new WriterOptions();
    writerOptions.setLatencyMs(10000);
    writerOptions.setMaxMemory(2);
    writerOptions.setThreads(1);
    writerOptions.setTimeoutMs(100000);

    String batchWriter = client.createWriter(creds, TABLE_TEST, writerOptions);
    client.update(batchWriter, mutation("row1", "cf", "cq", "x"));
    client.update(batchWriter, mutation("row1", "cf", "cq", "x"));
    try {
      client.flush(batchWriter);
      fail("constraint did not fire");
    } catch (MutationsRejectedException ex) {}
    try {
      client.closeWriter(batchWriter);
      fail("constraint did not fire");
    } catch (MutationsRejectedException e) {}

    client.removeConstraint(creds, TABLE_TEST, 2);

    assertScan(new String[][] {}, TABLE_TEST);

    UtilWaitThread.sleep(2000);

    writerOptions = new WriterOptions();
    writerOptions.setLatencyMs(10000);
    writerOptions.setMaxMemory(3000);
    writerOptions.setThreads(1);
    writerOptions.setTimeoutMs(100000);

    batchWriter = client.createWriter(creds, TABLE_TEST, writerOptions);

    client.update(batchWriter, mutation("row1", "cf", "cq", "x"));
    client.flush(batchWriter);
    client.closeWriter(batchWriter);

    assertScan(new String[][] {{"row1", "cf", "cq", "x"}}, TABLE_TEST);

    client.deleteTable(creds, TABLE_TEST);
  }

  @Test
  public void testTableOperations() throws Exception {
    final String TABLE_TEST = makeTableName();

    client.createTable(creds, TABLE_TEST, true, TimeType.MILLIS);
    // constraints
    client.addConstraint(creds, TABLE_TEST, NumericValueConstraint.class.getName());
    assertEquals(2, client.listConstraints(creds, TABLE_TEST).size());

    UtilWaitThread.sleep(2000);

    client.updateAndFlush(creds, TABLE_TEST, mutation("row1", "cf", "cq", "123"));

    try {
      client.updateAndFlush(creds, TABLE_TEST, mutation("row1", "cf", "cq", "x"));
      fail("constraint did not fire");
    } catch (MutationsRejectedException ex) {}

    client.removeConstraint(creds, TABLE_TEST, 2);

    UtilWaitThread.sleep(2000);

    assertEquals(1, client.listConstraints(creds, TABLE_TEST).size());

    client.updateAndFlush(creds, TABLE_TEST, mutation("row1", "cf", "cq", "x"));
    assertScan(new String[][] {{"row1", "cf", "cq", "x"}}, TABLE_TEST);
    // splits, merge
    client.addSplits(creds, TABLE_TEST, new HashSet<ByteBuffer>(Arrays.asList(s2bb("a"), s2bb("m"), s2bb("z"))));
    List<ByteBuffer> splits = client.listSplits(creds, TABLE_TEST, 1);
    assertEquals(Arrays.asList(s2bb("m")), splits);
    client.mergeTablets(creds, TABLE_TEST, null, s2bb("m"));
    splits = client.listSplits(creds, TABLE_TEST, 10);
    assertEquals(Arrays.asList(s2bb("m"), s2bb("z")), splits);
    client.mergeTablets(creds, TABLE_TEST, null, null);
    splits = client.listSplits(creds, TABLE_TEST, 10);
    List<ByteBuffer> empty = Collections.emptyList();
    assertEquals(empty, splits);
    // iterators
    client.deleteTable(creds, TABLE_TEST);
    client.createTable(creds, TABLE_TEST, true, TimeType.MILLIS);
    HashMap<String,String> options = new HashMap<String,String>();
    options.put("type", "STRING");
    options.put("columns", "cf");
    IteratorSetting setting = new IteratorSetting(10, TABLE_TEST, SummingCombiner.class.getName(), options);
    client.attachIterator(creds, TABLE_TEST, setting, EnumSet.allOf(IteratorScope.class));
    for (int i = 0; i < 10; i++) {
      client.updateAndFlush(creds, TABLE_TEST, mutation("row1", "cf", "cq", "1"));
    }
    assertScan(new String[][] {{"row1", "cf", "cq", "10"}}, TABLE_TEST);
    try {
      client.checkIteratorConflicts(creds, TABLE_TEST, setting, EnumSet.allOf(IteratorScope.class));
      fail("checkIteratorConflicts did not throw an exception");
    } catch (Exception ex) {}
    client.deleteRows(creds, TABLE_TEST, null, null);
    client.removeIterator(creds, TABLE_TEST, "test", EnumSet.allOf(IteratorScope.class));
    String expected[][] = new String[10][];
    for (int i = 0; i < 10; i++) {
      client.updateAndFlush(creds, TABLE_TEST, mutation("row" + i, "cf", "cq", "" + i));
      expected[i] = new String[] {"row" + i, "cf", "cq", "" + i};
      client.flushTable(creds, TABLE_TEST, null, null, true);
    }
    assertScan(expected, TABLE_TEST);
    // clone
    final String TABLE_TEST2 = makeTableName();
    client.cloneTable(creds, TABLE_TEST, TABLE_TEST2, true, null, null);
    assertScan(expected, TABLE_TEST2);
    client.deleteTable(creds, TABLE_TEST2);

    // don't know how to test this, call it just for fun
    client.clearLocatorCache(creds, TABLE_TEST);

    // compact
    client.compactTable(creds, TABLE_TEST, null, null, null, true, true);
    assertEquals(1, countFiles(TABLE_TEST));
    assertScan(expected, TABLE_TEST);

    // get disk usage
    client.cloneTable(creds, TABLE_TEST, TABLE_TEST2, true, null, null);
    Set<String> tablesToScan = new HashSet<String>();
    tablesToScan.add(TABLE_TEST);
    tablesToScan.add(TABLE_TEST2);
    tablesToScan.add("foo");
    client.createTable(creds, "foo", true, TimeType.MILLIS);
    List<DiskUsage> diskUsage = (client.getDiskUsage(creds, tablesToScan));
    assertEquals(2, diskUsage.size());
    assertEquals(1, diskUsage.get(0).getTables().size());
    assertEquals(2, diskUsage.get(1).getTables().size());
    client.compactTable(creds, TABLE_TEST2, null, null, null, true, true);
    diskUsage = (client.getDiskUsage(creds, tablesToScan));
    assertEquals(3, diskUsage.size());
    assertEquals(1, diskUsage.get(0).getTables().size());
    assertEquals(1, diskUsage.get(1).getTables().size());
    assertEquals(1, diskUsage.get(2).getTables().size());
    client.deleteTable(creds, "foo");
    client.deleteTable(creds, TABLE_TEST2);

    // export/import
    File dir = tempFolder.newFolder("test");
    File destDir = tempFolder.newFolder("test_dest");
    client.offlineTable(creds, TABLE_TEST, false);
    client.exportTable(creds, TABLE_TEST, dir.getAbsolutePath());
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
    client.deleteTable(creds, TABLE_TEST);
    client.importTable(creds, "testify", destDir.getAbsolutePath());
    assertScan(expected, "testify");
    client.deleteTable(creds, "testify");

    try {
      // ACCUMULO-1558 a second import from the same dir should fail, the first import moved the files
      client.importTable(creds, "testify2", destDir.getAbsolutePath());
      fail();
    } catch (Exception e) {}

    assertFalse(client.listTables(creds).contains("testify2"));

    // Locality groups
    client.createTable(creds, "test", true, TimeType.MILLIS);
    Map<String,Set<String>> groups = new HashMap<String,Set<String>>();
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
    String scanner = client.createScanner(creds, "bar", null);
    ScanResult more = client.nextK(scanner, 100);
    client.closeScanner(scanner);
    assertEquals(1, more.results.size());
    ByteBuffer maxRow = client.getMaxRow(creds, "bar", null, null, false, null, false);
    assertEquals(s2bb("a"), maxRow);

    assertFalse(client.testTableClassLoad(creds, "bar", "abc123", SortedKeyValueIterator.class.getName()));
    assertTrue(client.testTableClassLoad(creds, "bar", VersioningIterator.class.getName(), SortedKeyValueIterator.class.getName()));
  }

  private Condition newCondition(String cf, String cq) {
    return new Condition(new Column(s2bb(cf), s2bb(cq), s2bb("")));
  }

  private Condition newCondition(String cf, String cq, String val) {
    return newCondition(cf, cq).setValue(s2bb(val));
  }

  private Condition newCondition(String cf, String cq, long ts, String val) {
    return newCondition(cf, cq).setValue(s2bb(val)).setTimestamp(ts);
  }

  private ColumnUpdate newColUpdate(String cf, String cq, String val) {
    return new ColumnUpdate(s2bb(cf), s2bb(cq)).setValue(s2bb(val));
  }

  private ColumnUpdate newColUpdate(String cf, String cq, long ts, String val) {
    return new ColumnUpdate(s2bb(cf), s2bb(cq)).setTimestamp(ts).setValue(s2bb(val));
  }

  private void assertScan(String[][] expected, String table) throws Exception {
    String scid = client.createScanner(creds, table, new ScanOptions());
    ScanResult keyValues = client.nextK(scid, expected.length + 1);

    assertEquals(expected.length, keyValues.results.size());
    assertFalse(keyValues.more);

    for (int i = 0; i < keyValues.results.size(); i++) {
      checkKey(expected[i][0], expected[i][1], expected[i][2], expected[i][3], keyValues.results.get(i));
    }

    client.closeScanner(scid);
  }

  @Test
  public void testConditionalWriter() throws Exception {
    final String TABLE_TEST = makeTableName();

    client.createTable(creds, TABLE_TEST, true, TimeType.MILLIS);

    client.addConstraint(creds, TABLE_TEST, NumericValueConstraint.class.getName());

    String cwid = client.createConditionalWriter(creds, TABLE_TEST, new ConditionalWriterOptions());

    Map<ByteBuffer,ConditionalUpdates> updates = new HashMap<ByteBuffer,ConditionalUpdates>();

    updates.put(
        s2bb("00345"),
        new ConditionalUpdates(Arrays.asList(newCondition("meta", "seq")), Arrays.asList(newColUpdate("meta", "seq", 10, "1"),
            newColUpdate("data", "img", "73435435"))));

    Map<ByteBuffer,ConditionalStatus> results = client.updateRowsConditionally(cwid, updates);

    assertEquals(1, results.size());
    assertEquals(ConditionalStatus.ACCEPTED, results.get(s2bb("00345")));

    assertScan(new String[][] { {"00345", "data", "img", "73435435"}, {"00345", "meta", "seq", "1"}}, TABLE_TEST);

    // test not setting values on conditions
    updates.clear();

    updates.put(s2bb("00345"), new ConditionalUpdates(Arrays.asList(newCondition("meta", "seq")), Arrays.asList(newColUpdate("meta", "seq", "2"))));
    updates.put(s2bb("00346"), new ConditionalUpdates(Arrays.asList(newCondition("meta", "seq")), Arrays.asList(newColUpdate("meta", "seq", "1"))));

    results = client.updateRowsConditionally(cwid, updates);

    assertEquals(2, results.size());
    assertEquals(ConditionalStatus.REJECTED, results.get(s2bb("00345")));
    assertEquals(ConditionalStatus.ACCEPTED, results.get(s2bb("00346")));

    assertScan(new String[][] { {"00345", "data", "img", "73435435"}, {"00345", "meta", "seq", "1"}, {"00346", "meta", "seq", "1"}}, TABLE_TEST);

    // test setting values on conditions
    updates.clear();

    updates.put(
        s2bb("00345"),
        new ConditionalUpdates(Arrays.asList(newCondition("meta", "seq", "1")), Arrays.asList(newColUpdate("meta", "seq", 20, "2"),
            newColUpdate("data", "img", "567890"))));

    updates.put(s2bb("00346"), new ConditionalUpdates(Arrays.asList(newCondition("meta", "seq", "2")), Arrays.asList(newColUpdate("meta", "seq", "3"))));

    results = client.updateRowsConditionally(cwid, updates);

    assertEquals(2, results.size());
    assertEquals(ConditionalStatus.ACCEPTED, results.get(s2bb("00345")));
    assertEquals(ConditionalStatus.REJECTED, results.get(s2bb("00346")));

    assertScan(new String[][] { {"00345", "data", "img", "567890"}, {"00345", "meta", "seq", "2"}, {"00346", "meta", "seq", "1"}}, TABLE_TEST);

    // test setting timestamp on condition to a non-existant version
    updates.clear();

    updates.put(
        s2bb("00345"),
        new ConditionalUpdates(Arrays.asList(newCondition("meta", "seq", 10, "2")), Arrays.asList(newColUpdate("meta", "seq", 30, "3"),
            newColUpdate("data", "img", "1234567890"))));

    results = client.updateRowsConditionally(cwid, updates);

    assertEquals(1, results.size());
    assertEquals(ConditionalStatus.REJECTED, results.get(s2bb("00345")));

    assertScan(new String[][] { {"00345", "data", "img", "567890"}, {"00345", "meta", "seq", "2"}, {"00346", "meta", "seq", "1"}}, TABLE_TEST);

    // test setting timestamp to an existing version

    updates.clear();

    updates.put(
        s2bb("00345"),
        new ConditionalUpdates(Arrays.asList(newCondition("meta", "seq", 20, "2")), Arrays.asList(newColUpdate("meta", "seq", 30, "3"),
            newColUpdate("data", "img", "1234567890"))));

    results = client.updateRowsConditionally(cwid, updates);

    assertEquals(1, results.size());
    assertEquals(ConditionalStatus.ACCEPTED, results.get(s2bb("00345")));

    assertScan(new String[][] { {"00345", "data", "img", "1234567890"}, {"00345", "meta", "seq", "3"}, {"00346", "meta", "seq", "1"}}, TABLE_TEST);

    // run test w/ condition that has iterators
    // following should fail w/o iterator
    client.updateAndFlush(creds, TABLE_TEST, Collections.singletonMap(s2bb("00347"), Arrays.asList(newColUpdate("data", "count", "1"))));
    client.updateAndFlush(creds, TABLE_TEST, Collections.singletonMap(s2bb("00347"), Arrays.asList(newColUpdate("data", "count", "1"))));
    client.updateAndFlush(creds, TABLE_TEST, Collections.singletonMap(s2bb("00347"), Arrays.asList(newColUpdate("data", "count", "1"))));

    updates.clear();
    updates.put(s2bb("00347"),
        new ConditionalUpdates(Arrays.asList(newCondition("data", "count", "3")), Arrays.asList(newColUpdate("data", "img", "1234567890"))));

    results = client.updateRowsConditionally(cwid, updates);

    assertEquals(1, results.size());
    assertEquals(ConditionalStatus.REJECTED, results.get(s2bb("00347")));

    assertScan(new String[][] { {"00345", "data", "img", "1234567890"}, {"00345", "meta", "seq", "3"}, {"00346", "meta", "seq", "1"},
        {"00347", "data", "count", "1"}}, TABLE_TEST);

    // following test w/ iterator setup should succeed
    Condition iterCond = newCondition("data", "count", "3");
    Map<String,String> props = new HashMap<String,String>();
    props.put("type", "STRING");
    props.put("columns", "data:count");
    IteratorSetting is = new IteratorSetting(1, "sumc", SummingCombiner.class.getName(), props);
    iterCond.setIterators(Arrays.asList(is));

    updates.clear();
    updates.put(s2bb("00347"), new ConditionalUpdates(Arrays.asList(iterCond), Arrays.asList(newColUpdate("data", "img", "1234567890"))));

    results = client.updateRowsConditionally(cwid, updates);

    assertEquals(1, results.size());
    assertEquals(ConditionalStatus.ACCEPTED, results.get(s2bb("00347")));

    assertScan(new String[][] { {"00345", "data", "img", "1234567890"}, {"00345", "meta", "seq", "3"}, {"00346", "meta", "seq", "1"},
        {"00347", "data", "count", "1"}, {"00347", "data", "img", "1234567890"}}, TABLE_TEST);

    // test a mutation that violated a constraint
    updates.clear();
    updates.put(s2bb("00347"),
        new ConditionalUpdates(Arrays.asList(newCondition("data", "img", "1234567890")), Arrays.asList(newColUpdate("data", "count", "A"))));

    results = client.updateRowsConditionally(cwid, updates);

    assertEquals(1, results.size());
    assertEquals(ConditionalStatus.VIOLATED, results.get(s2bb("00347")));

    assertScan(new String[][] { {"00345", "data", "img", "1234567890"}, {"00345", "meta", "seq", "3"}, {"00346", "meta", "seq", "1"},
        {"00347", "data", "count", "1"}, {"00347", "data", "img", "1234567890"}}, TABLE_TEST);

    // run test with two conditions
    // both conditions should fail
    updates.clear();
    updates.put(
        s2bb("00347"),
        new ConditionalUpdates(Arrays.asList(newCondition("data", "img", "565"), newCondition("data", "count", "2")), Arrays.asList(
            newColUpdate("data", "count", "3"), newColUpdate("data", "img", "0987654321"))));

    results = client.updateRowsConditionally(cwid, updates);

    assertEquals(1, results.size());
    assertEquals(ConditionalStatus.REJECTED, results.get(s2bb("00347")));

    assertScan(new String[][] { {"00345", "data", "img", "1234567890"}, {"00345", "meta", "seq", "3"}, {"00346", "meta", "seq", "1"},
        {"00347", "data", "count", "1"}, {"00347", "data", "img", "1234567890"}}, TABLE_TEST);

    // one condition should fail
    updates.clear();
    updates.put(
        s2bb("00347"),
        new ConditionalUpdates(Arrays.asList(newCondition("data", "img", "1234567890"), newCondition("data", "count", "2")), Arrays.asList(
            newColUpdate("data", "count", "3"), newColUpdate("data", "img", "0987654321"))));

    results = client.updateRowsConditionally(cwid, updates);

    assertEquals(1, results.size());
    assertEquals(ConditionalStatus.REJECTED, results.get(s2bb("00347")));

    assertScan(new String[][] { {"00345", "data", "img", "1234567890"}, {"00345", "meta", "seq", "3"}, {"00346", "meta", "seq", "1"},
        {"00347", "data", "count", "1"}, {"00347", "data", "img", "1234567890"}}, TABLE_TEST);

    // one condition should fail
    updates.clear();
    updates.put(
        s2bb("00347"),
        new ConditionalUpdates(Arrays.asList(newCondition("data", "img", "565"), newCondition("data", "count", "1")), Arrays.asList(
            newColUpdate("data", "count", "3"), newColUpdate("data", "img", "0987654321"))));

    results = client.updateRowsConditionally(cwid, updates);

    assertEquals(1, results.size());
    assertEquals(ConditionalStatus.REJECTED, results.get(s2bb("00347")));

    assertScan(new String[][] { {"00345", "data", "img", "1234567890"}, {"00345", "meta", "seq", "3"}, {"00346", "meta", "seq", "1"},
        {"00347", "data", "count", "1"}, {"00347", "data", "img", "1234567890"}}, TABLE_TEST);

    // both conditions should succeed

    ConditionalStatus result = client.updateRowConditionally(
        creds,
        TABLE_TEST,
        s2bb("00347"),
        new ConditionalUpdates(Arrays.asList(newCondition("data", "img", "1234567890"), newCondition("data", "count", "1")), Arrays.asList(
            newColUpdate("data", "count", "3"), newColUpdate("data", "img", "0987654321"))));

    assertEquals(ConditionalStatus.ACCEPTED, result);

    assertScan(new String[][] { {"00345", "data", "img", "1234567890"}, {"00345", "meta", "seq", "3"}, {"00346", "meta", "seq", "1"},
        {"00347", "data", "count", "3"}, {"00347", "data", "img", "0987654321"}}, TABLE_TEST);

    client.closeConditionalWriter(cwid);
    try {
      client.updateRowsConditionally(cwid, updates);
      fail("conditional writer not closed");
    } catch (UnknownWriter uk) {}

    // run test with colvis
    client.createLocalUser(creds, "cwuser", s2bb("bestpasswordever"));
    client.changeUserAuthorizations(creds, "cwuser", Collections.singleton(s2bb("A")));
    client.grantTablePermission(creds, "cwuser", TABLE_TEST, TablePermission.WRITE);
    client.grantTablePermission(creds, "cwuser", TABLE_TEST, TablePermission.READ);

    ByteBuffer cwuCreds = client.login("cwuser", Collections.singletonMap("password", "bestpasswordever"));

    cwid = client.createConditionalWriter(cwuCreds, TABLE_TEST, new ConditionalWriterOptions().setAuthorizations(Collections.singleton(s2bb("A"))));

    updates.clear();
    updates.put(
        s2bb("00348"),
        new ConditionalUpdates(Arrays.asList(new Condition(new Column(s2bb("data"), s2bb("c"), s2bb("A")))), Arrays.asList(newColUpdate("data", "seq", "1"),
            newColUpdate("data", "c", "1").setColVisibility(s2bb("A")))));
    updates.put(s2bb("00349"),
        new ConditionalUpdates(Arrays.asList(new Condition(new Column(s2bb("data"), s2bb("c"), s2bb("B")))), Arrays.asList(newColUpdate("data", "seq", "1"))));

    results = client.updateRowsConditionally(cwid, updates);

    assertEquals(2, results.size());
    assertEquals(ConditionalStatus.ACCEPTED, results.get(s2bb("00348")));
    assertEquals(ConditionalStatus.INVISIBLE_VISIBILITY, results.get(s2bb("00349")));

    assertScan(new String[][] { {"00345", "data", "img", "1234567890"}, {"00345", "meta", "seq", "3"}, {"00346", "meta", "seq", "1"},
        {"00347", "data", "count", "3"}, {"00347", "data", "img", "0987654321"}, {"00348", "data", "seq", "1"}}, TABLE_TEST);

    updates.clear();

    updates.clear();
    updates.put(
        s2bb("00348"),
        new ConditionalUpdates(Arrays.asList(new Condition(new Column(s2bb("data"), s2bb("c"), s2bb("A"))).setValue(s2bb("0"))), Arrays.asList(
            newColUpdate("data", "seq", "2"), newColUpdate("data", "c", "2").setColVisibility(s2bb("A")))));

    results = client.updateRowsConditionally(cwid, updates);

    assertEquals(1, results.size());
    assertEquals(ConditionalStatus.REJECTED, results.get(s2bb("00348")));

    assertScan(new String[][] { {"00345", "data", "img", "1234567890"}, {"00345", "meta", "seq", "3"}, {"00346", "meta", "seq", "1"},
        {"00347", "data", "count", "3"}, {"00347", "data", "img", "0987654321"}, {"00348", "data", "seq", "1"}}, TABLE_TEST);

    updates.clear();
    updates.put(
        s2bb("00348"),
        new ConditionalUpdates(Arrays.asList(new Condition(new Column(s2bb("data"), s2bb("c"), s2bb("A"))).setValue(s2bb("1"))), Arrays.asList(
            newColUpdate("data", "seq", "2"), newColUpdate("data", "c", "2").setColVisibility(s2bb("A")))));

    results = client.updateRowsConditionally(cwid, updates);

    assertEquals(1, results.size());
    assertEquals(ConditionalStatus.ACCEPTED, results.get(s2bb("00348")));

    assertScan(new String[][] { {"00345", "data", "img", "1234567890"}, {"00345", "meta", "seq", "3"}, {"00346", "meta", "seq", "1"},
        {"00347", "data", "count", "3"}, {"00347", "data", "img", "0987654321"}, {"00348", "data", "seq", "2"}}, TABLE_TEST);

    client.closeConditionalWriter(cwid);
    try {
      client.updateRowsConditionally(cwid, updates);
      fail("conditional writer not closed");
    } catch (UnknownWriter uk) {}

    client.dropLocalUser(creds, "cwuser");

  }

  private void checkKey(String row, String cf, String cq, String val, KeyValue keyValue) {
    assertEquals(row, ByteBufferUtil.toString(keyValue.key.row));
    assertEquals(cf, ByteBufferUtil.toString(keyValue.key.colFamily));
    assertEquals(cq, ByteBufferUtil.toString(keyValue.key.colQualifier));
    assertEquals("", ByteBufferUtil.toString(keyValue.key.colVisibility));
    assertEquals(val, ByteBufferUtil.toString(keyValue.value));
  }

  // scan metadata for file entries for the given table
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
    String scanner = client.createScanner(creds, MetadataTable.NAME, opt);
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

  private Map<String,String> s2pp(String cf) {
    Map<String,String> toRet = new TreeMap<String,String>();
    toRet.put("password", cf);
    return toRet;
  }

  static private ByteBuffer t2bb(Text t) {
    return ByteBuffer.wrap(t.getBytes());
  }

  @Test
  public void testGetRowRange() throws Exception {
    Range range = client.getRowRange(s2bb("xyzzy"));
    org.apache.accumulo.core.data.Range range2 = new org.apache.accumulo.core.data.Range(new Text("xyzzy"));
    assertEquals(0, range.start.row.compareTo(t2bb(range2.getStartKey().getRow())));
    assertEquals(0, range.stop.row.compareTo(t2bb(range2.getEndKey().getRow())));
    assertEquals(range.startInclusive, range2.isStartKeyInclusive());
    assertEquals(range.stopInclusive, range2.isEndKeyInclusive());
    assertEquals(0, range.start.colFamily.compareTo(t2bb(range2.getStartKey().getColumnFamily())));
    assertEquals(0, range.start.colQualifier.compareTo(t2bb(range2.getStartKey().getColumnQualifier())));
    assertEquals(0, range.stop.colFamily.compareTo(t2bb(range2.getEndKey().getColumnFamily())));
    assertEquals(0, range.stop.colQualifier.compareTo(t2bb(range2.getEndKey().getColumnQualifier())));
    assertEquals(range.start.timestamp, range.start.timestamp);
    assertEquals(range.stop.timestamp, range.stop.timestamp);
  }
}

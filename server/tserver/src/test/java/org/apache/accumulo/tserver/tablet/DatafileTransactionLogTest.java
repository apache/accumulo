/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.tablet;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.NamespaceConfiguration;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

public class DatafileTransactionLogTest {

  private static TestTable FOO = new TestTable("Foo", TableId.of("1"));
  private static KeyExtent FOO_EXTENT = new KeyExtent(FOO.getId(), new Text("2"), new Text("1"));

  private ServerConfigurationFactory factory;

  @BeforeEach
  public void init() {
    ServerContext context = createMockContext();
    replay(context);
    factory = new TestServerConfigurationFactory(context);
    initFactory(factory);
  }

  private DatafileTransactionLog createLog(Set<StoredTabletFile> initialFiles) {
    return createLog(initialFiles, 0);
  }

  private DatafileTransactionLog createLog(final Set<StoredTabletFile> initialFiles,
      final int maxSize) {
    setMaxSize(maxSize);
    DatafileTransactionLog log = new DatafileTransactionLog(FOO_EXTENT, initialFiles,
        factory.getTableConfiguration(FOO.getId()));
    assertEquals(log.getExpectedFiles(), initialFiles);
    return log;
  }

  private void setMaxSize(int maxSize) {
    ((ConfigurationCopy) (factory.getSystemConfiguration()))
        .set(Property.TABLE_OPERATION_LOG_MAX_SIZE, Integer.toString(maxSize));
  }

  @Test
  public void testFlushed() throws InterruptedException {
    StoredTabletFile initialFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile1.rf");
    Set<StoredTabletFile> initialFiles = Sets.newHashSet(initialFile);
    DatafileTransactionLog log = createLog(initialFiles);
    String dump = log.dumpLog();
    long logDate = log.getInitialDate().getTime();

    Thread.sleep(2);
    log.flushed(Optional.empty());
    assertEquals(log.getExpectedFiles(), initialFiles);
    assertEquals(0, log.getTransactions().size());
    assertTrue(log.getInitialDate().getTime() > logDate);
    logDate = log.getInitialDate().getTime();
    assertNotEquals(dump, log.dumpLog());
    dump = log.dumpLog();

    Thread.sleep(2);
    StoredTabletFile flushedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ffile1.rf");
    log.flushed(Optional.of(flushedFile));
    assertEquals(log.getExpectedFiles(), Sets.newHashSet(initialFile, flushedFile));
    assertEquals(0, log.getTransactions().size());
    assertTrue(log.getInitialDate().getTime() > logDate);
    assertNotEquals(dump, log.dumpLog());
  }

  @Test
  public void testBulkImport() throws InterruptedException {
    StoredTabletFile initialFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile1.rf");
    Set<StoredTabletFile> initialFiles = Sets.newHashSet(initialFile);
    DatafileTransactionLog log = createLog(initialFiles);
    String dump = log.dumpLog();
    long logDate = log.getInitialDate().getTime();

    Thread.sleep(2);
    StoredTabletFile importedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ifile1.rf");
    log.bulkImported(importedFile);
    assertEquals(log.getExpectedFiles(), Sets.newHashSet(initialFile, importedFile));
    assertEquals(0, log.getTransactions().size());
    assertTrue(log.getInitialDate().getTime() > logDate);
    assertNotEquals(dump, log.dumpLog());
  }

  @Test
  public void testCompacted() throws InterruptedException {
    StoredTabletFile initialFile1 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile1.rf");
    StoredTabletFile initialFile2 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile2.rf");
    StoredTabletFile initialFile3 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile3.rf");
    Set<StoredTabletFile> initialFiles = Sets.newHashSet(initialFile1, initialFile2, initialFile3);
    DatafileTransactionLog log = createLog(initialFiles);
    String dump = log.dumpLog();
    long logDate = log.getInitialDate().getTime();

    Thread.sleep(2);
    StoredTabletFile compactedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Cfile1.rf");
    log.compacted(Sets.newHashSet(initialFile1, initialFile2), Optional.of(compactedFile));
    assertEquals(log.getExpectedFiles(), Sets.newHashSet(initialFile3, compactedFile));
    assertEquals(0, log.getTransactions().size());
    assertTrue(log.getInitialDate().getTime() > logDate);
    assertNotEquals(dump, log.dumpLog());
    dump = log.dumpLog();
    logDate = log.getInitialDate().getTime();

    Thread.sleep(2);
    log.compacted(Sets.newHashSet(compactedFile), Optional.empty());
    assertEquals(log.getExpectedFiles(), Sets.newHashSet(initialFile3));
    assertEquals(0, log.getTransactions().size());
    assertTrue(log.getInitialDate().getTime() > logDate);
    assertNotEquals(dump, log.dumpLog());
  }

  @Test
  public void testNonEmptyLog() throws InterruptedException {
    StoredTabletFile initialFile1 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile1.rf");
    StoredTabletFile initialFile2 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile2.rf");
    StoredTabletFile initialFile3 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile3.rf");
    Set<StoredTabletFile> initialFiles = Sets.newHashSet(initialFile1, initialFile2, initialFile3);
    DatafileTransactionLog log = createLog(initialFiles, 3);
    String dump = log.dumpLog();
    long logDate = log.getInitialDate().getTime();

    Thread.sleep(2);
    StoredTabletFile importedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ifile1.rf");
    log.bulkImported(importedFile);
    assertTrue(log.getExpectedFiles()
        .equals(Sets.newHashSet(initialFile1, initialFile2, initialFile3, importedFile)));
    List<DatafileTransaction> logs = log.getTransactions();
    assertEquals(1, logs.size());
    assertEquals(logDate, log.getInitialDate().getTime());
    assertNotEquals(dump, log.dumpLog());
    dump = log.dumpLog();

    Thread.sleep(2);
    StoredTabletFile compactedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Cfile1.rf");
    log.compacted(Sets.newHashSet(initialFile3, importedFile), Optional.of(compactedFile));
    assertEquals(log.getExpectedFiles(),
        Sets.newHashSet(initialFile1, initialFile2, compactedFile));
    assertEquals(logDate, log.getInitialDate().getTime());
    logs = log.getTransactions();
    assertEquals(2, logs.size());
    assertNotEquals(dump, log.dumpLog());
    dump = log.dumpLog();

    Thread.sleep(2);
    log.compacted(Sets.newHashSet(compactedFile), Optional.empty());
    assertEquals(log.getExpectedFiles(), Sets.newHashSet(initialFile1, initialFile2));
    assertEquals(logDate, log.getInitialDate().getTime());
    logs = log.getTransactions();
    assertEquals(3, logs.size());
    assertNotEquals(dump, log.dumpLog());
    dump = log.dumpLog();

    assertTrue(logs.get(0) instanceof DatafileTransaction.BulkImported);
    assertEquals(importedFile, ((DatafileTransaction.BulkImported) logs.get(0)).getImportFile());
    assertTrue(logs.get(1) instanceof DatafileTransaction.Compacted);
    assertEquals(Sets.newHashSet(initialFile3, importedFile),
        ((DatafileTransaction.Compacted) logs.get(1)).getCompactedFiles());
    assertEquals(Optional.of(compactedFile),
        ((DatafileTransaction.Compacted) logs.get(1)).getDestination());
    assertTrue(logs.get(2) instanceof DatafileTransaction.Compacted);
    assertEquals(Sets.newHashSet(compactedFile),
        ((DatafileTransaction.Compacted) logs.get(2)).getCompactedFiles());
    assertEquals(Optional.empty(), ((DatafileTransaction.Compacted) logs.get(2)).getDestination());

    Thread.sleep(2);
    StoredTabletFile flushedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ffile1.rf");
    log.flushed(Optional.of(flushedFile));
    assertEquals(log.getExpectedFiles(), Sets.newHashSet(initialFile1, initialFile2, flushedFile));
    assertEquals(logs.get(0).ts, log.getInitialDate().getTime());
    logs = log.getTransactions();
    assertEquals(3, logs.size());
    assertNotEquals(dump, log.dumpLog());
    dump = log.dumpLog();

    assertTrue(logs.get(0) instanceof DatafileTransaction.Compacted);
    assertEquals(Sets.newHashSet(initialFile3, importedFile),
        ((DatafileTransaction.Compacted) logs.get(0)).getCompactedFiles());
    assertEquals(Optional.of(compactedFile),
        ((DatafileTransaction.Compacted) logs.get(0)).getDestination());
    assertTrue(logs.get(1) instanceof DatafileTransaction.Compacted);
    assertEquals(Sets.newHashSet(compactedFile),
        ((DatafileTransaction.Compacted) logs.get(1)).getCompactedFiles());
    assertEquals(Optional.empty(), ((DatafileTransaction.Compacted) logs.get(1)).getDestination());
    assertTrue(logs.get(2) instanceof DatafileTransaction.Flushed);
    assertEquals(Optional.of(flushedFile),
        ((DatafileTransaction.Flushed) logs.get(2)).getFlushFile());

    Thread.sleep(2);
    setMaxSize(1);
    log.flushed(Optional.empty());
    assertEquals(log.getExpectedFiles(), Sets.newHashSet(initialFile1, initialFile2, flushedFile));
    // we went from 3 to 1 logs, so the first one should not be the last of the original logs
    assertEquals(logs.get(2).ts, log.getInitialDate().getTime());
    logs = log.getTransactions();
    assertEquals(1, logs.size());
    assertNotEquals(dump, log.dumpLog());

    assertTrue(logs.get(0) instanceof DatafileTransaction.Flushed);
    assertEquals(Optional.empty(), ((DatafileTransaction.Flushed) logs.get(0)).getFlushFile());
  }

  protected ServerContext createMockContext() {
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());

    ServerContext mockContext = createMock(ServerContext.class);
    PropStore propStore = createMock(ZooPropStore.class);
    expect(mockContext.getProperties()).andReturn(new Properties()).anyTimes();
    expect(mockContext.getZooKeepers()).andReturn("").anyTimes();
    expect(mockContext.getInstanceName()).andReturn("test").anyTimes();
    expect(mockContext.getZooKeepersSessionTimeOut()).andReturn(30).anyTimes();
    expect(mockContext.getInstanceID()).andReturn(instanceId).anyTimes();
    expect(mockContext.getZooKeeperRoot()).andReturn(Constants.ZROOT + "/1111").anyTimes();

    expect(mockContext.getPropStore()).andReturn(propStore).anyTimes();
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    expect(propStore.get(eq(NamespacePropKey.of(instanceId, NamespaceId.of("+default")))))
        .andReturn(new VersionedProperties()).anyTimes();

    expect(propStore.get(eq(TablePropKey.of(instanceId, TableId.of("1")))))
        .andReturn(new VersionedProperties()).anyTimes();

    replay(propStore);
    return mockContext;
  }

  private void initFactory(ServerConfigurationFactory factory) {
    ServerContext context = createMockContext();
    expect(context.getConfiguration()).andReturn(factory.getSystemConfiguration()).anyTimes();
    expect(context.getTableConfiguration(FOO.getId()))
        .andReturn(factory.getTableConfiguration(FOO.getId())).anyTimes();
    replay(context);
  }

  protected static class TestTable {
    private String tableName;
    private TableId id;

    TestTable(String tableName, TableId id) {
      this.tableName = tableName;
      this.id = id;
    }

    public String getTableName() {
      return tableName;
    }

    public TableId getId() {
      return id;
    }
  }

  protected static final HashMap<String,String> DEFAULT_TABLE_PROPERTIES = new HashMap<>();
  {
    DEFAULT_TABLE_PROPERTIES.put(Property.TABLE_OPERATION_LOG_MAX_SIZE.getKey(), "0");
  }

  private static SiteConfiguration siteConfg = SiteConfiguration.empty().build();

  protected static class TestServerConfigurationFactory extends ServerConfigurationFactory {

    private final ServerContext context;
    protected final ConfigurationCopy config;

    public TestServerConfigurationFactory(ServerContext context) {
      super(context, siteConfg);
      this.context = context;
      this.config = new ConfigurationCopy(DEFAULT_TABLE_PROPERTIES);
    }

    @Override
    public synchronized AccumuloConfiguration getSystemConfiguration() {
      return config;
    }

    @Override
    public TableConfiguration getTableConfiguration(final TableId tableId) {
      // create a dummy namespaceConfiguration to satisfy requireNonNull in TableConfiguration
      // constructor
      NamespaceConfiguration dummyConf = new NamespaceConfiguration(context, Namespace.DEFAULT.id(),
          DefaultConfiguration.getInstance());
      return new TableConfiguration(context, tableId, dummyConf) {
        @Override
        public String get(Property property) {
          return getSystemConfiguration().get(property.getKey());
        }

        @Override
        public void getProperties(Map<String,String> props, Predicate<String> filter) {
          getSystemConfiguration().getProperties(props, filter);
        }

        @Override
        public long getUpdateCount() {
          return getSystemConfiguration().getUpdateCount();
        }
      };
    }
  }

}

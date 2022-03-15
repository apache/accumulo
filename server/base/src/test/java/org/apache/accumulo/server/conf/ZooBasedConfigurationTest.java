/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.conf;

import static org.apache.accumulo.core.conf.Property.GC_PORT;
import static org.apache.accumulo.core.conf.Property.TABLE_BLOOM_ENABLED;
import static org.apache.accumulo.core.conf.Property.TABLE_BLOOM_SIZE;
import static org.apache.accumulo.core.conf.Property.TABLE_DURABILITY;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedPropGzipCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheId;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreException;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooBasedConfigurationTest {

  private final static Logger log = LoggerFactory.getLogger(ZooBasedConfigurationTest.class);

  private InstanceId instanceId;
  private ServerContext context;
  private PropStore propStore;

  @BeforeEach
  public void initMocks() {
    instanceId = InstanceId.of(UUID.randomUUID());
    context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    propStore = createMock(ZooPropStore.class);
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
  }

  @Test
  public void get() throws PropStoreException {
    PropCacheId tableCacheId = PropCacheId.forTable(instanceId, TableId.of("a"));

    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall();

    var siteConfig = SiteConfiguration.auto();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();

    VersionedProperties vProps =
        new VersionedProperties(3, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(propStore.get(eq(tableCacheId))).andReturn(vProps).once();
    expect(propStore.getNodeVersion(eq(tableCacheId))).andReturn(3).once();

    VersionedProperties invalid =
        new VersionedProperties(4, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "1234"));
    expect(propStore.get(eq(tableCacheId))).andReturn(invalid);
    expect(propStore.getNodeVersion(eq(tableCacheId))).andReturn(4).once();

    AccumuloConfiguration parent = mock(AccumuloConfiguration.class);
    // expect(parent.get(isA(Property.class))).andReturn(null);

    replay(context, propStore, parent);

    ZooBasedConfiguration configuration =
        new SystemConfiguration(log, context, tableCacheId, parent);

    assertNotNull(configuration);

    verify(context, parent);
  }

  @Test
  public void getPropertiesTest() throws Exception {

    InstanceId IID = InstanceId.of(UUID.randomUUID());
    PropCacheId tableCacheId = PropCacheId.forTable(IID, TableId.of("t1"));
    VersionedPropCodec codec = VersionedPropGzipCodec.codec(true);

    ServerContext context = createMock(ServerContext.class);
    ZooReaderWriter zrw = createMock(ZooReaderWriter.class);

    // set-up mocks for prop store initialization.
    expect(context.getZooKeepersSessionTimeOut()).andReturn(5_000).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(context.getInstanceID()).andReturn(IID).anyTimes();

    expect(zrw.exists(eq(ZooUtil.getRoot(IID)), anyObject())).andReturn(true).anyTimes();
    expect(zrw.getStatus(eq(ZooUtil.getRoot(IID)), anyObject())).andReturn(new Stat()).anyTimes();

    replay(context, zrw);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    // set-up mock for ZooBasedConfig
    reset(context, zrw);

    AccumuloConfiguration parent = createMock(AccumuloConfiguration.class);

    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    expect(context.getInstanceID()).andReturn(IID).anyTimes();

    VersionedProperties vProps =
        new VersionedProperties(Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(zrw.getStatus(eq(tableCacheId.getPath()))).andReturn(new Stat()).once();
    expect(zrw.getData(eq(tableCacheId.getPath()), anyObject(), anyObject()))
        .andReturn(codec.toBytes(vProps)).once();

    var siteConfig = SiteConfiguration.auto();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();

    replay(context, parent, zrw);

    AccumuloConfiguration defaultConfig = new ConfigurationCopy(DefaultConfiguration.getInstance());

    ZooBasedConfiguration zbc = new SystemConfiguration(log, context, tableCacheId, defaultConfig);
    Map<String,String> readProps = zbc.getSnapshot();

    assertNotNull(zbc.getSnapshot());
    assertEquals("true", readProps.get(TABLE_BLOOM_ENABLED.getKey()));

    assertEquals("true", zbc.get(TABLE_BLOOM_ENABLED));

    log.info("ZBC: {} = {}", zbc, readProps);

    // fixed - default
    assertEquals(GC_PORT.getDefaultValue(), zbc.get(GC_PORT));

    // TODO force invalid format path
    // assertEquals("false", zbc.get(TABLE_BLOOM_ENABLED));

    verify(context, parent, zrw);
  }

  @Test
  public void systemPropTest() {

    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall();

    PropCacheId sysId = PropCacheId.forSystem(instanceId);
    VersionedProperties vProps =
        new VersionedProperties(99, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(propStore.get(eq(sysId))).andReturn(vProps).once();
    expect(propStore.getNodeVersion(eq(sysId))).andReturn(99).once();

    replay(propStore, context);

    AccumuloConfiguration defaultConfig = new ConfigurationCopy(DefaultConfiguration.getInstance());

    ZooBasedConfiguration sysConfig = new ZooBasedConfiguration(log, context, sysId, defaultConfig);
    assertNotNull(sysConfig);
    assertEquals("true", sysConfig.get(TABLE_BLOOM_ENABLED));

    verify(propStore, context);
  }

  @Test
  public void reloadFailTest() {

    PropCacheId sysId = PropCacheId.forSystem(instanceId);
    VersionedProperties vProps =
        new VersionedProperties(99, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));

    expect(propStore.get(eq(sysId))).andReturn(vProps).times(5);
    expect(propStore.getNodeVersion(eq(sysId))).andReturn(101).times(5);

    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall();

    replay(propStore, context);

    AccumuloConfiguration defaultConfig = new ConfigurationCopy(DefaultConfiguration.getInstance());

    assertThrows(IllegalStateException.class,
        () -> new ZooBasedConfiguration(log, context, sysId, defaultConfig));
    verify(propStore, context);
  }

  @Test
  public void eventChangeTest() {

    PropCacheId sysId = PropCacheId.forSystem(instanceId);

    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall();

    expect(propStore.get(eq(sysId))).andReturn(
        new VersionedProperties(99, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true")))
        .once();
    expect(propStore.getNodeVersion(eq(sysId))).andReturn(99).once();

    expect(propStore.get(eq(sysId))).andReturn(
        new VersionedProperties(100, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "false")))
        .once();
    expect(propStore.getNodeVersion(eq(sysId))).andReturn(100).once();

    replay(propStore, context);

    AccumuloConfiguration defaultConfig = new ConfigurationCopy(DefaultConfiguration.getInstance());

    ZooBasedConfiguration sysConfig = new ZooBasedConfiguration(log, context, sysId, defaultConfig);
    assertNotNull(sysConfig);
    assertEquals("true", sysConfig.get(TABLE_BLOOM_ENABLED));

    // change event expected to trigger re-read.
    sysConfig.zkChangeEvent(sysId);
    assertEquals("false", sysConfig.get(TABLE_BLOOM_ENABLED));
    verify(propStore, context);
  }

  @Test
  public void deleteEventTest() {

    PropCacheId sysId = PropCacheId.forSystem(instanceId);

    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall();

    expect(propStore.get(eq(sysId))).andReturn(
        new VersionedProperties(123, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true")))
        .once();
    expect(propStore.getNodeVersion(eq(sysId))).andReturn(123);

    expect(propStore.getNodeVersion(eq(sysId)))
        .andThrow(new PropStoreException("mocked - no node", null)).once();

    replay(propStore, context);

    AccumuloConfiguration defaultConfig = new ConfigurationCopy(DefaultConfiguration.getInstance());

    ZooBasedConfiguration sysConfig = new ZooBasedConfiguration(log, context, sysId, defaultConfig);
    assertNotNull(sysConfig);
    assertEquals("true", sysConfig.get(TABLE_BLOOM_ENABLED));

    // change event expected to trigger re-read.
    sysConfig.deleteEvent(sysId);
    assertThrows(PropStoreException.class, () -> sysConfig.get(TABLE_BLOOM_ENABLED));
    verify(propStore, context);
  }

  /**
   * Walk configuration tree sys -> namespace -> table and test properties are retrieved .
   */
  @Test
  public void tablePropTest() {
    // this test is ignoring listeners
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    PropCacheId sysId = PropCacheId.forSystem(instanceId);
    VersionedProperties sysProps =
        new VersionedProperties(1, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(propStore.get(eq(sysId))).andReturn(sysProps).once();
    expect(propStore.getNodeVersion(eq(sysId))).andReturn(1).once();

    PropCacheId nsId = PropCacheId.forNamespace(instanceId, NamespaceId.of("ns1"));
    VersionedProperties nsProps =
        new VersionedProperties(2, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "false"));
    expect(propStore.get(eq(nsId))).andReturn(nsProps).once();
    expect(propStore.getNodeVersion(eq(nsId))).andReturn(2).once();

    PropCacheId tableId = PropCacheId.forTable(instanceId, TableId.of("ns1.table1"));
    VersionedProperties tableProps =
        new VersionedProperties(3, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(propStore.get(eq(tableId))).andReturn(tableProps).once();
    expect(propStore.getNodeVersion(eq(tableId))).andReturn(3).once();

    replay(propStore, context);

    ConfigurationCopy defaultConfig =
        new ConfigurationCopy(Map.of(TABLE_BLOOM_SIZE.getKey(), TABLE_BLOOM_SIZE.getDefaultValue(),
            TABLE_DURABILITY.getKey(), TABLE_DURABILITY.getDefaultValue()));

    ZooBasedConfiguration sysConfig = new ZooBasedConfiguration(log, context, sysId, defaultConfig);
    ZooBasedConfiguration nsConfig = new ZooBasedConfiguration(log, context, nsId, sysConfig);
    ZooBasedConfiguration tableConfig = new ZooBasedConfiguration(log, context, tableId, nsConfig);

    assertNotNull(tableConfig);
    assertEquals("true", sysConfig.get(TABLE_BLOOM_ENABLED));
    assertEquals("false", nsConfig.get(TABLE_BLOOM_ENABLED));
    assertEquals("true", tableConfig.get(TABLE_BLOOM_ENABLED));

    // get from "default" - root parent config
    assertEquals(TABLE_BLOOM_SIZE.getDefaultValue(), tableConfig.get(TABLE_BLOOM_SIZE));
    assertEquals(TABLE_DURABILITY.getDefaultValue(), tableConfig.get(TABLE_DURABILITY.getKey()));

    // test getProperties
    Map<String,String> props = new HashMap<>();
    Predicate<String> all = x -> true;
    tableConfig.getProperties(props, all);
    assertEquals("true", props.get(TABLE_BLOOM_ENABLED.getKey()));
    assertEquals(TABLE_BLOOM_SIZE.getDefaultValue(), props.get(TABLE_BLOOM_SIZE.getKey()));
    assertEquals(TABLE_DURABILITY.getDefaultValue(), props.get(TABLE_DURABILITY.getKey()));

    // test filtered getProperties
    props = new HashMap<>();
    Predicate<String> blooms = x -> (x.contains("bloom"));
    tableConfig.getProperties(props, blooms);
    assertEquals("true", props.get(TABLE_BLOOM_ENABLED.getKey()));
    // in parent
    assertEquals(TABLE_BLOOM_SIZE.getDefaultValue(), props.get(TABLE_BLOOM_SIZE.getKey()));
    // in parent - but excluded by filter
    assertNull(props.get(TABLE_DURABILITY.getKey()));

    props = new HashMap<>();
    Predicate<String> noBlooms = x -> (!x.contains("bloom"));
    tableConfig.getProperties(props, noBlooms);
    // in table - excluded by filter
    assertNull(props.get(TABLE_BLOOM_ENABLED.getKey()));
    // in parent - excluded by filter
    assertNull(props.get(TABLE_BLOOM_SIZE.getKey()));
    // in parent - allowed by filter
    assertEquals(TABLE_DURABILITY.getDefaultValue(), props.get(TABLE_DURABILITY.getKey()));
    verify(propStore, context);
  }

  /**
   * Walk configuration tree sys -> namespace -> table and test update count is calculated correctly
   * if sysid node is deleted.
   */
  @Test
  public void updateCountTest() {
    // this test is ignoring listeners
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    PropCacheId sysId = PropCacheId.forSystem(instanceId);
    VersionedProperties sysProps = new VersionedProperties(100, Instant.now(), Map.of());
    expect(propStore.get(eq(sysId))).andReturn(sysProps).once();
    expect(propStore.getNodeVersion(eq(sysId))).andReturn(100).once();
    // mock node deleted after event
    expect(propStore.getNodeVersion(eq(sysId)))
        .andThrow(new PropStoreException("mocked - no node", null)).times(3);

    PropCacheId nsId = PropCacheId.forNamespace(instanceId, NamespaceId.of("ns1"));
    VersionedProperties nsProps = new VersionedProperties(20, Instant.now(), Map.of());
    expect(propStore.get(eq(nsId))).andReturn(nsProps).once();
    expect(propStore.getNodeVersion(eq(nsId))).andReturn(20).once();

    PropCacheId tableId = PropCacheId.forTable(instanceId, TableId.of("ns1.table1"));
    VersionedProperties tableProps = new VersionedProperties(3, Instant.now(), Map.of());
    expect(propStore.get(eq(tableId))).andReturn(tableProps).once();
    expect(propStore.getNodeVersion(eq(tableId))).andReturn(3).times(1);

    replay(propStore, context);

    ConfigurationCopy defaultConfig = new ConfigurationCopy(Map.of());

    ZooBasedConfiguration sysConfig = new ZooBasedConfiguration(log, context, sysId, defaultConfig);
    ZooBasedConfiguration nsConfig = new ZooBasedConfiguration(log, context, nsId, sysConfig);
    ZooBasedConfiguration tableConfig = new ZooBasedConfiguration(log, context, tableId, nsConfig);

    assertNotNull(tableConfig);
    assertEquals(0, defaultConfig.getUpdateCount());
    assertEquals(100, sysConfig.getUpdateCount());
    assertEquals(120, nsConfig.getUpdateCount());
    assertEquals(123, tableConfig.getUpdateCount());

    sysConfig.deleteEvent(sysId);

    assertThrows(PropStoreException.class, sysConfig::getUpdateCount);
    assertThrows(PropStoreException.class, nsConfig::getUpdateCount);
    assertThrows(PropStoreException.class, tableConfig::getUpdateCount);
  }

  /**
   * Walk configuration tree sys -> namespace -> table and test update count is calculated correctly
   * if table node is deleted.
   */
  @Test
  public void updateCountTableTest() {
    // this test is ignoring listeners
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    PropCacheId sysId = PropCacheId.forSystem(instanceId);
    VersionedProperties sysProps = new VersionedProperties(100, Instant.now(), Map.of());
    expect(propStore.get(eq(sysId))).andReturn(sysProps).once();
    expect(propStore.getNodeVersion(eq(sysId))).andReturn(100).once();

    PropCacheId nsId = PropCacheId.forNamespace(instanceId, NamespaceId.of("ns1"));
    VersionedProperties nsProps = new VersionedProperties(20, Instant.now(), Map.of());
    expect(propStore.get(eq(nsId))).andReturn(nsProps).once();
    expect(propStore.getNodeVersion(eq(nsId))).andReturn(20).once();

    PropCacheId tableId = PropCacheId.forTable(instanceId, TableId.of("ns1.table1"));
    VersionedProperties tableProps = new VersionedProperties(3, Instant.now(), Map.of());
    expect(propStore.get(eq(tableId))).andReturn(tableProps).once();
    expect(propStore.getNodeVersion(eq(tableId))).andReturn(3).once();

    // after table id config delete.
    expect(propStore.getNodeVersion(eq(tableId)))
        .andThrow(new PropStoreException("test - no node", null)).once();

    replay(propStore, context);

    ConfigurationCopy defaultConfig = new ConfigurationCopy(Map.of());

    ZooBasedConfiguration sysConfig = new ZooBasedConfiguration(log, context, sysId, defaultConfig);
    ZooBasedConfiguration nsConfig = new ZooBasedConfiguration(log, context, nsId, sysConfig);
    ZooBasedConfiguration tableConfig = new ZooBasedConfiguration(log, context, tableId, nsConfig);

    assertNotNull(tableConfig);
    assertEquals(0, defaultConfig.getUpdateCount());
    assertEquals(100, sysConfig.getUpdateCount());
    assertEquals(120, nsConfig.getUpdateCount());
    assertEquals(123, tableConfig.getUpdateCount());

    tableConfig.deleteEvent(tableId);
    assertThrows(PropStoreException.class, tableConfig::getUpdateCount);

    verify(propStore, context);
  }
}

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
package org.apache.accumulo.server.conf;

import static org.apache.accumulo.core.conf.Property.GC_PORT;
import static org.apache.accumulo.core.conf.Property.MANAGER_BULK_RETRIES;
import static org.apache.accumulo.core.conf.Property.TABLE_BLOOM_ENABLED;
import static org.apache.accumulo.core.conf.Property.TABLE_BLOOM_SIZE;
import static org.apache.accumulo.core.conf.Property.TABLE_DURABILITY;
import static org.apache.accumulo.core.conf.Property.TABLE_SPLIT_THRESHOLD;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
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
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.junit.jupiter.api.AfterEach;
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
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
  }

  @AfterEach
  public void verifyMocks() {
    // verify mocks common to each test
    verify(context, propStore);
  }

  @Test
  public void defaultInitializationTest() {
    replay(context, propStore);
    assertNotNull(context.getPropStore());
  }

  @Test
  public void defaultSysConfigTest() {
    var sysKey = SystemPropKey.of(instanceId);

    var siteConfig = SiteConfiguration.empty().build();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();

    expect(propStore.get(eq(sysKey))).andReturn(new VersionedProperties()).once(); // default empty
                                                                                   // sys props
    replay(context, propStore);
    assertNotNull(context.getPropStore());

    ZooBasedConfiguration configuration = new SystemConfiguration(context, sysKey, siteConfig);
    assertNotNull(configuration);

  }

  @Test
  public void get() {
    var sysPropKey = SystemPropKey.of(instanceId);

    var siteConfig = SiteConfiguration.empty().build();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();

    VersionedProperties vProps = new VersionedProperties(3, Instant.now(), Map
        .of(TABLE_BLOOM_ENABLED.getKey(), "true", TABLE_SPLIT_THRESHOLD.getKey(), "int expected"));
    expect(propStore.get(eq(sysPropKey))).andReturn(vProps).once();

    replay(context, propStore);

    ZooBasedConfiguration configuration = new SystemConfiguration(context, sysPropKey, siteConfig);

    assertNotNull(configuration);
    assertEquals("1G", configuration.get(TABLE_SPLIT_THRESHOLD));
    assertEquals("true", configuration.get(TABLE_BLOOM_ENABLED));
  }

  @Test
  public void getPropertiesTest() {
    var tableId = TableId.of("t1");
    var tablePropKey = TablePropKey.of(instanceId, tableId);

    VersionedProperties tProps =
        new VersionedProperties(Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));

    var siteConfig = SiteConfiguration.empty().build();
    // expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();

    expect(propStore.get(eq(tablePropKey))).andReturn(tProps).once();
    NamespaceId nsId = NamespaceId.of("n1");

    VersionedProperties nProps =
        new VersionedProperties(Map.of(TABLE_SPLIT_THRESHOLD.getKey(), "3G"));
    expect(propStore.get(eq(NamespacePropKey.of(instanceId, nsId)))).andReturn(nProps).once();

    replay(context, propStore);

    NamespaceConfiguration nsConfig = new NamespaceConfiguration(context, nsId, siteConfig);

    ZooBasedConfiguration zbc = new TableConfiguration(context, tableId, nsConfig);
    Map<String,String> readProps = zbc.getSnapshot();

    assertNotNull(zbc.getSnapshot());
    assertEquals("true", readProps.get(TABLE_BLOOM_ENABLED.getKey()));
    assertEquals("true", zbc.get(TABLE_BLOOM_ENABLED));

    // read a property that is from the namespace config
    assertEquals("3G", zbc.get(TABLE_SPLIT_THRESHOLD));
    // TABLE_SPLIT_THRESHOLD

    // read a fixed property from the system config
    assertEquals("9998", zbc.get(GC_PORT));

    // read a property from the sysconfig
    assertEquals("3", zbc.get(MANAGER_BULK_RETRIES));
  }

  @Test
  public void systemPropTest() {
    var sysPropKey = SystemPropKey.of(instanceId);
    VersionedProperties vProps =
        new VersionedProperties(99, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(propStore.get(eq(sysPropKey))).andReturn(vProps).once();

    replay(propStore, context);

    AccumuloConfiguration defaultConfig = new ConfigurationCopy(DefaultConfiguration.getInstance());

    ZooBasedConfiguration sysConfig =
        new ZooBasedConfiguration(log, context, sysPropKey, defaultConfig);
    assertNotNull(sysConfig);
    assertEquals("true", sysConfig.get(TABLE_BLOOM_ENABLED));
  }

  @Test
  public void loadFailBecauseNodeNotExistTest() {
    var sysPropKey = SystemPropKey.of(instanceId);

    expect(propStore.get(eq(sysPropKey))).andThrow(new IllegalStateException("fake no node"))
        .once();

    replay(propStore, context);

    var zbc =
        new ZooBasedConfiguration(log, context, sysPropKey, DefaultConfiguration.getInstance());
    assertThrows(IllegalStateException.class, () -> zbc.get("anyproperty"));
  }

  /**
   * Walk configuration tree sys -> namespace -> table and test properties are retrieved .
   */
  @Test
  public void tablePropTest() {
    var sysPropKey = SystemPropKey.of(instanceId);
    VersionedProperties sysProps =
        new VersionedProperties(1, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(propStore.get(eq(sysPropKey))).andReturn(sysProps).once();

    var nsPropKey = NamespacePropKey.of(instanceId, NamespaceId.of("ns1"));
    VersionedProperties nsProps =
        new VersionedProperties(2, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "false"));
    expect(propStore.get(eq(nsPropKey))).andReturn(nsProps).once();

    var tablePropKey = TablePropKey.of(instanceId, TableId.of("ns1.table1"));
    VersionedProperties tableProps =
        new VersionedProperties(3, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(propStore.get(eq(tablePropKey))).andReturn(tableProps).once();

    replay(propStore, context);

    ConfigurationCopy defaultConfig =
        new ConfigurationCopy(Map.of(TABLE_BLOOM_SIZE.getKey(), TABLE_BLOOM_SIZE.getDefaultValue(),
            TABLE_DURABILITY.getKey(), TABLE_DURABILITY.getDefaultValue()));

    ZooBasedConfiguration sysConfig =
        new ZooBasedConfiguration(log, context, sysPropKey, defaultConfig);
    ZooBasedConfiguration nsConfig = new ZooBasedConfiguration(log, context, nsPropKey, sysConfig);
    ZooBasedConfiguration tableConfig =
        new ZooBasedConfiguration(log, context, tablePropKey, nsConfig);

    assertNotNull(tableConfig);
    assertEquals("true", sysConfig.get(TABLE_BLOOM_ENABLED));
    assertEquals("false", nsConfig.get(TABLE_BLOOM_ENABLED));
    assertEquals("true", tableConfig.get(TABLE_BLOOM_ENABLED));

    // get from "default" - root parent config
    assertEquals(TABLE_BLOOM_SIZE.getDefaultValue(), tableConfig.get(TABLE_BLOOM_SIZE));
    assertEquals(TABLE_DURABILITY.getDefaultValue(), tableConfig.get(TABLE_DURABILITY));

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
  }

  /**
   * Walk configuration tree sys -> namespace -> table and test update count is calculated correctly
   * if sysid node is deleted.
   */
  @Test
  public void updateCountTest() {
    var sysPropKey = SystemPropKey.of(instanceId);
    VersionedProperties sysProps = new VersionedProperties(100, Instant.now(), Map.of());
    expect(propStore.get(eq(sysPropKey))).andReturn(sysProps).once();
    expect(propStore.get(eq(sysPropKey))).andThrow(new IllegalStateException("fake no node"))
        .anyTimes();
    // mock node deleted after event

    var nsPropKey = NamespacePropKey.of(instanceId, NamespaceId.of("ns1"));
    VersionedProperties nsProps = new VersionedProperties(20, Instant.now(), Map.of());
    expect(propStore.get(eq(nsPropKey))).andReturn(nsProps).once();

    var tablePropKey = TablePropKey.of(instanceId, TableId.of("ns1.table1"));
    VersionedProperties tableProps = new VersionedProperties(3, Instant.now(), Map.of());
    expect(propStore.get(eq(tablePropKey))).andReturn(tableProps).once();

    replay(propStore, context);

    ConfigurationCopy defaultConfig = new ConfigurationCopy(Map.of());

    ZooBasedConfiguration sysConfig =
        new ZooBasedConfiguration(log, context, sysPropKey, defaultConfig);
    ZooBasedConfiguration nsConfig = new ZooBasedConfiguration(log, context, nsPropKey, sysConfig);
    ZooBasedConfiguration tableConfig =
        new ZooBasedConfiguration(log, context, tablePropKey, nsConfig);

    assertNotNull(tableConfig);
    assertEquals(0, defaultConfig.getUpdateCount());
    assertEquals(100, sysConfig.getUpdateCount());
    assertEquals(120, nsConfig.getUpdateCount());
    assertEquals(123, tableConfig.getUpdateCount());

  }

  /**
   * Walk configuration tree sys -> namespace -> table and test update count is calculated correctly
   * if table node is deleted.
   */
  @Test
  public void updateCountTableTest() {
    var sysPropKey = SystemPropKey.of(instanceId);
    VersionedProperties sysProps = new VersionedProperties(100, Instant.now(), Map.of());
    expect(propStore.get(eq(sysPropKey))).andReturn(sysProps).once();

    var nsPropKey = NamespacePropKey.of(instanceId, NamespaceId.of("ns1"));
    VersionedProperties nsProps = new VersionedProperties(20, Instant.now(), Map.of());
    expect(propStore.get(eq(nsPropKey))).andReturn(nsProps).once();

    var tablePropKey = TablePropKey.of(instanceId, TableId.of("ns1.table1"));
    VersionedProperties tableProps = new VersionedProperties(3, Instant.now(), Map.of());
    expect(propStore.get(eq(tablePropKey))).andReturn(tableProps).once();

    replay(propStore, context);

    ConfigurationCopy defaultConfig = new ConfigurationCopy(Map.of());

    ZooBasedConfiguration sysConfig =
        new ZooBasedConfiguration(log, context, sysPropKey, defaultConfig);
    ZooBasedConfiguration nsConfig = new ZooBasedConfiguration(log, context, nsPropKey, sysConfig);
    ZooBasedConfiguration tableConfig =
        new ZooBasedConfiguration(log, context, tablePropKey, nsConfig);

    assertNotNull(tableConfig);
    assertEquals(0, defaultConfig.getUpdateCount());
    assertEquals(100, sysConfig.getUpdateCount());
    assertEquals(120, nsConfig.getUpdateCount());
    assertEquals(123, tableConfig.getUpdateCount());
  }
}

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

import static org.apache.accumulo.core.conf.Property.INSTANCE_SECRET;
import static org.apache.accumulo.core.conf.Property.TABLE_BLOOM_ENABLED;
import static org.apache.accumulo.core.conf.Property.TABLE_BLOOM_SIZE;
import static org.apache.accumulo.core.conf.Property.TABLE_DURABILITY;
import static org.apache.accumulo.core.conf.Property.TABLE_FILE_MAX;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableConfigurationTest {

  private static final TableId TID = TableId.of("3");
  private static final NamespaceId NID = NamespaceId.of("2");

  private InstanceId instanceId;

  private PropStore propStore;

  private TableConfiguration tableConfig;
  private NamespaceConfiguration nsConfig;

  @BeforeEach
  public void initMocks() {
    instanceId = InstanceId.of(UUID.randomUUID());
    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    propStore = createMock(ZooPropStore.class);
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    var siteConfig = SiteConfiguration.empty().build();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();

    replay(context); // prop store is read from context.

    // this test is ignoring listeners
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    var sysPropKey = SystemPropKey.of(instanceId);
    VersionedProperties sysProps =
        new VersionedProperties(1, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(propStore.get(eq(sysPropKey))).andReturn(sysProps).times(2);

    var nsPropKey = NamespacePropKey.of(instanceId, NID);
    VersionedProperties nsProps = new VersionedProperties(2, Instant.now(),
        Map.of(TABLE_FILE_MAX.getKey(), "21", TABLE_BLOOM_ENABLED.getKey(), "false"));
    expect(propStore.get(eq(nsPropKey))).andReturn(nsProps).once();

    var tablePropKey = TablePropKey.of(instanceId, TID);
    VersionedProperties tableProps =
        new VersionedProperties(3, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(propStore.get(eq(tablePropKey))).andReturn(tableProps).once();

    ConfigurationCopy defaultConfig =
        new ConfigurationCopy(Map.of(TABLE_BLOOM_SIZE.getKey(), TABLE_BLOOM_SIZE.getDefaultValue(),
            TABLE_DURABILITY.getKey(), TABLE_DURABILITY.getDefaultValue()));

    replay(propStore);

    SystemConfiguration sysConfig = new SystemConfiguration(context, sysPropKey, defaultConfig);
    NamespaceId nsid = nsPropKey.getId();
    if (nsid == null) {
      throw new IllegalStateException("missing test namespaceId");
    }
    nsConfig = new NamespaceConfiguration(context, nsid, sysConfig);

    TableId tid = tablePropKey.getId();
    if (tid == null) {
      throw new IllegalStateException("missing test tableId");
    }
    tableConfig = new TableConfiguration(context, tid, nsConfig);

  }

  @Test
  public void testGetters() {
    assertEquals(TID, tableConfig.getTableId());
    assertEquals(nsConfig, tableConfig.getParent());
  }

  @Test
  public void testGet_InZK() {
    Property p = Property.INSTANCE_SECRET;
    reset(propStore);

    var propKey = TablePropKey.of(instanceId, TID);
    expect(propStore.get(eq(propKey)))
        .andReturn(new VersionedProperties(37, Instant.now(), Map.of(p.getKey(), "sekrit")))
        .anyTimes();
    replay(propStore);

    tableConfig.zkChangeEvent(propKey);

    assertEquals("sekrit", tableConfig.get(p));

    verify(propStore);
  }

  @Test
  public void testGet_InParent() {

    String expectedPass = "aPassword1";

    reset(propStore);
    expect(propStore.get(eq(NamespacePropKey.of(instanceId, NID))))
        .andReturn(new VersionedProperties(13, Instant.now(), Map.of(TABLE_FILE_MAX.getKey(), "123",
            Property.INSTANCE_SECRET.getKey(), expectedPass)))
        .anyTimes();
    expect(propStore.get(eq(TablePropKey.of(instanceId, TID))))
        .andReturn(new VersionedProperties(Map.of())).anyTimes();
    replay(propStore);

    nsConfig.zkChangeEvent(NamespacePropKey.of(instanceId, NID));

    assertEquals("123", tableConfig.get(TABLE_FILE_MAX)); // from ns
    assertEquals("aPassword1", tableConfig.get(INSTANCE_SECRET)); // from sys

    verify(propStore);
  }

  @Test
  public void testGetProperties() {
    Predicate<String> all = x -> true;

    reset(propStore);

    expect(propStore.get(eq(SystemPropKey.of(instanceId))))
        .andReturn(new VersionedProperties(1, Instant.now(), Map.of()));

    expect(propStore.get(eq(NamespacePropKey.of(instanceId, NID))))
        .andReturn(new VersionedProperties(2, Instant.now(), Map.of("dog", "bark", "cat", "meow")));

    expect(propStore.get(eq(TablePropKey.of(instanceId, TID))))
        .andReturn(new VersionedProperties(4, Instant.now(), Map.of("foo", "bar", "tick", "tock")))
        .anyTimes();

    replay(propStore);

    Map<String,String> props = new java.util.HashMap<>();

    tableConfig.zkChangeEvent(TablePropKey.of(instanceId, TID));
    nsConfig.zkChangeEvent(NamespacePropKey.of(instanceId, NID));

    tableConfig.getProperties(props, all);

    log.info("Props returned: {}", props);
    assertEquals(7, props.size());
    assertEquals(TABLE_BLOOM_SIZE.getDefaultValue(), props.get(TABLE_BLOOM_SIZE.getKey())); // system
    assertEquals(TABLE_DURABILITY.getDefaultValue(), props.get(TABLE_DURABILITY.getKey())); // system
    assertEquals("true", props.get(TABLE_BLOOM_ENABLED.getKey())); // system
    assertEquals("bar", props.get("foo")); // table
    assertEquals("tock", props.get("tick")); // table
    assertEquals("bark", props.get("dog")); // namespace
    assertEquals("meow", props.get("cat")); // namespace
  }

  @Test
  public void testGetFilteredProperties() {
    Predicate<String> filter = x -> !x.equals("filter");

    reset(propStore);

    expect(propStore.get(eq(SystemPropKey.of(instanceId))))
        .andReturn(new VersionedProperties(1, Instant.now(), Map.of()));

    expect(propStore.get(eq(NamespacePropKey.of(instanceId, NID))))
        .andReturn(new VersionedProperties(2, Instant.now(),
            Map.of("dog", "bark", "cat", "meow", "filter", "from_parent")));

    expect(propStore.get(eq(TablePropKey.of(instanceId, TID)))).andReturn(new VersionedProperties(4,
        Instant.now(), Map.of("filter", "not_returned_by_table", "foo", "bar", "tick", "tock")))
        .anyTimes();

    replay(propStore);

    tableConfig.zkChangeEvent(TablePropKey.of(instanceId, TID));
    nsConfig.zkChangeEvent(NamespacePropKey.of(instanceId, NID));

    Map<String,String> props = new java.util.HashMap<>();

    tableConfig.getProperties(props, filter);

    log.info("Props returned: {}", props);
    assertEquals(7, props.size());
    assertEquals(TABLE_BLOOM_SIZE.getDefaultValue(), props.get(TABLE_BLOOM_SIZE.getKey())); // system
    assertEquals(TABLE_DURABILITY.getDefaultValue(), props.get(TABLE_DURABILITY.getKey())); // system
    assertEquals("true", props.get(TABLE_BLOOM_ENABLED.getKey())); // system
    assertEquals("bar", props.get("foo"));
    assertEquals("tock", props.get("tick"));
    assertEquals("bark", props.get("dog"));
    assertEquals("meow", props.get("cat"));
    assertNull(props.get("filter"));
  }

  @Test
  public void testInvalidateCache() {
    // need to do a get so the accessor is created
    Property p = Property.INSTANCE_SECRET;

    reset(propStore);

    var propKey = TablePropKey.of(instanceId, TID);

    expect(propStore.get(eq(propKey)))
        .andReturn(new VersionedProperties(23, Instant.now(), Map.of(p.getKey(), "invalid")))
        .once();
    expect(propStore.get(eq(propKey)))
        .andReturn(new VersionedProperties(39, Instant.now(), Map.of(p.getKey(), "sekrit"))).once();

    replay(propStore);

    tableConfig.zkChangeEvent(propKey);
    assertEquals("invalid", tableConfig.get(Property.INSTANCE_SECRET));

    tableConfig.invalidateCache();

    assertEquals("sekrit", tableConfig.get(Property.INSTANCE_SECRET));

    verify(propStore);
  }

  private final static Logger log = LoggerFactory.getLogger(TableConfigurationTest.class);

  @Test
  public void getParsedIteratorConfig() {
    var iterConfig = tableConfig.getParsedIteratorConfig(IteratorUtil.IteratorScope.scan);
    assertNotNull(iterConfig);
  }
}

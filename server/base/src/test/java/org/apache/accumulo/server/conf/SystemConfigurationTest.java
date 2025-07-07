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
import static org.apache.accumulo.core.conf.Property.TABLE_BLOOM_ENABLED;
import static org.apache.accumulo.core.conf.Property.TABLE_BLOOM_SIZE;
import static org.apache.accumulo.core.conf.Property.TABLE_DURABILITY;
import static org.apache.accumulo.core.conf.Property.TSERV_CLIENTPORT;
import static org.apache.accumulo.core.conf.Property.TSERV_SCAN_MAX_OPENFILES;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SystemConfigurationTest {

  private InstanceId instanceId;

  private ServerContext context;
  private PropStore propStore;

  private SystemConfiguration sysConfig;

  @BeforeEach
  public void initMocks() {
    instanceId = InstanceId.of(UUID.randomUUID());
    context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    propStore = createMock(ZooPropStore.class);
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    var siteConfig = SiteConfiguration.empty().build();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();

    replay(context); // prop store is read from context.

    // this test is ignoring listeners
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    var sysPropKey = SystemPropKey.of();
    VersionedProperties sysProps =
        new VersionedProperties(1, Instant.now(), Map.of(GC_PORT.getKey(), "1234",
            TSERV_SCAN_MAX_OPENFILES.getKey(), "19", TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(propStore.get(eq(sysPropKey))).andReturn(sysProps).once();
    replay(propStore);

    Map<String,String> props = new HashMap<>();
    DefaultConfiguration.getInstance().getProperties(props, p -> true);
    props.put(TABLE_BLOOM_SIZE.getKey(), TABLE_BLOOM_SIZE.getDefaultValue());
    props.put(TABLE_DURABILITY.getKey(), TABLE_DURABILITY.getDefaultValue());
    ConfigurationCopy defaultConfig = new ConfigurationCopy(props);

    sysConfig = new SystemConfiguration(context, sysPropKey, defaultConfig);
  }

  @AfterEach
  public void verifyMocks() {
    verify(context, propStore);
  }

  @Test
  public void testFromDefault() {
    assertEquals("true", sysConfig.get(TABLE_BLOOM_ENABLED));
    assertTrue(sysConfig.isPropertySet(TABLE_BLOOM_ENABLED));
  }

  @Test
  public void testFromFixed() {
    var sysPropKey = SystemPropKey.of();

    assertEquals("9997", sysConfig.get(TSERV_CLIENTPORT)); // default
    assertEquals("1234", sysConfig.get(GC_PORT)); // fixed sys config
    assertEquals("19", sysConfig.get(TSERV_SCAN_MAX_OPENFILES)); // fixed sys config
    assertEquals("true", sysConfig.get(TABLE_BLOOM_ENABLED)); // sys config
    assertEquals(TABLE_BLOOM_SIZE.getDefaultValue(), sysConfig.get(TABLE_BLOOM_SIZE)); // default

    assertTrue(sysConfig.isPropertySet(TSERV_CLIENTPORT)); // default
    assertTrue(sysConfig.isPropertySet(GC_PORT)); // fixed sys config
    assertTrue(sysConfig.isPropertySet(TSERV_SCAN_MAX_OPENFILES)); // fixed sys config
    assertTrue(sysConfig.isPropertySet(TABLE_BLOOM_ENABLED)); // sys config
    assertTrue(sysConfig.isPropertySet(TABLE_BLOOM_SIZE)); // default

    reset(propStore);

    VersionedProperties sysUpdateProps = new VersionedProperties(2, Instant.now(),
        Map.of(GC_PORT.getKey(), "3456", TSERV_SCAN_MAX_OPENFILES.getKey(), "27",
            TABLE_BLOOM_ENABLED.getKey(), "false", TABLE_BLOOM_SIZE.getKey(), "2048"));
    expect(propStore.get(eq(sysPropKey))).andReturn(sysUpdateProps).anyTimes();
    propStore.invalidate(sysPropKey);
    expectLastCall().atLeastOnce();
    replay(propStore);

    sysConfig.zkChangeEvent(sysPropKey);

    assertEquals("9997", sysConfig.get(TSERV_CLIENTPORT)); // default
    assertEquals("3456", sysConfig.get(GC_PORT)); // fixed sys config
    assertEquals("27", sysConfig.get(TSERV_SCAN_MAX_OPENFILES)); // fixed sys config
    assertEquals("false", sysConfig.get(TABLE_BLOOM_ENABLED)); // sys config
    assertEquals("2048", sysConfig.get(TABLE_BLOOM_SIZE)); // default

    assertTrue(sysConfig.isPropertySet(TSERV_CLIENTPORT)); // default
    assertTrue(sysConfig.isPropertySet(GC_PORT)); // fixed sys config
    assertTrue(sysConfig.isPropertySet(TSERV_SCAN_MAX_OPENFILES)); // fixed sys config
    assertTrue(sysConfig.isPropertySet(TABLE_BLOOM_ENABLED)); // sys config
    assertTrue(sysConfig.isPropertySet(TABLE_BLOOM_SIZE)); // default
  }

  @Test
  public void testChangeMonitor() {

    long version = 1;
    Map<String,String> initialProps = new HashMap<>();
    DefaultConfiguration.getInstance().getProperties(initialProps, p -> true);
    SystemConfiguration.ChangedPropertyMonitor monitor = sysConfig.new ChangedPropertyMonitor(
        SystemPropKey.of(), version, initialProps, ServerId.Type.TABLET_SERVER);
    assertEquals(1, monitor.getVersion());
    assertTrue(monitor.getChangedProperties().isEmpty());
    monitor.update(2, Map.of());
    assertEquals(1, monitor.getVersion());
    assertTrue(monitor.getChangedProperties().isEmpty());
    monitor.update(2, null);
    assertEquals(1, monitor.getVersion());
    assertTrue(monitor.getChangedProperties().isEmpty());

    // Test property change
    initialProps.put(Property.TSERV_SESSION_MAXIDLE.getKey(), "15000");
    monitor.update(2, initialProps);
    assertEquals(2, monitor.getVersion());
    assertTrue(monitor.getChangedProperties().contains(Property.TSERV_SESSION_MAXIDLE.getKey()));
    monitor.propertyRead(Property.TSERV_ASSIGNMENT_MAXCONCURRENT);
    assertEquals(2, monitor.getVersion());
    assertTrue(
        monitor.getChangedProperties().equals(Set.of(Property.TSERV_SESSION_MAXIDLE.getKey())));
    monitor.propertyRead(Property.TSERV_SESSION_MAXIDLE);
    assertEquals(2, monitor.getVersion());
    assertTrue(monitor.getChangedProperties().equals(Set.of()));

    // Test property removal
    initialProps.remove(Property.TSERV_SESSION_MAXIDLE.getKey());
    monitor.update(3, initialProps);
    assertEquals(3, monitor.getVersion());
    assertTrue(monitor.getChangedProperties().contains(Property.TSERV_SESSION_MAXIDLE.getKey()));
    monitor.propertyRead(Property.TSERV_ASSIGNMENT_MAXCONCURRENT);
    assertEquals(3, monitor.getVersion());
    assertTrue(
        monitor.getChangedProperties().equals(Set.of(Property.TSERV_SESSION_MAXIDLE.getKey())));
    monitor.propertyRead(Property.TSERV_SESSION_MAXIDLE);
    assertEquals(3, monitor.getVersion());
    assertTrue(monitor.getChangedProperties().equals(Set.of()));

    // Test property addition
    initialProps.put(Property.TSERV_SESSION_MAXIDLE.getKey(), "10000");
    monitor.update(4, initialProps);
    assertEquals(4, monitor.getVersion());
    assertTrue(monitor.getChangedProperties().contains(Property.TSERV_SESSION_MAXIDLE.getKey()));
    monitor.propertyRead(Property.TSERV_ASSIGNMENT_MAXCONCURRENT);
    assertEquals(4, monitor.getVersion());
    assertTrue(
        monitor.getChangedProperties().equals(Set.of(Property.TSERV_SESSION_MAXIDLE.getKey())));
    monitor.propertyRead(Property.TSERV_SESSION_MAXIDLE);
    assertEquals(4, monitor.getVersion());
    assertTrue(monitor.getChangedProperties().equals(Set.of()));

  }
}

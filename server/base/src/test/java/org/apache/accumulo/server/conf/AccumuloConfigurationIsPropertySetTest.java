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

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.accumulo.core.conf.Property.GC_PORT;
import static org.apache.accumulo.core.conf.Property.INSTANCE_SECRET;
import static org.apache.accumulo.core.conf.Property.INSTANCE_ZK_HOST;
import static org.apache.accumulo.core.conf.Property.MANAGER_BULK_TIMEOUT;
import static org.apache.accumulo.core.conf.Property.TABLE_BLOOM_ENABLED;
import static org.apache.accumulo.core.conf.Property.TABLE_BLOOM_SIZE;
import static org.apache.accumulo.core.conf.Property.TABLE_DURABILITY;
import static org.apache.accumulo.core.conf.Property.TABLE_FILE_MAX;
import static org.apache.accumulo.core.conf.Property.TSERV_SCAN_MAX_OPENFILES;
import static org.apache.accumulo.server.MockServerContext.getMockContextWithPropStore;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.WithTestNames;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * Ensure that each implementation of AccumuloConfiguration has a working implementation of
 * isPropertySet()
 */
public class AccumuloConfigurationIsPropertySetTest extends WithTestNames {

  private static final Set<Property> ALL_PROPERTIES = Set.of(Property.values());
  private static final Logger log =
      LoggerFactory.getLogger(AccumuloConfigurationIsPropertySetTest.class);
  private static final InstanceId instanceId = InstanceId.of(UUID.randomUUID());

  private final SystemPropKey sysPropKey = SystemPropKey.of(instanceId);
  private final ArrayList<Object> mocks = new ArrayList<>();

  private ZooPropStore propStore;
  private ServerContext context;

  @BeforeEach
  public void setupMocks() {
    propStore = createMock(ZooPropStore.class);
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    context = getMockContextWithPropStore(instanceId, null, propStore);
    SiteConfiguration siteConfig = SiteConfiguration.empty().build();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();
  }

  private void readyMocks(Object... mocksToReplay) {
    mocks.addAll(Arrays.asList(mocksToReplay));
    replay(mocksToReplay);
  }

  @AfterEach
  public void verifyMocks() {
    verify(mocks.toArray());
  }

  private static void verifyIsSet(AccumuloConfiguration conf, Set<Property> expectIsSet,
      Set<Property> expectNotSet, Predicate<Property> isSetFunction) {
    var notSetButShouldBe = expectIsSet.stream().filter(isSetFunction.negate()).collect(toSet());
    var setButShouldNotBe = expectNotSet.stream().filter(isSetFunction).collect(toSet());
    assertTrue(notSetButShouldBe.isEmpty(),
        "Properties that should be set but are not: " + notSetButShouldBe);
    assertTrue(setButShouldNotBe.isEmpty(),
        "Properties that should not be set but are: " + setButShouldNotBe);
  }

  private static Predicate<Property> inGetProperties(AccumuloConfiguration conf) {
    Map<String,String> propsMap = new HashMap<>();
    conf.getProperties(propsMap, x -> true);
    return property -> propsMap.containsKey(property.getKey());
  }

  private static Map<String,String> setToMap(Set<Property> props) {
    return props.stream().collect(toMap(Property::getKey, Property::getDefaultValue));
  }

  @Test
  public void testConfigurationCopy() {
    var shouldBeSet = Set.of(TABLE_BLOOM_SIZE, GC_PORT);
    var shouldNotBeSet = Sets.difference(ALL_PROPERTIES, shouldBeSet);
    assertFalse(shouldNotBeSet.isEmpty());

    var conf = new ConfigurationCopy(setToMap(shouldBeSet));

    verifyIsSet(conf, shouldBeSet, shouldNotBeSet, inGetProperties(conf));

    // verify using isPropertySet
    verifyIsSet(conf, shouldBeSet, shouldNotBeSet, conf::isPropertySet);
  }

  @Test
  public void testDefaultConfiguration() {
    // isPropertySet should always be false since users can't set anything on DefaultConfiguration
    var shouldBeSet = Set.<Property>of();
    var shouldNotBeSet = new HashSet<>(ALL_PROPERTIES);

    var conf = DefaultConfiguration.getInstance();

    // verify using isPropertySet
    verifyIsSet(conf, shouldBeSet, shouldNotBeSet, conf::isPropertySet);
  }

  @Test
  public void testNamespaceConfiguration() {
    var namespaceId = NamespaceId.of("namespace");
    var nsPropKey = NamespacePropKey.of(instanceId, namespaceId);

    var setOnParent = Set.of(TABLE_BLOOM_SIZE);
    var parent = new ConfigurationCopy(setToMap(setOnParent));

    var setOnNamespace = Set.of(INSTANCE_SECRET);
    var nsProps = new VersionedProperties(123, Instant.now(), setToMap(setOnNamespace));
    expect(propStore.get(eq(nsPropKey))).andReturn(nsProps).once();

    readyMocks(context, propStore);

    var namespaceConfig = new NamespaceConfiguration(context, namespaceId, parent);

    var shouldBeSet = new HashSet<Property>();
    shouldBeSet.addAll(setOnParent);
    shouldBeSet.addAll(setOnNamespace);
    assertFalse(shouldBeSet.isEmpty());
    var shouldNotBeSet = Sets.difference(ALL_PROPERTIES, shouldBeSet);
    assertFalse(shouldNotBeSet.isEmpty());

    verifyIsSet(namespaceConfig, shouldBeSet, shouldNotBeSet, inGetProperties(namespaceConfig));

    // verify using isPropertySet
    verifyIsSet(namespaceConfig, shouldBeSet, shouldNotBeSet, namespaceConfig::isPropertySet);
  }

  @Test
  public void testSiteConfiguration() throws IOException {
    var shouldBeSet = Set.of(INSTANCE_ZK_HOST, INSTANCE_SECRET, MANAGER_BULK_TIMEOUT);
    var shouldNotBeSet = Sets.difference(ALL_PROPERTIES, shouldBeSet);
    assertFalse(shouldNotBeSet.isEmpty());

    var conf = SiteConfiguration.empty().withOverrides(setToMap(shouldBeSet)).build();

    // verify properties are in the object without using defaults
    Map<String,String> propsMap = new HashMap<>();
    conf.getProperties(propsMap, x -> true, false);
    Predicate<Property> mapContainsProp = property -> propsMap.containsKey(property.getKey());
    verifyIsSet(conf, shouldBeSet, shouldNotBeSet, mapContainsProp);

    // verify using isPropertySet
    verifyIsSet(conf, shouldBeSet, shouldNotBeSet, conf::isPropertySet);
  }

  @Test
  public void testSystemConfiguration() {
    var setOnSystem = Set.of(GC_PORT, TSERV_SCAN_MAX_OPENFILES);
    var sysProps = new VersionedProperties(1, Instant.now(), setToMap(setOnSystem));
    expect(propStore.get(eq(sysPropKey))).andReturn(sysProps).once();

    readyMocks(context, propStore);

    var setOnParent = Set.of(TABLE_BLOOM_SIZE);
    var parent = new ConfigurationCopy(setToMap(setOnParent));

    var shouldBeSet = new HashSet<Property>();
    shouldBeSet.addAll(setOnSystem);
    shouldBeSet.addAll(setOnParent);
    assertFalse(shouldBeSet.isEmpty());
    var shouldNotBeSet = Sets.difference(ALL_PROPERTIES, shouldBeSet);
    assertFalse(shouldNotBeSet.isEmpty());

    var conf = new SystemConfiguration(context, sysPropKey, parent);

    verifyIsSet(conf, shouldBeSet, shouldNotBeSet, inGetProperties(conf));
    // these get added from the constructor via RuntimeFixedProperties and get checked in the
    // isPropertySet impl; adding these to the expected list is a workaround until
    // https://github.com/apache/accumulo/issues/3529 is fixed
    shouldBeSet.addAll(Property.fixedProperties);

    // verify using isPropertySet
    verifyIsSet(conf, shouldBeSet, shouldNotBeSet, conf::isPropertySet);
  }

  @Test
  public void testTableConfiguration() {
    var namespaceId = NamespaceId.of("namespace");
    var nsPropKey = NamespacePropKey.of(instanceId, namespaceId);

    var tableId = TableId.of("3");
    var tablePropKey = TablePropKey.of(instanceId, tableId);

    var setOnNamespace = Set.of(TABLE_FILE_MAX);
    var nsProps = new VersionedProperties(2, Instant.now(), setToMap(setOnNamespace));
    expect(propStore.get(eq(nsPropKey))).andReturn(nsProps).once();

    var setOnTable = Set.of(TABLE_BLOOM_ENABLED);
    var tableProps = new VersionedProperties(3, Instant.now(), setToMap(setOnTable));
    expect(propStore.get(eq(tablePropKey))).andReturn(tableProps).once();

    readyMocks(context, propStore);

    var setOnSystem = Set.of(TABLE_BLOOM_SIZE, TABLE_DURABILITY);
    var sysConfig = new ConfigurationCopy(setToMap(setOnSystem));

    var namespaceConfig = new NamespaceConfiguration(context, namespaceId, sysConfig);
    var tableConfig = new TableConfiguration(context, tableId, namespaceConfig);

    var shouldBeSet = new HashSet<Property>();
    shouldBeSet.addAll(setOnSystem);
    shouldBeSet.addAll(setOnNamespace);
    shouldBeSet.addAll(setOnTable);
    assertFalse(shouldBeSet.isEmpty());
    var shouldNotBeSet = Sets.difference(ALL_PROPERTIES, shouldBeSet);
    assertFalse(shouldNotBeSet.isEmpty());

    verifyIsSet(tableConfig, shouldBeSet, shouldNotBeSet, inGetProperties(tableConfig));

    // verify using isPropertySet
    verifyIsSet(tableConfig, shouldBeSet, shouldNotBeSet, tableConfig::isPropertySet);
  }

  @Test
  public void testZooBasedConfiguration() {
    var setOnSystem = Set.of(GC_PORT);
    var sysProps = new VersionedProperties(1, Instant.now(), setToMap(setOnSystem));
    expect(propStore.get(eq(sysPropKey))).andReturn(sysProps).once();

    readyMocks(context, propStore);

    var setOnParent = Set.of(TABLE_BLOOM_SIZE);
    var parent = new ConfigurationCopy(setToMap(setOnParent));

    var shouldBeSet = new HashSet<Property>();
    shouldBeSet.addAll(setOnSystem);
    shouldBeSet.addAll(setOnParent);
    assertFalse(shouldBeSet.isEmpty());
    var shouldNotBeSet = Sets.difference(ALL_PROPERTIES, shouldBeSet);
    assertFalse(shouldNotBeSet.isEmpty());

    var conf = new ZooBasedConfiguration(log, context, sysPropKey, parent);

    verifyIsSet(conf, shouldBeSet, shouldNotBeSet, inGetProperties(conf));

    // verify using isPropertySet
    verifyIsSet(conf, shouldBeSet, shouldNotBeSet, conf::isPropertySet);
  }

}

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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.conf.Property.GC_PORT;
import static org.apache.accumulo.core.conf.Property.INSTANCE_SECRET;
import static org.apache.accumulo.core.conf.Property.INSTANCE_ZK_HOST;
import static org.apache.accumulo.core.conf.Property.MANAGER_BULK_TSERVER_REGEX;
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
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.WithTestNames;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Ensure that each implementation of AccumuloConfiguration has a working implementation of
 * isPropertySet()
 */
public class AccumuloConfigurationIsPropertySetTest extends WithTestNames {

  private static final Set<Property> ALL_PROPERTIES =
      Arrays.stream(Property.values()).collect(Collectors.toSet());
  private static final Logger log =
      LoggerFactory.getLogger(AccumuloConfigurationIsPropertySetTest.class);

  @TempDir
  private static File tempDir;

  /**
   * Test that the provided properties are set or not set using
   * {@link AccumuloConfiguration#isPropertySet(Property)}
   *
   * @param accumuloConfiguration impl to test against
   * @param expectIsSet set of props that should be set
   * @param expectNotSet set of props that should not be set
   */
  private static void testPropertyIsSetImpl(AccumuloConfiguration accumuloConfiguration,
      Set<Property> expectIsSet, Set<Property> expectNotSet) {
    for (Property prop : expectIsSet) {
      assertTrue(accumuloConfiguration.isPropertySet(prop), "Expected " + prop + " to be set");
    }
    for (Property prop : expectNotSet) {
      assertFalse(accumuloConfiguration.isPropertySet(prop), "Expected " + prop + " to NOT be set");
    }
  }

  /**
   * Verifies that the expected properties are in the given config object without using
   * isPropertySet
   */
  private static void verifyProps(AccumuloConfiguration accumuloConfiguration,
      Set<Property> shouldBeSet, Set<Property> shouldNotBeSet) {
    Map<String,String> propsMap = new HashMap<>();
    accumuloConfiguration.getProperties(propsMap, x -> true);
    Predicate<Property> mapContainsProp = property -> propsMap.containsKey(property.getKey());

    // Collect properties that should be set but are not
    Set<Property> notSetButShouldBe =
        shouldBeSet.stream().filter(mapContainsProp.negate()).collect(Collectors.toSet());

    // Collect properties that should not be set but are
    Set<Property> setButShouldNotBe =
        shouldNotBeSet.stream().filter(mapContainsProp).collect(Collectors.toSet());

    // Assert and print offending sets
    assertTrue(notSetButShouldBe.isEmpty(),
        "Properties that should be set but are not: " + notSetButShouldBe);
    assertTrue(setButShouldNotBe.isEmpty(),
        "Properties that should not be set but are: " + setButShouldNotBe);
  }

  @Test
  public void configurationCopy() {
    Set<Property> shouldBeSet = Set.of(TABLE_BLOOM_SIZE, GC_PORT);
    Set<Property> shouldNotBeSet = Sets.difference(ALL_PROPERTIES, shouldBeSet);

    assertFalse(shouldNotBeSet.isEmpty());

    // set up object
    ConfigurationCopy configurationCopy = new ConfigurationCopy();
    shouldBeSet.forEach(property -> configurationCopy.set(property, "foo"));

    verifyProps(configurationCopy, shouldBeSet, shouldNotBeSet);

    testPropertyIsSetImpl(configurationCopy, shouldBeSet, shouldNotBeSet);
  }

  @Test
  public void defaultConfiguration() {
    Set<Property> shouldBeSet = Collections.emptySet();
    Set<Property> shouldNotBeSet = new HashSet<>(ALL_PROPERTIES);

    DefaultConfiguration defaultConfiguration = DefaultConfiguration.getInstance();

    // we don't expect isPropertySet to be true for any prop since props cannot be set by the user
    // for DefaultConfiguration
    testPropertyIsSetImpl(defaultConfiguration, shouldBeSet, shouldNotBeSet);
  }

  @Test
  public void namespaceConfiguration() {
    InstanceId iid = InstanceId.of(UUID.randomUUID());
    ZooPropStore propStore = createMock(ZooPropStore.class);
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();
    ZooReaderWriter zrw = createMock(ZooReaderWriter.class);
    ServerContext context = getMockContextWithPropStore(iid, zrw, propStore);
    ConfigurationCopy parent = new ConfigurationCopy(
        Map.of(TABLE_BLOOM_SIZE.getKey(), TABLE_BLOOM_SIZE.getDefaultValue()));
    reset(propStore);
    NamespaceId NSID = NamespaceId.of("namespace");
    var nsPropStoreKey = NamespacePropKey.of(iid, NSID);
    expect(propStore.get(eq(nsPropStoreKey))).andReturn(new VersionedProperties(123, Instant.now(),
        Map.of(Property.INSTANCE_SECRET.getKey(), "sekrit"))).anyTimes();
    propStore.registerAsListener(eq(nsPropStoreKey), anyObject());
    expectLastCall().anyTimes();
    replay(propStore, context);

    NamespaceConfiguration namespaceConfiguration =
        new NamespaceConfiguration(context, NSID, parent);

    Set<Property> shouldBeSet = Set.of(TABLE_BLOOM_SIZE, INSTANCE_SECRET);
    Set<Property> shouldNotBeSet = Sets.difference(ALL_PROPERTIES, shouldBeSet);

    verifyProps(namespaceConfiguration, shouldBeSet, shouldNotBeSet);

    testPropertyIsSetImpl(namespaceConfiguration, shouldBeSet, shouldNotBeSet);
  }

  @SuppressFBWarnings(value = {"PATH_TRAVERSAL_IN"}, justification = "path provided by test")
  @Test
  public void siteConfiguration() throws IOException {

    Set<Property> shouldBeSet =
        Set.of(INSTANCE_ZK_HOST, INSTANCE_SECRET, MANAGER_BULK_TSERVER_REGEX);
    Set<Property> shouldNotBeSet = Sets.difference(ALL_PROPERTIES, shouldBeSet);

    // create a properties file contents
    StringBuilder sb = new StringBuilder();
    for (Property p : shouldBeSet) {
      sb.append(p.getKey()).append("=foo").append(System.lineSeparator());
    }
    String propsFileContents = sb.toString();

    // create a new file and write the properties to it
    File propsDir = new File(tempDir, testName());
    assertTrue(propsDir.mkdirs());
    File propsFile = new File(propsDir, "accumulo2.properties");
    assertTrue(propsFile.exists() || propsFile.createNewFile());
    try (FileWriter writer = new FileWriter(propsFile, UTF_8)) {
      log.info("Writing the following properties to {}:\n{}", propsFile, propsFileContents);
      writer.write(propsFileContents);
    }

    // create the object
    SiteConfiguration siteConfiguration = SiteConfiguration.fromFile(propsFile).build();

    verifyProps(siteConfiguration, shouldBeSet, shouldNotBeSet);

    testPropertyIsSetImpl(siteConfiguration, shouldBeSet, shouldNotBeSet);
  }

  @Test
  public void systemConfiguration() {
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());
    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    PropStore propStore = createMock(ZooPropStore.class);
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    SiteConfiguration siteConfig = SiteConfiguration.empty().build();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();
    replay(context);
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();
    SystemPropKey sysPropKey = SystemPropKey.of(instanceId);
    VersionedProperties sysProps = new VersionedProperties(1, Instant.now(),
        Map.of(GC_PORT.getKey(), "1234", TSERV_SCAN_MAX_OPENFILES.getKey(), "19"));
    expect(propStore.get(eq(sysPropKey))).andReturn(sysProps).once();
    replay(propStore);
    ConfigurationCopy defaultConfig = new ConfigurationCopy(
        Map.of(TABLE_BLOOM_SIZE.getKey(), TABLE_BLOOM_SIZE.getDefaultValue()));

    Set<Property> shouldBeSet = Set.of(TABLE_BLOOM_SIZE, GC_PORT, TSERV_SCAN_MAX_OPENFILES);
    Set<Property> shouldNotBeSet = Sets.difference(ALL_PROPERTIES, shouldBeSet);

    // create SystemConfiguration object
    SystemConfiguration systemConfiguration =
        new SystemConfiguration(context, sysPropKey, defaultConfig);

    verifyProps(systemConfiguration, shouldBeSet, shouldNotBeSet);

    testPropertyIsSetImpl(systemConfiguration, shouldBeSet, shouldNotBeSet);

    verify(context, propStore);
  }

  @Test
  public void tableConfiguration() {
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());
    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    PropStore propStore = createMock(PropStore.class);
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    var siteConfig = SiteConfiguration.empty().build();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();
    replay(context); // prop store is read from context.
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();
    NamespaceId NID = NamespaceId.of("namespace");
    var nsPropKey = NamespacePropKey.of(instanceId, NID);
    VersionedProperties nsProps = new VersionedProperties(2, Instant.now(),
        Map.of(TABLE_FILE_MAX.getKey(), "21", TABLE_BLOOM_ENABLED.getKey(), "false"));
    expect(propStore.get(eq(nsPropKey))).andReturn(nsProps).once();
    var TID = TableId.of("3");
    var tablePropKey = TablePropKey.of(instanceId, TID);
    VersionedProperties tableProps =
        new VersionedProperties(3, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(propStore.get(eq(tablePropKey))).andReturn(tableProps).once();
    ConfigurationCopy parentConfig =
        new ConfigurationCopy(Map.of(TABLE_BLOOM_SIZE.getKey(), TABLE_BLOOM_SIZE.getDefaultValue(),
            TABLE_DURABILITY.getKey(), TABLE_DURABILITY.getDefaultValue()));
    replay(propStore);

    NamespaceId nsid = nsPropKey.getId();

    NamespaceConfiguration namespaceConfig =
        new NamespaceConfiguration(context, nsid, parentConfig);

    Set<Property> shouldBeSet =
        Set.of(TABLE_FILE_MAX, TABLE_BLOOM_ENABLED, TABLE_BLOOM_SIZE, TABLE_DURABILITY);
    Set<Property> shouldNotBeSet = Sets.difference(ALL_PROPERTIES, shouldBeSet);

    // Create a TableConfiguration object
    TableConfiguration tableConfiguration = new TableConfiguration(context, TID, namespaceConfig);

    verifyProps(tableConfiguration, shouldBeSet, shouldNotBeSet);

    testPropertyIsSetImpl(tableConfiguration, shouldBeSet, shouldNotBeSet);

    verify(context, propStore);
  }

  @Test
  public void zooBasedConfiguration() {
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());
    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    PropStore propStore = createMock(ZooPropStore.class);
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    var siteConfig = SiteConfiguration.empty().build();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();
    replay(context); // prop store is read from context.
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();
    var sysPropKey = SystemPropKey.of(instanceId);
    VersionedProperties sysProps =
        new VersionedProperties(1, Instant.now(), Map.of(GC_PORT.getKey(), "1234"));
    expect(propStore.get(eq(sysPropKey))).andReturn(sysProps).once();
    replay(propStore);
    ConfigurationCopy defaultConfig = new ConfigurationCopy(
        Map.of(TABLE_BLOOM_SIZE.getKey(), TABLE_BLOOM_SIZE.getDefaultValue()));

    Set<Property> shouldBeSet = Set.of(TABLE_BLOOM_SIZE, GC_PORT);
    Set<Property> shouldNotBeSet = Sets.difference(ALL_PROPERTIES, shouldBeSet);

    assertFalse(shouldNotBeSet.isEmpty());

    // create ZooBasedConfiguration object
    ZooBasedConfiguration zooBasedConfiguration =
        new ZooBasedConfiguration(log, context, sysPropKey, defaultConfig);

    verifyProps(zooBasedConfiguration, shouldBeSet, shouldNotBeSet);

    testPropertyIsSetImpl(zooBasedConfiguration, shouldBeSet, shouldNotBeSet);

    verify(context, propStore);
  }

}

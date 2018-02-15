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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

public class NewTableConfigurationIT extends SharedMiniClusterBase {
  private static final Logger log = LoggerFactory.getLogger(NewTableConfigurationIT.class);

  @Override
  protected int defaultTimeoutSeconds() {
    return 30;
  }

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterClass
  public static void teardown() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  /**
   * Test that setting properties more than once overwrites the previous property settings.
   */
  @Test
  public void testSetPropertiesOverwriteOlderProperties() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];
    NewTableConfiguration ntc = new NewTableConfiguration();
    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1", "val1");
    initialProps.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop2", "val2");
    ntc.setProperties(initialProps);
    // Create a new set of properties and set them with setProperties
    Map<String,String> updatedProps = new HashMap<>();
    updatedProps.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "newerprop1", "newerval1");
    updatedProps.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "newerprop2", "newerval2");
    ntc.setProperties(updatedProps);
    conn.tableOperations().create(tableName, ntc);
    // verify
    Map<String,String> props = ntc.getProperties();
    assertEquals(props.get(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "newerprop1"), "newerval1");
    assertEquals(props.get(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "newerprop2"), "newerval2");
    assertFalse(props.keySet().contains(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1"));
    assertFalse(props.keySet().contains(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop2"));
  }

  /**
   * Verify that you cannot have overlapping locality groups.
   *
   * Attempt to set a locality group with overlapping groups. This test should throw an IllegalArgumentException indicating that groups overlap.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testOverlappingGroupsFail() throws AccumuloSecurityException, AccumuloException, TableExistsException {
    NewTableConfiguration ntc = new NewTableConfiguration();
    Map<String,Set<Text>> lgroups = new HashMap<>();
    lgroups.put("lg1", ImmutableSet.of(new Text("colFamA"), new Text("colFamB")));
    lgroups.put("lg2", ImmutableSet.of(new Text("colFamC"), new Text("colFamB")));
    ntc.setLocalityGroups(lgroups);
  }

  /**
   * Test simplest case of setting locality groups at table creation.
   */
  @Test
  public void testSimpleLocalityGroupCreation() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];
    NewTableConfiguration ntc = new NewTableConfiguration();
    // set locality groups map
    Map<String,Set<Text>> lgroups = new HashMap<>();
    lgroups.put("lg1", ImmutableSet.of(new Text("dog"), new Text("cat")));
    lgroups.put("lg2", ImmutableSet.of(new Text("lion"), new Text("tiger")));
    // set groups via NewTableConfiguration
    ntc.setLocalityGroups(lgroups);
    conn.tableOperations().create(tableName, ntc);
    // verify
    Map<String,Set<Text>> createdLocalityGroups = conn.tableOperations().getLocalityGroups(tableName);
    assertEquals(2, createdLocalityGroups.size());
    assertEquals(createdLocalityGroups.get("lg1"), ImmutableSet.of(new Text("dog"), new Text("cat")));
    assertEquals(createdLocalityGroups.get("lg2"), ImmutableSet.of(new Text("lion"), new Text("tiger")));
  }

  /**
   * Verify that setting locality groups more than once overwrite initial locality settings.
   */
  @Test
  public void testMulitpleCallsToSetLocalityGroups() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];
    NewTableConfiguration ntc = new NewTableConfiguration();
    // set first locality groups map
    Map<String,Set<Text>> initalGroup = new HashMap<>();
    initalGroup.put("lg1", ImmutableSet.of(new Text("dog"), new Text("cat")));
    ntc.setLocalityGroups(initalGroup);
    // set a second locality groups map and set in method call
    Map<String,Set<Text>> secondGroup = new HashMap<>();
    secondGroup.put("lg1", ImmutableSet.of(new Text("blue"), new Text("red")));
    ntc.setLocalityGroups(secondGroup);
    conn.tableOperations().create(tableName, ntc);
    // verify
    Map<String,Set<Text>> createdLocalityGroups = conn.tableOperations().getLocalityGroups(tableName);
    assertEquals(1, createdLocalityGroups.size());
    assertEquals(createdLocalityGroups.get("lg1"), ImmutableSet.of(new Text("red"), new Text("blue")));
  }

  /**
   * Verify that setting locality groups along with other properties works.
   */
  @Test
  public void testSetPropertiesAndGroups() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];
    NewTableConfiguration ntc = new NewTableConfiguration();

    Map<String,String> props = new HashMap<>();
    props.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1", "val1");
    props.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop2", "val2");
    ntc.setProperties(props);

    Map<String,Set<Text>> lgroups = new HashMap<>();
    lgroups.put("lg1", ImmutableSet.of(new Text("dog")));
    ntc.setLocalityGroups(lgroups);
    conn.tableOperations().create(tableName, ntc);
    // verify
    int count = 0;
    for (Entry<String,String> property : conn.tableOperations().getProperties(tableName)) {
      if (property.getKey().equals("table.group.lg1")) {
        assertEquals(property.getValue(), "dog");
        count++;
      }
      if (property.getKey().equals("table.groups.enabled")) {
        assertEquals(property.getValue(), "lg1");
        count++;
      }
      if (property.getKey().equals(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1")) {
        assertEquals(property.getValue(), "val1");
        count++;
      }
      if (property.getKey().equals(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop2")) {
        assertEquals(property.getValue(), "val2");
        count++;
      }
    }
    assertEquals(4, count);
    Map<String,Set<Text>> createdLocalityGroups = conn.tableOperations().getLocalityGroups(tableName);
    assertEquals(1, createdLocalityGroups.size());
    assertEquals(createdLocalityGroups.get("lg1"), ImmutableSet.of(new Text("dog")));
  }

  /**
   * Verify that properties set using NewTableConfiguration must be table properties.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTablePropertiesSet() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
    NewTableConfiguration ntc = new NewTableConfiguration();
    Map<String,String> props = new HashMap<>();

    // These properties should work just with no issue
    props.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1", "val1");
    props.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop2", "val2");
    ntc.setProperties(props);

    // These properties should result in an illegalArgumentException
    props.put("invalidProp1", "value1");
    props.put("invalidProp2", "value2");
    ntc.setProperties(props);
  }

  /**
   * Create table with initial locality groups but no default iterators
   */
  @Test
  public void testSetGroupsWithoutDefaultIterators() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];
    NewTableConfiguration ntc = new NewTableConfiguration().withoutDefaultIterators();

    Map<String,Set<Text>> lgroups = new HashMap<>();
    lgroups.put("lg1", ImmutableSet.of(new Text("colF")));
    ntc.setLocalityGroups(lgroups);
    conn.tableOperations().create(tableName, ntc);
    // verify groups and verify no iterators
    Map<String,Set<Text>> createdLocalityGroups = conn.tableOperations().getLocalityGroups(tableName);
    assertEquals(1, createdLocalityGroups.size());
    assertEquals(createdLocalityGroups.get("lg1"), ImmutableSet.of(new Text("colF")));
    Map<String,EnumSet<IteratorScope>> iterators = conn.tableOperations().listIterators(tableName);
    assertEquals(0, iterators.size());
  }

  /**
   * Test pre-configuring iterator along with default iterator. Configure IteratorSetting values within method call.
   */
  @Test
  public void testPreconfigureIteratorWithDefaultIterator1() throws AccumuloException, TableNotFoundException, AccumuloSecurityException, TableExistsException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];

    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.attachIterator(new IteratorSetting(10, "anIterator", "it.class", Collections.emptyMap()), EnumSet.of(IteratorScope.scan));
    conn.tableOperations().create(tableName, ntc);

    Map<String,EnumSet<IteratorScope>> iteratorList = conn.tableOperations().listIterators(tableName);
    // should count the created iterator plus the default iterator
    assertEquals(2, iteratorList.size());
    verifyIterators(conn, tableName, new String[] {"table.iterator.scan.anIterator=10,it.class"}, true);
    conn.tableOperations().removeIterator(tableName, "anIterator", EnumSet.of(IteratorScope.scan));
    verifyIterators(conn, tableName, new String[] {}, true);
    iteratorList = conn.tableOperations().listIterators(tableName);
    assertEquals(1, iteratorList.size());
  }

  /**
   * Test pre-configuring iterator with default iterator. Configure IteratorSetting values into method call.
   */
  @Test
  public void testPreconfiguredIteratorWithDefaultIterator2() throws AccumuloException, TableNotFoundException, AccumuloSecurityException, TableExistsException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];

    NewTableConfiguration ntc = new NewTableConfiguration();
    IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
    ntc.attachIterator(setting);
    conn.tableOperations().create(tableName, ntc);

    Map<String,EnumSet<IteratorScope>> iteratorList = conn.tableOperations().listIterators(tableName);
    // should count the created iterator plus the default iterator
    assertEquals(2, iteratorList.size());
    verifyIterators(conn, tableName, new String[] {"table.iterator.scan.someName=10,foo.bar"}, true);
    conn.tableOperations().removeIterator(tableName, "someName", EnumSet.allOf((IteratorScope.class)));
    verifyIterators(conn, tableName, new String[] {}, true);
    Map<String,EnumSet<IteratorScope>> iteratorList2 = conn.tableOperations().listIterators(tableName);
    assertEquals(1, iteratorList2.size());
  }

  /**
   * Test pre-configuring iterator with default iterator. Pass in IteratorScope value in method arguments.
   */
  @Test
  public void testPreconfiguredIteratorWithDefaultIterator3() throws AccumuloException, TableNotFoundException, AccumuloSecurityException, TableExistsException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];

    NewTableConfiguration ntc = new NewTableConfiguration();
    IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
    ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));
    conn.tableOperations().create(tableName, ntc);

    verifyIterators(conn, tableName, new String[] {"table.iterator.scan.someName=10,foo.bar"}, true);
    Map<String,EnumSet<IteratorScope>> iteratorList = conn.tableOperations().listIterators(tableName);
    assertEquals(2, iteratorList.size());
    assertEquals(iteratorList.get("someName"), EnumSet.of(IteratorScope.scan));
    conn.tableOperations().removeIterator(tableName, "someName", EnumSet.of(IteratorScope.scan));
    verifyIterators(conn, tableName, new String[] {}, true);
    iteratorList = conn.tableOperations().listIterators(tableName);
    assertEquals(1, iteratorList.size());
  }

  /**
   * Test pre-configuring iterator with additional options.
   */
  @Test
  public void testSettingInitialIteratorWithAdditionalIteratorOptions() throws AccumuloException, TableNotFoundException, AccumuloSecurityException,
      TableExistsException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];

    NewTableConfiguration ntc = new NewTableConfiguration();
    IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
    setting.addOptions(Collections.singletonMap("key", "value"));
    ntc.attachIterator(setting);

    conn.tableOperations().create(tableName, ntc);
    verifyIterators(conn, tableName, new String[] {"table.iterator.scan.someName=10,foo.bar", "table.iterator.scan.someName.opt.key=value"}, true);
    conn.tableOperations().removeIterator(tableName, "someName", EnumSet.of(IteratorScope.scan));
    verifyIterators(conn, tableName, new String[] {}, true);
  }

  /**
   * Set up a pre-configured iterator while disabling the default iterators
   */
  @Test
  public void testSetIteratorWithoutDefaultIterators() throws AccumuloException, TableNotFoundException, AccumuloSecurityException, TableExistsException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];

    NewTableConfiguration ntc = new NewTableConfiguration().withoutDefaultIterators();
    IteratorSetting setting = new IteratorSetting(10, "myIterator", "my.class");
    ntc.attachIterator(setting);
    conn.tableOperations().create(tableName, ntc);

    Map<String,EnumSet<IteratorScope>> iteratorList = conn.tableOperations().listIterators(tableName);
    assertEquals(1, iteratorList.size());
    verifyIterators(conn, tableName, new String[] {"table.iterator.scan.myIterator=10,my.class"}, false);
    conn.tableOperations().removeIterator(tableName, "myIterator", EnumSet.allOf(IteratorScope.class));
    verifyIterators(conn, tableName, new String[] {}, false);
    Map<String,EnumSet<IteratorScope>> iteratorList2 = conn.tableOperations().listIterators(tableName);
    assertEquals(0, iteratorList2.size());
  }

  /**
   * Create iterator and setProperties method together.
   */
  @Test
  public void testSettingIteratorAndProperties() throws AccumuloException, TableNotFoundException, AccumuloSecurityException, TableExistsException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];

    NewTableConfiguration ntc = new NewTableConfiguration();
    IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
    ntc.attachIterator(setting);

    Map<String,String> props = new HashMap<>();
    props.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1", "val1");
    props.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop2", "val2");
    ntc.setProperties(props);

    conn.tableOperations().create(tableName, ntc);

    int count = 0;
    for (Entry<String,String> property : conn.tableOperations().getProperties(tableName)) {
      if (property.getKey().equals(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1")) {
        assertEquals(property.getValue(), "val1");
        count++;
      }
      if (property.getKey().equals(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop2")) {
        assertEquals(property.getValue(), "val2");
        count++;
      }
    }
    assertEquals(2, count);
    verifyIterators(conn, tableName, new String[] {"table.iterator.scan.someName=10,foo.bar"}, true);
    conn.tableOperations().removeIterator(tableName, "someName", EnumSet.of(IteratorScope.scan));
    verifyIterators(conn, tableName, new String[] {}, true);
  }

  /**
   * Verify iterator conflicts are discovered
   */
  @Test(expected = IllegalArgumentException.class)
  public void testIteratorConflictFound1() throws AccumuloException, TableNotFoundException, AccumuloSecurityException, TableExistsException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];

    NewTableConfiguration ntc = new NewTableConfiguration();
    IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
    ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));
    setting = new IteratorSetting(12, "someName", "foo2.bar");
    ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));
    conn.tableOperations().create(tableName, ntc);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIteratorConflictFound2() throws AccumuloException, TableNotFoundException, AccumuloSecurityException, TableExistsException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];

    NewTableConfiguration ntc = new NewTableConfiguration();
    IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
    ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));
    setting = new IteratorSetting(10, "anotherName", "foo2.bar");
    ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));
    conn.tableOperations().create(tableName, ntc);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIteratorConflictFound3() throws AccumuloException, TableNotFoundException, AccumuloSecurityException, TableExistsException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];

    NewTableConfiguration ntc = new NewTableConfiguration();
    IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
    ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));
    setting = new IteratorSetting(12, "someName", "foo.bar");
    ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));
    conn.tableOperations().create(tableName, ntc);
  }

  /**
   * Verify that multiple calls to attachIterator keep adding to iterators, i.e., do not overwrite existing iterators.
   */
  @Test
  public void testMultipleIteratorValid() throws AccumuloException, TableNotFoundException, AccumuloSecurityException, TableExistsException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];

    NewTableConfiguration ntc = new NewTableConfiguration();
    IteratorSetting setting = new IteratorSetting(10, "firstIterator", "first.class");
    ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));
    setting = new IteratorSetting(11, "secondIterator", "second.class");
    ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));

    conn.tableOperations().create(tableName, ntc);
    verifyIterators(conn, tableName, new String[] {"table.iterator.scan.firstIterator=10,first.class", "table.iterator.scan.secondIterator=11,second.class"},
        true);
    conn.tableOperations().removeIterator(tableName, "firstIterator", EnumSet.of(IteratorScope.scan));
    verifyIterators(conn, tableName, new String[] {"table.iterator.scan.secondIterator=11,second.class"}, true);
    conn.tableOperations().removeIterator(tableName, "secondIterator", EnumSet.of(IteratorScope.scan));
    verifyIterators(conn, tableName, new String[] {}, true);
  }

  /**
   * Verify use of all three ntc methods - setProperties, setLocalityGroups and attachIterator
   */
  @Test
  public void testGroupsIteratorAndPropsTogether() throws AccumuloException, TableNotFoundException, AccumuloSecurityException, TableExistsException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];

    NewTableConfiguration ntc = new NewTableConfiguration();
    IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
    ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));
    Map<String,String> props = new HashMap<>();
    props.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1", "val1");
    ntc.setProperties(props);
    Map<String,Set<Text>> lgroups = new HashMap<>();
    lgroups.put("lg1", ImmutableSet.of(new Text("colF")));
    ntc.setLocalityGroups(lgroups);
    conn.tableOperations().create(tableName, ntc);
    // verify user table properties
    int count = 0;
    for (Entry<String,String> property : conn.tableOperations().getProperties(tableName)) {
      if (property.getKey().equals(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1")) {
        assertEquals(property.getValue(), "val1");
        count++;
      }
    }
    assertEquals(1, count);
    // verify locality groups
    Map<String,Set<Text>> createdLocalityGroups = conn.tableOperations().getLocalityGroups(tableName);
    assertEquals(1, createdLocalityGroups.size());
    assertEquals(createdLocalityGroups.get("lg1"), ImmutableSet.of(new Text("colF")));
    // verify iterators
    verifyIterators(conn, tableName, new String[] {"table.iterator.scan.someName=10,foo.bar"}, true);
    conn.tableOperations().removeIterator(tableName, "someName", EnumSet.of(IteratorScope.scan));
    verifyIterators(conn, tableName, new String[] {}, true);
  }

  /**
   * Test NewTableConfiguration chaining.
   */
  @Test
  public void testNtcChaining() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    Connector conn = getConnector();
    String tableName = getUniqueNames(2)[0];

    IteratorSetting setting = new IteratorSetting(10, "anIterator", "it.class", Collections.emptyMap());
    Map<String,Set<Text>> lgroups = new HashMap<>();
    lgroups.put("lgp", ImmutableSet.of(new Text("col")));

    NewTableConfiguration ntc = new NewTableConfiguration().withoutDefaultIterators().attachIterator(setting, EnumSet.of(IteratorScope.scan))
        .setLocalityGroups(lgroups);

    conn.tableOperations().create(tableName, ntc);

    Map<String,EnumSet<IteratorScope>> iteratorList = conn.tableOperations().listIterators(tableName);
    assertEquals(1, iteratorList.size());
    verifyIterators(conn, tableName, new String[] {"table.iterator.scan.anIterator=10,it.class"}, false);
    conn.tableOperations().removeIterator(tableName, "anIterator", EnumSet.of(IteratorScope.scan));
    verifyIterators(conn, tableName, new String[] {}, false);
    iteratorList = conn.tableOperations().listIterators(tableName);
    assertEquals(0, iteratorList.size());

    int count = 0;
    for (Entry<String,String> property : conn.tableOperations().getProperties(tableName)) {
      if (property.getKey().equals("table.group.lgp")) {
        assertEquals(property.getValue(), "col");
        count++;
      }
      if (property.getKey().equals("table.groups.enabled")) {
        assertEquals(property.getValue(), "lgp");
        count++;
      }
    }
    assertEquals(2, count);
    Map<String,Set<Text>> createdLocalityGroups = conn.tableOperations().getLocalityGroups(tableName);
    assertEquals(1, createdLocalityGroups.size());
    assertEquals(createdLocalityGroups.get("lgp"), ImmutableSet.of(new Text("col")));
  }

  /**
   * Verify that disjoint check works as expected with setProperties
   */
  @Test(expected = IllegalArgumentException.class)
  public void testSetPropertiesDisjointCheck() {
    NewTableConfiguration ntc = new NewTableConfiguration();

    Map<String,Set<Text>> lgroups = new HashMap<>();
    lgroups.put("lg1", ImmutableSet.of(new Text("dog")));
    ntc.setLocalityGroups(lgroups);

    Map<String,String> props = new HashMap<>();
    props.put("table.key1", "val1");
    props.put("table.group.lg1", "cat");
    ntc.setProperties(props);
  }

  /**
   * Verify checkDisjoint works with locality groups.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testSetLocalityGroupsDisjointCheck() {
    NewTableConfiguration ntc = new NewTableConfiguration();

    Map<String,String> props = new HashMap<>();
    props.put("table.group.lg1", "cat");
    ntc.setProperties(props);

    Map<String,Set<Text>> lgroups = new HashMap<>();
    lgroups.put("lg1", ImmutableSet.of(new Text("dog")));
    ntc.setLocalityGroups(lgroups);
  }

  /**
   * Verify checkDisjoint works with iterators groups.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testAttachIteratorDisjointCheck() throws AccumuloException {
    NewTableConfiguration ntc = new NewTableConfiguration();

    Map<String,String> props = new HashMap<>();
    props.put("table.iterator.scan.someName", "10");
    ntc.setProperties(props);

    IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
    ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));
  }

  /**
   * Verify the expected iterator properties exist.
   */
  private void verifyIterators(Connector conn, String tablename, String[] values, boolean withDefaultIts) throws AccumuloException, TableNotFoundException {
    Map<String,String> expected = new TreeMap<>();
    if (withDefaultIts) {
      expected.put("table.iterator.scan.vers", "20,org.apache.accumulo.core.iterators.user.VersioningIterator");
      expected.put("table.iterator.scan.vers.opt.maxVersions", "1");
    }
    for (String value : values) {
      String parts[] = value.split("=", 2);
      expected.put(parts[0], parts[1]);
    }

    Map<String,String> actual = new TreeMap<>();
    for (Entry<String,String> entry : this.getProperties(conn, tablename).entrySet()) {
      if (entry.getKey().contains("table.iterator.scan.")) {
        actual.put(entry.getKey(), entry.getValue());
      }
    }
    Assert.assertEquals(expected, actual);
  }

  private Map<String,String> getProperties(Connector connector, String tableName) throws AccumuloException, TableNotFoundException {
    Iterable<Entry<String,String>> properties = connector.tableOperations().getProperties(tableName);
    Map<String,String> propertyMap = new HashMap<>();
    for (Entry<String,String> entry : properties) {
      propertyMap.put(entry.getKey(), entry.getValue());
    }
    return propertyMap;
  }

  public int numProperties(Connector connector, String tableName) throws AccumuloException, TableNotFoundException {
    return Iterators.size(connector.tableOperations().getProperties(tableName).iterator());
  }

  public int compareProperties(Connector connector, String tableNameOrig, String tableName, String changedProp) throws AccumuloException,
      TableNotFoundException {
    boolean inNew = false;
    int countOrig = 0;
    for (Entry<String,String> orig : connector.tableOperations().getProperties(tableNameOrig)) {
      countOrig++;
      for (Entry<String,String> entry : connector.tableOperations().getProperties(tableName)) {
        if (entry.equals(orig)) {
          inNew = true;
          break;
        } else if (entry.getKey().equals(orig.getKey()) && !entry.getKey().equals(changedProp))
          Assert.fail("Property " + orig.getKey() + " has different value than deprecated method");
      }
      if (!inNew)
        Assert.fail("Original property missing after using the new create method");
    }
    return countOrig;
  }

  public boolean checkTimeType(Connector connector, String tableName, TimeType expectedTimeType) throws TableNotFoundException {
    final Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    String tableID = connector.tableOperations().tableIdMap().get(tableName) + "<";
    for (Entry<Key,Value> entry : scanner) {
      Key k = entry.getKey();

      if (k.getRow().toString().equals(tableID) && k.getColumnQualifier().toString().equals(ServerColumnFamily.TIME_COLUMN.getColumnQualifier().toString())) {
        if (expectedTimeType == TimeType.MILLIS && entry.getValue().toString().charAt(0) == 'M')
          return true;
        if (expectedTimeType == TimeType.LOGICAL && entry.getValue().toString().charAt(0) == 'L')
          return true;
      }
    }
    return false;
  }

  @SuppressWarnings("deprecation")
  @Test
  public void tableNameOnly() throws Exception {
    log.info("Starting tableNameOnly");

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, new NewTableConfiguration());

    String tableNameOrig = "original";
    connector.tableOperations().create(tableNameOrig, true);

    int countNew = numProperties(connector, tableName);
    int countOrig = compareProperties(connector, tableNameOrig, tableName, null);

    Assert.assertEquals("Extra properties using the new create method", countOrig, countNew);
    Assert.assertTrue("Wrong TimeType", checkTimeType(connector, tableName, TimeType.MILLIS));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void tableNameAndLimitVersion() throws Exception {
    log.info("Starting tableNameAndLimitVersion");

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    boolean limitVersion = false;
    connector.tableOperations().create(tableName, new NewTableConfiguration().withoutDefaultIterators());

    String tableNameOrig = "originalWithLimitVersion";
    connector.tableOperations().create(tableNameOrig, limitVersion);

    int countNew = numProperties(connector, tableName);
    int countOrig = compareProperties(connector, tableNameOrig, tableName, null);

    Assert.assertEquals("Extra properties using the new create method", countOrig, countNew);
    Assert.assertTrue("Wrong TimeType", checkTimeType(connector, tableName, TimeType.MILLIS));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void tableNameLimitVersionAndTimeType() throws Exception {
    log.info("Starting tableNameLimitVersionAndTimeType");

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    boolean limitVersion = false;
    TimeType tt = TimeType.LOGICAL;
    connector.tableOperations().create(tableName, new NewTableConfiguration().withoutDefaultIterators().setTimeType(tt));

    String tableNameOrig = "originalWithLimitVersionAndTimeType";
    connector.tableOperations().create(tableNameOrig, limitVersion, tt);

    int countNew = numProperties(connector, tableName);
    int countOrig = compareProperties(connector, tableNameOrig, tableName, null);

    Assert.assertEquals("Extra properties using the new create method", countOrig, countNew);
    Assert.assertTrue("Wrong TimeType", checkTimeType(connector, tableName, tt));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void addCustomPropAndChangeExisting() throws Exception {
    log.info("Starting addCustomPropAndChangeExisting");

    // Create and populate initial properties map for creating table 1
    Map<String,String> properties = new HashMap<>();
    String propertyName = Property.TABLE_SPLIT_THRESHOLD.getKey();
    String volume = "10K";
    properties.put(propertyName, volume);

    String propertyName2 = "table.custom.testProp";
    String volume2 = "Test property";
    properties.put(propertyName2, volume2);

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, new NewTableConfiguration().setProperties(properties));

    String tableNameOrig = "originalWithTableName";
    connector.tableOperations().create(tableNameOrig, true);

    int countNew = numProperties(connector, tableName);
    int countOrig = compareProperties(connector, tableNameOrig, tableName, propertyName);

    for (Entry<String,String> entry : connector.tableOperations().getProperties(tableName)) {
      if (entry.getKey().equals(Property.TABLE_SPLIT_THRESHOLD.getKey()))
        Assert.assertTrue("TABLE_SPLIT_THRESHOLD has been changed", entry.getValue().equals("10K"));
      if (entry.getKey().equals("table.custom.testProp"))
        Assert.assertTrue("table.custom.testProp has been changed", entry.getValue().equals("Test property"));
    }

    Assert.assertEquals("Extra properties using the new create method", countOrig + 1, countNew);
    Assert.assertTrue("Wrong TimeType", checkTimeType(connector, tableName, TimeType.MILLIS));

  }
}

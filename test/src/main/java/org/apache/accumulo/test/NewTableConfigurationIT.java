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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class NewTableConfigurationIT extends SharedMiniClusterBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(30);
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  /**
   * Test that setting properties more than once overwrites the previous property settings.
   */
  @Test
  public void testSetPropertiesOverwriteOlderProperties()
      throws AccumuloSecurityException, AccumuloException, TableExistsException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
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
      client.tableOperations().create(tableName, ntc);
      // verify
      Map<String,String> props = ntc.getProperties();
      assertEquals(props.get(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "newerprop1"),
          "newerval1");
      assertEquals(props.get(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "newerprop2"),
          "newerval2");
      assertFalse(props.containsKey(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1"));
      assertFalse(props.containsKey(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop2"));
    }
  }

  /**
   * Test simplest case of setting locality groups at table creation.
   */
  @Test
  public void testSimpleLocalityGroupCreation() throws AccumuloSecurityException, AccumuloException,
      TableExistsException, TableNotFoundException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(2)[0];
      NewTableConfiguration ntc = new NewTableConfiguration();
      // set locality groups map
      Map<String,Set<Text>> lgroups = new HashMap<>();
      lgroups.put("lg1", Set.of(new Text("dog"), new Text("cat")));
      lgroups.put("lg2", Set.of(new Text("lion"), new Text("tiger")));
      // set groups via NewTableConfiguration
      ntc.setLocalityGroups(lgroups);
      client.tableOperations().create(tableName, ntc);
      // verify
      Map<String,Set<Text>> createdLocalityGroups =
          client.tableOperations().getLocalityGroups(tableName);
      assertEquals(2, createdLocalityGroups.size());
      assertEquals(createdLocalityGroups.get("lg1"), Set.of(new Text("dog"), new Text("cat")));
      assertEquals(createdLocalityGroups.get("lg2"), Set.of(new Text("lion"), new Text("tiger")));
    }
  }

  /**
   * Verify that setting locality groups more than once overwrite initial locality settings.
   */
  @Test
  public void testMulitpleCallsToSetLocalityGroups() throws AccumuloSecurityException,
      AccumuloException, TableExistsException, TableNotFoundException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(2)[0];
      NewTableConfiguration ntc = new NewTableConfiguration();
      // set first locality groups map
      Map<String,Set<Text>> initalGroup = new HashMap<>();
      initalGroup.put("lg1", Set.of(new Text("dog"), new Text("cat")));
      ntc.setLocalityGroups(initalGroup);
      // set a second locality groups map and set in method call
      Map<String,Set<Text>> secondGroup = new HashMap<>();
      secondGroup.put("lg1", Set.of(new Text("blue"), new Text("red")));
      ntc.setLocalityGroups(secondGroup);
      client.tableOperations().create(tableName, ntc);
      // verify
      Map<String,Set<Text>> createdLocalityGroups =
          client.tableOperations().getLocalityGroups(tableName);
      assertEquals(1, createdLocalityGroups.size());
      assertEquals(createdLocalityGroups.get("lg1"), Set.of(new Text("red"), new Text("blue")));
    }
  }

  /**
   * Verify that setting locality groups along with other properties works.
   */
  @Test
  public void testSetPropertiesAndGroups() throws AccumuloSecurityException, AccumuloException,
      TableExistsException, TableNotFoundException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(2)[0];
      NewTableConfiguration ntc = new NewTableConfiguration();

      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1", "val1");
      props.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop2", "val2");
      ntc.setProperties(props);

      Map<String,Set<Text>> lgroups = new HashMap<>();
      lgroups.put("lg1", Set.of(new Text("dog")));
      ntc.setLocalityGroups(lgroups);
      client.tableOperations().create(tableName, ntc);
      // verify
      int count = 0;
      for (Entry<String,String> property : client.tableOperations().getProperties(tableName)) {
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
      Map<String,Set<Text>> createdLocalityGroups =
          client.tableOperations().getLocalityGroups(tableName);
      assertEquals(1, createdLocalityGroups.size());
      assertEquals(createdLocalityGroups.get("lg1"), Set.of(new Text("dog")));
    }
  }

  /**
   * Create table with initial locality groups but no default iterators
   */
  @Test
  public void testSetGroupsWithoutDefaultIterators() throws AccumuloSecurityException,
      AccumuloException, TableExistsException, TableNotFoundException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(2)[0];
      NewTableConfiguration ntc = new NewTableConfiguration().withoutDefaultIterators();

      Map<String,Set<Text>> lgroups = new HashMap<>();
      lgroups.put("lg1", Set.of(new Text("colF")));
      ntc.setLocalityGroups(lgroups);
      client.tableOperations().create(tableName, ntc);
      // verify groups and verify no iterators
      Map<String,Set<Text>> createdLocalityGroups =
          client.tableOperations().getLocalityGroups(tableName);
      assertEquals(1, createdLocalityGroups.size());
      assertEquals(createdLocalityGroups.get("lg1"), Set.of(new Text("colF")));
      Map<String,EnumSet<IteratorScope>> iterators =
          client.tableOperations().listIterators(tableName);
      assertEquals(0, iterators.size());
    }
  }

  /**
   * Test pre-configuring iterator along with default iterator. Configure IteratorSetting values
   * within method call.
   */
  @Test
  public void testPreconfigureIteratorWithDefaultIterator1() throws AccumuloException,
      TableNotFoundException, AccumuloSecurityException, TableExistsException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(2)[0];

      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.attachIterator(new IteratorSetting(10, "anIterator", "it.class", Collections.emptyMap()),
          EnumSet.of(IteratorScope.scan));
      client.tableOperations().create(tableName, ntc);

      Map<String,EnumSet<IteratorScope>> iteratorList =
          client.tableOperations().listIterators(tableName);
      // should count the created iterator plus the default iterator
      assertEquals(2, iteratorList.size());
      verifyIterators(client, tableName,
          new String[] {"table.iterator.scan.anIterator=10,it.class"}, true);
      client.tableOperations().removeIterator(tableName, "anIterator",
          EnumSet.of(IteratorScope.scan));
      verifyIterators(client, tableName, new String[] {}, true);
      iteratorList = client.tableOperations().listIterators(tableName);
      assertEquals(1, iteratorList.size());
    }
  }

  /**
   * Test pre-configuring iterator with default iterator. Configure IteratorSetting values into
   * method call.
   */
  @Test
  public void testPreconfiguredIteratorWithDefaultIterator2() throws AccumuloException,
      TableNotFoundException, AccumuloSecurityException, TableExistsException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(2)[0];

      NewTableConfiguration ntc = new NewTableConfiguration();
      IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
      ntc.attachIterator(setting);
      client.tableOperations().create(tableName, ntc);

      Map<String,EnumSet<IteratorScope>> iteratorList =
          client.tableOperations().listIterators(tableName);
      // should count the created iterator plus the default iterator
      assertEquals(2, iteratorList.size());
      verifyIterators(client, tableName, new String[] {"table.iterator.scan.someName=10,foo.bar"},
          true);
      client.tableOperations().removeIterator(tableName, "someName",
          EnumSet.allOf(IteratorScope.class));
      verifyIterators(client, tableName, new String[] {}, true);
      Map<String,EnumSet<IteratorScope>> iteratorList2 =
          client.tableOperations().listIterators(tableName);
      assertEquals(1, iteratorList2.size());
    }
  }

  /**
   * Test pre-configuring iterator with default iterator. Pass in IteratorScope value in method
   * arguments.
   */
  @Test
  public void testPreconfiguredIteratorWithDefaultIterator3() throws AccumuloException,
      TableNotFoundException, AccumuloSecurityException, TableExistsException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(2)[0];

      NewTableConfiguration ntc = new NewTableConfiguration();
      IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
      ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));
      client.tableOperations().create(tableName, ntc);

      verifyIterators(client, tableName, new String[] {"table.iterator.scan.someName=10,foo.bar"},
          true);
      Map<String,EnumSet<IteratorScope>> iteratorList =
          client.tableOperations().listIterators(tableName);
      assertEquals(2, iteratorList.size());
      assertEquals(iteratorList.get("someName"), EnumSet.of(IteratorScope.scan));
      client.tableOperations().removeIterator(tableName, "someName",
          EnumSet.of(IteratorScope.scan));
      verifyIterators(client, tableName, new String[] {}, true);
      iteratorList = client.tableOperations().listIterators(tableName);
      assertEquals(1, iteratorList.size());
    }
  }

  /**
   * Test pre-configuring iterator with additional options.
   */
  @Test
  public void testSettingInitialIteratorWithAdditionalIteratorOptions() throws AccumuloException,
      TableNotFoundException, AccumuloSecurityException, TableExistsException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(2)[0];

      NewTableConfiguration ntc = new NewTableConfiguration();
      IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
      setting.addOptions(Collections.singletonMap("key", "value"));
      ntc.attachIterator(setting);

      client.tableOperations().create(tableName, ntc);
      verifyIterators(client, tableName, new String[] {"table.iterator.scan.someName=10,foo.bar",
          "table.iterator.scan.someName.opt.key=value"}, true);
      client.tableOperations().removeIterator(tableName, "someName",
          EnumSet.of(IteratorScope.scan));
      verifyIterators(client, tableName, new String[] {}, true);
    }
  }

  /**
   * Set up a pre-configured iterator while disabling the default iterators
   */
  @Test
  public void testSetIteratorWithoutDefaultIterators() throws AccumuloException,
      TableNotFoundException, AccumuloSecurityException, TableExistsException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(2)[0];

      NewTableConfiguration ntc = new NewTableConfiguration().withoutDefaultIterators();
      IteratorSetting setting = new IteratorSetting(10, "myIterator", "my.class");
      ntc.attachIterator(setting);
      client.tableOperations().create(tableName, ntc);

      Map<String,EnumSet<IteratorScope>> iteratorList =
          client.tableOperations().listIterators(tableName);
      assertEquals(1, iteratorList.size());
      verifyIterators(client, tableName,
          new String[] {"table.iterator.scan.myIterator=10,my.class"}, false);
      client.tableOperations().removeIterator(tableName, "myIterator",
          EnumSet.allOf(IteratorScope.class));
      verifyIterators(client, tableName, new String[] {}, false);
      Map<String,EnumSet<IteratorScope>> iteratorList2 =
          client.tableOperations().listIterators(tableName);
      assertEquals(0, iteratorList2.size());
    }
  }

  /**
   * Create iterator and setProperties method together.
   */
  @Test
  public void testSettingIteratorAndProperties() throws AccumuloException, TableNotFoundException,
      AccumuloSecurityException, TableExistsException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(2)[0];

      NewTableConfiguration ntc = new NewTableConfiguration();
      IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
      ntc.attachIterator(setting);

      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1", "val1");
      props.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop2", "val2");
      ntc.setProperties(props);

      client.tableOperations().create(tableName, ntc);

      int count = 0;
      for (Entry<String,String> property : client.tableOperations().getProperties(tableName)) {
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
      verifyIterators(client, tableName, new String[] {"table.iterator.scan.someName=10,foo.bar"},
          true);
      client.tableOperations().removeIterator(tableName, "someName",
          EnumSet.of(IteratorScope.scan));
      verifyIterators(client, tableName, new String[] {}, true);
    }
  }

  /**
   * Verify that multiple calls to attachIterator keep adding to iterators, i.e., do not overwrite
   * existing iterators.
   */
  @Test
  public void testMultipleIteratorValid() throws AccumuloException, TableNotFoundException,
      AccumuloSecurityException, TableExistsException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(2)[0];

      NewTableConfiguration ntc = new NewTableConfiguration();
      IteratorSetting setting = new IteratorSetting(10, "firstIterator", "first.class");
      ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));
      setting = new IteratorSetting(11, "secondIterator", "second.class");
      ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));

      client.tableOperations().create(tableName, ntc);
      verifyIterators(client, tableName,
          new String[] {"table.iterator.scan.firstIterator=10,first.class",
              "table.iterator.scan.secondIterator=11,second.class"},
          true);
      client.tableOperations().removeIterator(tableName, "firstIterator",
          EnumSet.of(IteratorScope.scan));
      verifyIterators(client, tableName,
          new String[] {"table.iterator.scan.secondIterator=11,second.class"}, true);
      client.tableOperations().removeIterator(tableName, "secondIterator",
          EnumSet.of(IteratorScope.scan));
      verifyIterators(client, tableName, new String[] {}, true);
    }
  }

  /**
   * Verify use of all three ntc methods - setProperties, setLocalityGroups and attachIterator
   */
  @Test
  public void testGroupsIteratorAndPropsTogether() throws AccumuloException, TableNotFoundException,
      AccumuloSecurityException, TableExistsException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(2)[0];

      NewTableConfiguration ntc = new NewTableConfiguration();
      IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
      ntc.attachIterator(setting, EnumSet.of(IteratorScope.scan));
      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1", "val1");
      ntc.setProperties(props);
      Map<String,Set<Text>> lgroups = new HashMap<>();
      lgroups.put("lg1", Set.of(new Text("colF")));
      ntc.setLocalityGroups(lgroups);
      client.tableOperations().create(tableName, ntc);
      // verify user table properties
      int count = 0;
      for (Entry<String,String> property : client.tableOperations().getProperties(tableName)) {
        if (property.getKey().equals(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop1")) {
          assertEquals(property.getValue(), "val1");
          count++;
        }
      }
      assertEquals(1, count);
      // verify locality groups
      Map<String,Set<Text>> createdLocalityGroups =
          client.tableOperations().getLocalityGroups(tableName);
      assertEquals(1, createdLocalityGroups.size());
      assertEquals(createdLocalityGroups.get("lg1"), Set.of(new Text("colF")));
      // verify iterators
      verifyIterators(client, tableName, new String[] {"table.iterator.scan.someName=10,foo.bar"},
          true);
      client.tableOperations().removeIterator(tableName, "someName",
          EnumSet.of(IteratorScope.scan));
      verifyIterators(client, tableName, new String[] {}, true);
    }
  }

  /**
   * Test NewTableConfiguration chaining.
   */
  @Test
  public void testNtcChaining() throws AccumuloException, AccumuloSecurityException,
      TableExistsException, TableNotFoundException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(2)[0];

      IteratorSetting setting =
          new IteratorSetting(10, "anIterator", "it.class", Collections.emptyMap());
      Map<String,Set<Text>> lgroups = new HashMap<>();
      lgroups.put("lgp", Set.of(new Text("col")));

      NewTableConfiguration ntc = new NewTableConfiguration().withoutDefaultIterators()
          .attachIterator(setting, EnumSet.of(IteratorScope.scan)).setLocalityGroups(lgroups);

      client.tableOperations().create(tableName, ntc);

      Map<String,EnumSet<IteratorScope>> iteratorList =
          client.tableOperations().listIterators(tableName);
      assertEquals(1, iteratorList.size());
      verifyIterators(client, tableName,
          new String[] {"table.iterator.scan.anIterator=10,it.class"}, false);
      client.tableOperations().removeIterator(tableName, "anIterator",
          EnumSet.of(IteratorScope.scan));
      verifyIterators(client, tableName, new String[] {}, false);
      iteratorList = client.tableOperations().listIterators(tableName);
      assertEquals(0, iteratorList.size());

      int count = 0;
      for (Entry<String,String> property : client.tableOperations().getProperties(tableName)) {
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
      Map<String,Set<Text>> createdLocalityGroups =
          client.tableOperations().getLocalityGroups(tableName);
      assertEquals(1, createdLocalityGroups.size());
      assertEquals(createdLocalityGroups.get("lgp"), Set.of(new Text("col")));
    }
  }

  /**
   * Verify the expected iterator properties exist.
   */
  private void verifyIterators(AccumuloClient client, String tablename, String[] values,
      boolean withDefaultIts)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    Map<String,String> expected = new TreeMap<>();
    if (withDefaultIts) {
      expected.put("table.iterator.scan.vers",
          "20,org.apache.accumulo.core.iterators.user.VersioningIterator");
      expected.put("table.iterator.scan.vers.opt.maxVersions", "1");
    }
    for (String value : values) {
      String[] parts = value.split("=", 2);
      expected.put(parts[0], parts[1]);
    }

    Map<String,String> actual = new TreeMap<>();
    for (Entry<String,String> entry : this.getProperties(client, tablename).entrySet()) {
      if (entry.getKey().contains("table.iterator.scan.")) {
        actual.put(entry.getKey(), entry.getValue());
      }
    }
    assertEquals(expected, actual);
  }

  private Map<String,String> getProperties(AccumuloClient accumuloClient, String tableName)
      throws AccumuloException, TableNotFoundException {
    Map<String,String> properties = accumuloClient.tableOperations().getConfiguration(tableName);
    return Map.copyOf(properties);
  }

}

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

import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.harness.WithTestNames;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.functional.NativeMapIT;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.MemKey;
import org.apache.accumulo.tserver.memory.NativeMapLoader;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration Test for https://issues.apache.org/jira/browse/ACCUMULO-4148
 * <p>
 * User had problem writing one Mutation with multiple KV pairs that had the same key. Doing so
 * should write out all pairs in all mutations with a unique id. In typical operation, you would
 * only see the last one when scanning. User had a combiner on the table, and they noticed that when
 * using InMemoryMap with NativeMapWrapper, only the last KV pair was ever written. When InMemoryMap
 * used DefaultMap, all KV pairs were added and the behavior worked as expected.
 *
 * This IT inserts a variety of Mutations with and without the same KV pairs and then inspects
 * result of InMemoryMap mutate, looking for unique id stored with each key. This unique id, shown
 * as mc= in the MemKey toString, was originally used for scan Isolation. Writing the same key
 * multiple times in the same mutation is a secondary use case, discussed in
 * https://issues.apache.org/jira/browse/ACCUMULO-227. In addition to NativeMapWrapper and
 * DefaultMap, LocalityGroupMap was add in https://issues.apache.org/jira/browse/ACCUMULO-112.
 *
 * This test has to be an IT in accumulo-test, because libaccumulo is built in 'integration-test'
 * phase of accumulo-native, which currently runs right before accumulo-test. The tests for
 * DefaultMap could move to a unit test in tserver, but they are here for convenience of viewing
 * both at the same time.
 */
@Tag(SUNNY_DAY)
public class InMemoryMapIT extends WithTestNames {

  private static final Logger log = LoggerFactory.getLogger(InMemoryMapIT.class);

  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void ensureNativeLibrary() {
    File nativeMapLocation = NativeMapIT.nativeMapLocation();
    NativeMapLoader.loadForTest(List.of(nativeMapLocation), () -> fail("Can't load native maps"));
  }

  public static ServerContext getServerContext() {
    ServerContext context = EasyMock.createMock(ServerContext.class);
    EasyMock.replay(context);
    return context;
  }

  @Test
  public void testOneMutationOneKey() {
    Mutation m = new Mutation("a");
    m.put("1cf", "1cq", "vala");

    assertEquivalentMutate(m);
  }

  @Test
  public void testOneMutationManyKeys() {
    Mutation m = new Mutation("a");
    for (int i = 1; i < 6; i++) {
      m.put("2cf" + i, "2cq" + i, Integer.toString(i));
    }

    assertEquivalentMutate(m);
  }

  @Test
  public void testOneMutationManySameKeys() {
    Mutation m = new Mutation("a");
    for (int i = 1; i <= 5; i++) {
      // same keys
      m.put("3cf", "3cq", Integer.toString(i));
    }

    assertEquivalentMutate(m);
  }

  @Test
  public void testMultipleMutationsOneKey() {
    Mutation m1 = new Mutation("a");
    m1.put("4cf", "4cq", "vala");
    Mutation m2 = new Mutation("b");
    m2.put("4cf", "4cq", "vala");

    assertEquivalentMutate(Arrays.asList(m1, m2));
  }

  @Test
  public void testMultipleMutationsSameOneKey() {
    Mutation m1 = new Mutation("a");
    m1.put("5cf", "5cq", "vala");
    Mutation m2 = new Mutation("a");
    m2.put("5cf", "5cq", "vala");

    assertEquivalentMutate(Arrays.asList(m1, m2));
  }

  @Test
  public void testMutlipleMutationsMultipleKeys() {
    Mutation m1 = new Mutation("a");
    for (int i = 1; i < 6; i++) {
      m1.put("6cf" + i, "6cq" + i, Integer.toString(i));
    }
    Mutation m2 = new Mutation("b");
    for (int i = 1; i < 3; i++) {
      m2.put("6cf" + i, "6cq" + i, Integer.toString(i));
    }

    assertEquivalentMutate(Arrays.asList(m1, m2));
  }

  @Test
  public void testMultipleMutationsMultipleSameKeys() {
    Mutation m1 = new Mutation("a");
    for (int i = 1; i < 3; i++) {
      m1.put("7cf", "7cq", Integer.toString(i));
    }
    Mutation m2 = new Mutation("a");
    for (int i = 1; i < 4; i++) {
      m2.put("7cf", "7cq", Integer.toString(i));
    }

    assertEquivalentMutate(Arrays.asList(m1, m2));
  }

  @Test
  public void testMultipleMutationsMultipleKeysSomeSame() {
    Mutation m1 = new Mutation("a");
    for (int i = 1; i < 2; i++) {
      m1.put("8cf", "8cq", Integer.toString(i));
    }
    for (int i = 1; i < 3; i++) {
      m1.put("8cf" + i, "8cq" + i, Integer.toString(i));
    }
    for (int i = 1; i < 2; i++) {
      m1.put("8cf" + i, "8cq" + i, Integer.toString(i));
    }
    Mutation m2 = new Mutation("a");
    for (int i = 1; i < 3; i++) {
      m2.put("8cf", "8cq", Integer.toString(i));
    }
    for (int i = 1; i < 4; i++) {
      m2.put("8cf" + i, "8cq" + i, Integer.toString(i));
    }
    Mutation m3 = new Mutation("b");
    for (int i = 1; i < 3; i++) {
      m3.put("8cf" + i, "8cq" + i, Integer.toString(i));
    }

    assertEquivalentMutate(Arrays.asList(m1, m2, m3));
  }

  private void assertEquivalentMutate(Mutation m) {
    assertEquivalentMutate(Collections.singletonList(m));
  }

  private void assertEquivalentMutate(List<Mutation> mutations) {

    String[] tempFolders = new String[4];
    for (int i = 0; i < tempFolders.length; i++) {
      File dir = new File(tempDir, testName() + "_" + i);
      assertTrue(dir.isDirectory() || dir.mkdir());
      tempFolders[i] = dir.getAbsolutePath();
    }

    Map<String,String> defaultMapConfig = new HashMap<>();
    defaultMapConfig.put(Property.TSERV_NATIVEMAP_ENABLED.getKey(), "false");
    defaultMapConfig.put(Property.TSERV_MEMDUMP_DIR.getKey(), tempFolders[0]);
    defaultMapConfig.put(Property.TABLE_LOCALITY_GROUPS.getKey(), "");

    Map<String,String> nativeMapConfig = new HashMap<>();
    nativeMapConfig.put(Property.TSERV_NATIVEMAP_ENABLED.getKey(), "true");
    nativeMapConfig.put(Property.TSERV_MEMDUMP_DIR.getKey(), tempFolders[1]);
    nativeMapConfig.put(Property.TABLE_LOCALITY_GROUPS.getKey(), "");

    Map<String,String> localityGroupConfig = new HashMap<>();
    localityGroupConfig.put(Property.TSERV_NATIVEMAP_ENABLED.getKey(), "false");
    localityGroupConfig.put(Property.TSERV_MEMDUMP_DIR.getKey(), tempFolders[2]);

    Map<String,String> localityGroupNativeConfig = new HashMap<>();
    localityGroupNativeConfig.put(Property.TSERV_NATIVEMAP_ENABLED.getKey(), "true");
    localityGroupNativeConfig.put(Property.TSERV_MEMDUMP_DIR.getKey(), tempFolders[3]);

    TableId testId = TableId.of("TEST");

    try {
      InMemoryMap defaultMap =
          new InMemoryMap(new ConfigurationCopy(defaultMapConfig), getServerContext(), testId);
      InMemoryMap nativeMapWrapper =
          new InMemoryMap(new ConfigurationCopy(nativeMapConfig), getServerContext(), testId);
      InMemoryMap localityGroupMap = new InMemoryMap(
          updateConfigurationForLocalityGroups(new ConfigurationCopy(localityGroupConfig)),
          getServerContext(), testId);
      InMemoryMap localityGroupMapWithNative = new InMemoryMap(
          updateConfigurationForLocalityGroups(new ConfigurationCopy(localityGroupNativeConfig)),
          getServerContext(), testId);

      // ensure the maps are correct type
      assertEquals(InMemoryMap.TYPE_DEFAULT_MAP, defaultMap.getMapType(), "Not a DefaultMap");
      assertEquals(InMemoryMap.TYPE_NATIVE_MAP_WRAPPER, nativeMapWrapper.getMapType(),
          "Not a NativeMapWrapper");
      assertEquals(InMemoryMap.TYPE_LOCALITY_GROUP_MAP, localityGroupMap.getMapType(),
          "Not a LocalityGroupMap");
      assertEquals(InMemoryMap.TYPE_LOCALITY_GROUP_MAP_NATIVE,
          localityGroupMapWithNative.getMapType(), "Not a LocalityGroupMap with native");

      int count = 0;
      for (Mutation m : mutations) {
        count += m.size();
      }
      defaultMap.mutate(mutations, count);
      nativeMapWrapper.mutate(mutations, count);
      localityGroupMap.mutate(mutations, count);
      localityGroupMapWithNative.mutate(mutations, count);

      // let's use the transitive property to assert all four are equivalent
      assertMutatesEquivalent(mutations, defaultMap, nativeMapWrapper);
      assertMutatesEquivalent(mutations, defaultMap, localityGroupMap);
      assertMutatesEquivalent(mutations, defaultMap, localityGroupMapWithNative);
    } catch (Exception e) {
      log.error("Error getting new InMemoryMap ", e);
      fail(e.getMessage());
    }
  }

  /**
   * Assert that a set of mutations mutate to equivalent map in both of the InMemoryMaps.
   * <p>
   * In this case, equivalent means 2 things.
   * <ul>
   * <li>The size of both maps generated is equal to the number of key value pairs in all mutations
   * passed</li>
   * <li>The size of the map generated from the first InMemoryMap equals the size of the map
   * generated from the second</li>
   * <li>Each key value pair in each mutated map has a unique id (kvCount)</li>
   * </ul>
   *
   * @param mutations List of mutations
   * @param imm1 InMemoryMap to compare
   * @param imm2 InMemoryMap to compare
   */
  private void assertMutatesEquivalent(List<Mutation> mutations, InMemoryMap imm1,
      InMemoryMap imm2) {
    int mutationKVPairs = countKVPairs(mutations);

    List<MemKey> memKeys1 = getArrayOfMemKeys(imm1);
    List<MemKey> memKeys2 = getArrayOfMemKeys(imm2);

    assertEquals(mutationKVPairs, memKeys1.size(),
        "Not all key value pairs included: " + dumpInMemoryMap(imm1, memKeys1));
    assertEquals(memKeys1.size(), memKeys2.size(), "InMemoryMaps differ in size: "
        + dumpInMemoryMap(imm1, memKeys1) + "\n" + dumpInMemoryMap(imm2, memKeys2));
    assertEquals(mutationKVPairs, getUniqKVCount(memKeys1),
        "InMemoryMap did not have distinct kvCounts " + dumpInMemoryMap(imm1, memKeys1));
    assertEquals(mutationKVPairs, getUniqKVCount(memKeys2),
        "InMemoryMap did not have distinct kvCounts " + dumpInMemoryMap(imm2, memKeys2));

  }

  private int countKVPairs(List<Mutation> mutations) {
    int count = 0;
    for (Mutation m : mutations) {
      count += m.size();
    }
    return count;
  }

  private List<MemKey> getArrayOfMemKeys(InMemoryMap imm) {
    SortedKeyValueIterator<Key,Value> skvi = imm.compactionIterator();

    List<MemKey> memKeys = new ArrayList<>();
    try {
      skvi.seek(new Range(), new ArrayList<>(), false); // everything
      while (skvi.hasTop()) {
        memKeys.add((MemKey) skvi.getTopKey());
        skvi.next();
      }
    } catch (IOException ex) {
      log.error("Error getting memkeys", ex);
      throw new UncheckedIOException(ex);
    }

    return memKeys;
  }

  private String dumpInMemoryMap(InMemoryMap map, List<MemKey> memkeys) {
    StringBuilder sb = new StringBuilder();
    sb.append("InMemoryMap type ");
    sb.append(map.getMapType());
    sb.append("\n");

    for (MemKey mk : memkeys) {
      sb.append("  ");
      sb.append(mk);
      sb.append("\n");
    }

    return sb.toString();
  }

  private int getUniqKVCount(List<MemKey> memKeys) {
    List<Integer> kvCounts = new ArrayList<>();
    for (MemKey m : memKeys) {
      kvCounts.add(m.getKVCount());
    }
    return Set.copyOf(kvCounts).size();
  }

  private ConfigurationCopy updateConfigurationForLocalityGroups(ConfigurationCopy configuration) {
    Map<String,Set<ByteSequence>> locGroups = getLocalityGroups();
    StringBuilder enabledLGs = new StringBuilder();

    for (Entry<String,Set<ByteSequence>> entry : locGroups.entrySet()) {
      if (enabledLGs.length() > 0) {
        enabledLGs.append(",");
      }

      StringBuilder value = new StringBuilder();
      for (ByteSequence bytes : entry.getValue()) {
        if (value.length() > 0) {
          value.append(",");
        }
        value.append(new String(bytes.toArray()));
      }
      configuration.set("table.group." + entry.getKey(), value.toString());
      enabledLGs.append(entry.getKey());
    }
    configuration.set(Property.TABLE_LOCALITY_GROUPS, enabledLGs.toString());
    return configuration;
  }

  private Map<String,Set<ByteSequence>> getLocalityGroups() {
    Map<String,Set<ByteSequence>> locgro = new HashMap<>();
    locgro.put("a", newCFSet("cf", "cf2"));
    locgro.put("b", newCFSet("cf3", "cf4"));
    return locgro;
  }

  // from InMemoryMapTest
  private Set<ByteSequence> newCFSet(String... cfs) {
    HashSet<ByteSequence> cfSet = new HashSet<>();
    for (String cf : cfs) {
      cfSet.add(new ArrayByteSequence(cf));
    }
    return cfSet;
  }

}

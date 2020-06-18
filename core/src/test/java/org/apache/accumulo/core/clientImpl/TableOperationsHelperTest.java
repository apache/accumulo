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
package org.apache.accumulo.core.clientImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CloneConfiguration;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.SummaryRetriever;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TableOperationsHelperTest {

  static class Tester extends TableOperationsHelper {
    Map<String,Map<String,String>> settings = new HashMap<>();

    @Override
    public SortedSet<String> list() {
      return null;
    }

    @Override
    public boolean exists(String tableName) {
      return true;
    }

    @Override
    public void create(String tableName) {}

    @Override
    public void create(String tableName, NewTableConfiguration ntc) {}

    @Override
    public void addSplits(String tableName, SortedSet<Text> partitionKeys) {}

    @Override
    public Collection<Text> listSplits(String tableName) {
      return null;
    }

    @Override
    public Collection<Text> listSplits(String tableName, int maxSplits) {
      return null;
    }

    @Override
    public Text getMaxRow(String tableName, Authorizations auths, Text startRow,
        boolean startInclusive, Text endRow, boolean endInclusive) {
      return null;
    }

    @Override
    public void merge(String tableName, Text start, Text end) {

    }

    @Override
    public void deleteRows(String tableName, Text start, Text end) {}

    @Override
    public void compact(String tableName, Text start, Text end, boolean flush, boolean wait) {}

    @Override
    public void compact(String tableName, Text start, Text end, List<IteratorSetting> iterators,
        boolean flush, boolean wait) {}

    @Override
    public void compact(String tableName, CompactionConfig config) {}

    @Override
    public void delete(String tableName) {}

    @Override
    public void clone(String srcTableName, String newTableName, boolean flush,
        Map<String,String> propertiesToSet, Set<String> propertiesToExclude) {}

    @Override
    public void clone(String srcTableName, String newTableName, CloneConfiguration config) {}

    @Override
    public void rename(String oldTableName, String newTableName) {}

    @Override
    public void flush(String tableName) {}

    @Override
    public void flush(String tableName, Text start, Text end, boolean wait) {}

    @Override
    public void setProperty(String tableName, String property, String value) {
      if (!settings.containsKey(tableName))
        settings.put(tableName, new TreeMap<>());
      settings.get(tableName).put(property, value);
    }

    @Override
    public void removeProperty(String tableName, String property) {
      if (!settings.containsKey(tableName))
        return;
      settings.get(tableName).remove(property);
    }

    @Override
    public Iterable<Entry<String,String>> getProperties(String tableName) {
      Map<String,String> empty = Collections.emptyMap();
      if (!settings.containsKey(tableName))
        return empty.entrySet();
      return settings.get(tableName).entrySet();
    }

    @Override
    public void setLocalityGroups(String tableName, Map<String,Set<Text>> groups) {}

    @Override
    public Map<String,Set<Text>> getLocalityGroups(String tableName) {
      return null;
    }

    @Override
    public Set<Range> splitRangeByTablets(String tableName, Range range, int maxSplits) {
      return null;
    }

    @Override
    @Deprecated
    public void importDirectory(String tableName, String dir, String failureDir, boolean setTime) {}

    @Override
    public void offline(String tableName) {

    }

    @Override
    public void online(String tableName) {}

    @Override
    public void offline(String tableName, boolean wait) {

    }

    @Override
    public void online(String tableName, boolean wait) {}

    @Override
    public void clearLocatorCache(String tableName) {}

    @Override
    public Map<String,String> tableIdMap() {
      return null;
    }

    @Override
    public List<DiskUsage> getDiskUsage(Set<String> tables) {
      return null;
    }

    @Override
    public void importTable(String tableName, Set<String> exportDir) {}

    @Override
    public void exportTable(String tableName, String exportDir) {}

    @Override
    public void cancelCompaction(String tableName) {}

    @Override
    public boolean testClassLoad(String tableName, String className, String asTypeName) {
      return false;
    }

    @Override
    public void setSamplerConfiguration(String tableName,
        SamplerConfiguration samplerConfiguration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clearSamplerConfiguration(String tableName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SamplerConfiguration getSamplerConfiguration(String tableName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Locations locate(String tableName, Collection<Range> ranges) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SummaryRetriever summaries(String tableName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addSummarizers(String tableName, SummarizerConfiguration... summarizerConf) {
      throw new UnsupportedOperationException();

    }

    @Override
    public void removeSummarizers(String tableName, Predicate<SummarizerConfiguration> predicate) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<SummarizerConfiguration> listSummarizers(String tableName) {
      throw new UnsupportedOperationException();
    }
  }

  protected TableOperationsHelper getHelper() {
    return new Tester();
  }

  void check(TableOperationsHelper t, String tablename, String[] values) throws Exception {
    Map<String,String> expected = new TreeMap<>();
    for (String value : values) {
      String[] parts = value.split("=", 2);
      expected.put(parts[0], parts[1]);
    }
    Map<String,String> actual = new TreeMap<>();
    for (Entry<String,String> entry : t.getProperties(tablename)) {
      actual.put(entry.getKey(), entry.getValue());
    }
    assertEquals(expected, actual);
  }

  @Test
  public void testAttachIterator() throws Exception {
    TableOperationsHelper t = getHelper();
    Map<String,String> empty = Collections.emptyMap();
    t.attachIterator("table", new IteratorSetting(10, "someName", "foo.bar", empty),
        EnumSet.of(IteratorScope.scan));
    check(t, "table", new String[] {"table.iterator.scan.someName=10,foo.bar",});
    t.removeIterator("table", "someName", EnumSet.of(IteratorScope.scan));
    check(t, "table", new String[] {});

    IteratorSetting setting = new IteratorSetting(10, "someName", "foo.bar");
    setting.addOptions(Collections.singletonMap("key", "value"));
    t.attachIterator("table", setting, EnumSet.of(IteratorScope.majc));
    setting = new IteratorSetting(10, "someName", "foo.bar");
    t.attachIterator("table", setting, EnumSet.of(IteratorScope.scan));
    check(t, "table", new String[] {"table.iterator.majc.someName=10,foo.bar",
        "table.iterator.majc.someName.opt.key=value", "table.iterator.scan.someName=10,foo.bar",});

    t.removeIterator("table", "someName", EnumSet.of(IteratorScope.scan));
    setting = new IteratorSetting(20, "otherName", "some.classname");
    setting.addOptions(Collections.singletonMap("key", "value"));
    t.attachIterator("table", setting, EnumSet.of(IteratorScope.majc));
    setting = new IteratorSetting(20, "otherName", "some.classname");
    t.attachIterator("table", setting, EnumSet.of(IteratorScope.scan));
    Map<String,EnumSet<IteratorScope>> two = t.listIterators("table");
    assertEquals(2, two.size());
    assertTrue(two.containsKey("otherName"));
    assertEquals(2, two.get("otherName").size());
    assertTrue(two.get("otherName").contains(IteratorScope.majc));
    assertTrue(two.get("otherName").contains(IteratorScope.scan));
    assertTrue(two.containsKey("someName"));
    assertEquals(1, two.get("someName").size());
    assertTrue(two.get("someName").contains(IteratorScope.majc));
    t.removeIterator("table", "someName", EnumSet.allOf(IteratorScope.class));
    check(t, "table",
        new String[] {"table.iterator.majc.otherName=20,some.classname",
            "table.iterator.majc.otherName.opt.key=value",
            "table.iterator.scan.otherName=20,some.classname",});

    setting = t.getIteratorSetting("table", "otherName", IteratorScope.scan);
    assertEquals(20, setting.getPriority());
    assertEquals("some.classname", setting.getIteratorClass());
    assertTrue(setting.getOptions().isEmpty());
    setting = t.getIteratorSetting("table", "otherName", IteratorScope.majc);
    assertEquals(20, setting.getPriority());
    assertEquals("some.classname", setting.getIteratorClass());
    assertFalse(setting.getOptions().isEmpty());
    assertEquals(Collections.singletonMap("key", "value"), setting.getOptions());
    t.attachIterator("table", setting, EnumSet.of(IteratorScope.minc));
    check(t, "table",
        new String[] {"table.iterator.majc.otherName=20,some.classname",
            "table.iterator.majc.otherName.opt.key=value",
            "table.iterator.minc.otherName=20,some.classname",
            "table.iterator.minc.otherName.opt.key=value",
            "table.iterator.scan.otherName=20,some.classname",});

    try {
      t.attachIterator("table", setting);
      fail();
    } catch (AccumuloException e) {
      // expected, ignore
    }
    setting.setName("thirdName");
    try {
      t.attachIterator("table", setting);
      fail();
    } catch (AccumuloException e) {}
    setting.setPriority(10);
    t.setProperty("table", "table.iterator.minc.thirdName.opt.key", "value");
    try {
      t.attachIterator("table", setting);
      fail();
    } catch (AccumuloException e) {}
    t.removeProperty("table", "table.iterator.minc.thirdName.opt.key");
    t.attachIterator("table", setting);
  }
}

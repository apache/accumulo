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
package org.apache.accumulo.core.client.mock;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;

/**
 * @deprecated since 1.8.0; use MiniAccumuloCluster or a standard mock framework instead.
 */
@Deprecated
public class MockTable {

  static class MockMemKey extends Key {
    private int count;

    MockMemKey(Key key, int count) {
      super(key);
      this.count = count;
    }

    @Override
    public int hashCode() {
      return super.hashCode() + count;
    }

    @Override
    public boolean equals(Object other) {
      return (other instanceof MockMemKey) && super.equals(other) && count == ((MockMemKey) other).count;
    }

    @Override
    public String toString() {
      return super.toString() + " count=" + count;
    }

    @Override
    public int compareTo(Key o) {
      int compare = super.compareTo(o);
      if (compare != 0)
        return compare;
      if (o instanceof MockMemKey) {
        MockMemKey other = (MockMemKey) o;
        if (count < other.count)
          return 1;
        if (count > other.count)
          return -1;
      } else {
        return 1;
      }
      return 0;
    }
  };

  final SortedMap<Key,Value> table = new ConcurrentSkipListMap<>();
  int mutationCount = 0;
  final Map<String,String> settings;
  Map<String,EnumSet<TablePermission>> userPermissions = new HashMap<>();
  private TimeType timeType;
  SortedSet<Text> splits = new ConcurrentSkipListSet<>();
  Map<String,Set<Text>> localityGroups = new TreeMap<>();
  private MockNamespace namespace;
  private String namespaceName;
  private String tableId;

  MockTable(boolean limitVersion, TimeType timeType, String tableId) {
    this.timeType = timeType;
    this.tableId = tableId;
    settings = IteratorUtil.generateInitialTableProperties(limitVersion);
    for (Entry<String,String> entry : DefaultConfiguration.getInstance()) {
      String key = entry.getKey();
      if (key.startsWith(Property.TABLE_PREFIX.getKey()))
        settings.put(key, entry.getValue());
    }
  }

  MockTable(MockNamespace namespace, boolean limitVersion, TimeType timeType, String tableId, Map<String,String> properties) {
    this(limitVersion, timeType, tableId);
    Set<Entry<String,String>> set = namespace.settings.entrySet();
    Iterator<Entry<String,String>> entries = set.iterator();
    while (entries.hasNext()) {
      Entry<String,String> entry = entries.next();
      String key = entry.getKey();
      if (key.startsWith(Property.TABLE_PREFIX.getKey()))
        settings.put(key, entry.getValue());
    }

    for (Entry<String,String> initialProp : properties.entrySet()) {
      settings.put(initialProp.getKey(), initialProp.getValue());
    }
  }

  public MockTable(MockNamespace namespace, TimeType timeType, String tableId, Map<String,String> properties) {
    this.timeType = timeType;
    this.tableId = tableId;
    settings = properties;
    for (Entry<String,String> entry : DefaultConfiguration.getInstance()) {
      String key = entry.getKey();
      if (key.startsWith(Property.TABLE_PREFIX.getKey()))
        settings.put(key, entry.getValue());
    }

    Set<Entry<String,String>> set = namespace.settings.entrySet();
    Iterator<Entry<String,String>> entries = set.iterator();
    while (entries.hasNext()) {
      Entry<String,String> entry = entries.next();
      String key = entry.getKey();
      if (key.startsWith(Property.TABLE_PREFIX.getKey()))
        settings.put(key, entry.getValue());
    }
  }

  synchronized void addMutation(Mutation m) {
    if (m.size() == 0)
      throw new IllegalArgumentException("Can not add empty mutations");
    long now = System.currentTimeMillis();
    mutationCount++;
    for (ColumnUpdate u : m.getUpdates()) {
      Key key = new Key(m.getRow(), 0, m.getRow().length, u.getColumnFamily(), 0, u.getColumnFamily().length, u.getColumnQualifier(), 0,
          u.getColumnQualifier().length, u.getColumnVisibility(), 0, u.getColumnVisibility().length, u.getTimestamp());
      if (u.isDeleted())
        key.setDeleted(true);
      if (!u.hasTimestamp())
        if (timeType.equals(TimeType.LOGICAL))
          key.setTimestamp(mutationCount);
        else
          key.setTimestamp(now);

      table.put(new MockMemKey(key, mutationCount), new Value(u.getValue()));
    }
  }

  public void addSplits(SortedSet<Text> partitionKeys) {
    splits.addAll(partitionKeys);
  }

  public Collection<Text> getSplits() {
    return splits;
  }

  public void setLocalityGroups(Map<String,Set<Text>> groups) {
    localityGroups = groups;
  }

  public Map<String,Set<Text>> getLocalityGroups() {
    return localityGroups;
  }

  public void merge(Text start, Text end) {
    boolean reAdd = false;
    if (splits.contains(start))
      reAdd = true;
    splits.removeAll(splits.subSet(start, end));
    if (reAdd)
      splits.add(start);
  }

  public void setNamespaceName(String n) {
    this.namespaceName = n;
  }

  public void setNamespace(MockNamespace n) {
    this.namespace = n;
  }

  public String getNamespaceName() {
    return this.namespaceName;
  }

  public MockNamespace getNamespace() {
    return this.namespace;
  }

  public String getTableId() {
    return this.tableId;
  }
}

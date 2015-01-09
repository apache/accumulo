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
package org.apache.accumulo.core.client.admin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

/**
 * This class is left in place specifically to test for regressions against the published version of TableOperationsHelper.
 */
public class TableOperationsHelperTest extends org.apache.accumulo.core.client.impl.TableOperationsHelperTest {

  @SuppressWarnings("deprecation")
  static class Tester extends TableOperationsHelper {
    Map<String,Map<String,String>> settings = new HashMap<String,Map<String,String>>();

    @Override
    public SortedSet<String> list() {
      return null;
    }

    @Override
    public boolean exists(String tableName) {
      return true;
    }

    @Override
    public void create(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException {}

    @Override
    public void create(String tableName, boolean limitVersion) throws AccumuloException, AccumuloSecurityException, TableExistsException {
      create(tableName, limitVersion, TimeType.MILLIS);
    }

    @Override
    public void create(String tableName, boolean versioningIter, TimeType timeType) throws AccumuloException, AccumuloSecurityException, TableExistsException {}

    @Override
    public void addSplits(String tableName, SortedSet<Text> partitionKeys) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {}

    @Deprecated
    @Override
    public Collection<Text> getSplits(String tableName) throws TableNotFoundException {
      return null;
    }

    @Deprecated
    @Override
    public Collection<Text> getSplits(String tableName, int maxSplits) throws TableNotFoundException {
      return null;
    }

    @Override
    public Collection<Text> listSplits(String tableName) throws TableNotFoundException {
      return null;
    }

    @Override
    public Collection<Text> listSplits(String tableName, int maxSplits) throws TableNotFoundException {
      return null;
    }

    @Override
    public Text getMaxRow(String tableName, Authorizations auths, Text startRow, boolean startInclusive, Text endRow, boolean endInclusive)
        throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
      return null;
    }

    @Override
    public void merge(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    }

    @Override
    public void deleteRows(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {}

    @Override
    public void compact(String tableName, Text start, Text end, boolean flush, boolean wait) throws AccumuloSecurityException, TableNotFoundException,
        AccumuloException {}

    @Override
    public void compact(String tableName, Text start, Text end, List<IteratorSetting> iterators, boolean flush, boolean wait) throws AccumuloSecurityException,
        TableNotFoundException, AccumuloException {}

    @Override
    public void delete(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {}

    @Override
    public void clone(String srcTableName, String newTableName, boolean flush, Map<String,String> propertiesToSet, Set<String> propertiesToExclude)
        throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {}

    @Override
    public void rename(String oldTableName, String newTableName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException,
        TableExistsException {}

    @Deprecated
    @Override
    public void flush(String tableName) throws AccumuloException, AccumuloSecurityException {}

    @Override
    public void flush(String tableName, Text start, Text end, boolean wait) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {}

    @Override
    public void setProperty(String tableName, String property, String value) throws AccumuloException, AccumuloSecurityException {
      if (!settings.containsKey(tableName))
        settings.put(tableName, new TreeMap<String,String>());
      settings.get(tableName).put(property, value);
    }

    @Override
    public void removeProperty(String tableName, String property) throws AccumuloException, AccumuloSecurityException {
      if (!settings.containsKey(tableName))
        return;
      settings.get(tableName).remove(property);
    }

    @Override
    public Iterable<Entry<String,String>> getProperties(String tableName) throws AccumuloException, TableNotFoundException {
      Map<String,String> empty = Collections.emptyMap();
      if (!settings.containsKey(tableName))
        return empty.entrySet();
      return settings.get(tableName).entrySet();
    }

    @Override
    public void setLocalityGroups(String tableName, Map<String,Set<Text>> groups) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {}

    @Override
    public Map<String,Set<Text>> getLocalityGroups(String tableName) throws AccumuloException, TableNotFoundException {
      return null;
    }

    @Override
    public Set<Range> splitRangeByTablets(String tableName, Range range, int maxSplits) throws AccumuloException, AccumuloSecurityException,
        TableNotFoundException {
      return null;
    }

    @Override
    public void importDirectory(String tableName, String dir, String failureDir, boolean setTime) throws TableNotFoundException, IOException,
        AccumuloException, AccumuloSecurityException {}

    @Override
    public void offline(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

    }

    @Override
    public void online(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {}

    @Override
    public void offline(String tableName, boolean wait) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

    }

    @Override
    public void online(String tableName, boolean wait) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {}

    @Override
    public void clearLocatorCache(String tableName) throws TableNotFoundException {}

    @Override
    public Map<String,String> tableIdMap() {
      return null;
    }

    @Override
    public List<DiskUsage> getDiskUsage(Set<String> tables) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      return null;
    }

    @Override
    public void importTable(String tableName, String exportDir) throws TableExistsException, AccumuloException, AccumuloSecurityException {}

    @Override
    public void exportTable(String tableName, String exportDir) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {}

    @Override
    public void cancelCompaction(String tableName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException {}

    @Override
    public boolean testClassLoad(String tableName, String className, String asTypeName) throws AccumuloException, AccumuloSecurityException,
        TableNotFoundException {
      return false;
    }
  }

  @Override
  protected org.apache.accumulo.core.client.impl.TableOperationsHelper getHelper() {
    return new Tester();
  }
}

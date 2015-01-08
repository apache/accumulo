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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.system.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.NotImplementedException;

public class MockScannerBase extends ScannerOptions implements ScannerBase {

  protected final MockTable table;
  protected final Authorizations auths;

  MockScannerBase(MockTable mockTable, Authorizations authorizations) {
    this.table = mockTable;
    this.auths = authorizations;
  }

  static HashSet<ByteSequence> createColumnBSS(Collection<Column> columns) {
    HashSet<ByteSequence> columnSet = new HashSet<ByteSequence>();
    for (Column c : columns) {
      columnSet.add(new ArrayByteSequence(c.getColumnFamily()));
    }
    return columnSet;
  }

  static class MockIteratorEnvironment implements IteratorEnvironment {
    @Override
    public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
      throw new NotImplementedException();
    }

    @Override
    public AccumuloConfiguration getConfig() {
      return AccumuloConfiguration.getDefaultConfiguration();
    }

    @Override
    public IteratorScope getIteratorScope() {
      return IteratorScope.scan;
    }

    @Override
    public boolean isFullMajorCompaction() {
      return false;
    }

    private ArrayList<SortedKeyValueIterator<Key,Value>> topLevelIterators = new ArrayList<SortedKeyValueIterator<Key,Value>>();

    @Override
    public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
      topLevelIterators.add(iter);
    }

    SortedKeyValueIterator<Key,Value> getTopLevelIterator(SortedKeyValueIterator<Key,Value> iter) {
      if (topLevelIterators.isEmpty())
        return iter;
      ArrayList<SortedKeyValueIterator<Key,Value>> allIters = new ArrayList<SortedKeyValueIterator<Key,Value>>(topLevelIterators);
      allIters.add(iter);
      return new MultiIterator(allIters, false);
    }
  }

  public SortedKeyValueIterator<Key,Value> createFilter(SortedKeyValueIterator<Key,Value> inner) throws IOException {
    byte[] defaultLabels = {};
    inner = new ColumnFamilySkippingIterator(new DeletingIterator(inner, false));
    ColumnQualifierFilter cqf = new ColumnQualifierFilter(inner, new HashSet<Column>(fetchedColumns));
    VisibilityFilter vf = new VisibilityFilter(cqf, auths, defaultLabels);
    AccumuloConfiguration conf = new MockConfiguration(table.settings);
    MockIteratorEnvironment iterEnv = new MockIteratorEnvironment();
    SortedKeyValueIterator<Key,Value> result = iterEnv.getTopLevelIterator(IteratorUtil.loadIterators(IteratorScope.scan, vf, null, conf,
        serverSideIteratorList, serverSideIteratorOptions, iterEnv, false));
    return result;
  }

  @Override
  public Iterator<Entry<Key,Value>> iterator() {
    throw new UnsupportedOperationException();
  }
}

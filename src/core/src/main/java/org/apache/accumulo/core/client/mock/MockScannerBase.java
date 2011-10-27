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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.iterators.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.DeletingIterator;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SystemScanIterator;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

public class MockScannerBase implements ScannerBase {
  
  List<IterInfo> ssiList = new ArrayList<IterInfo>();
  Map<String,Map<String,String>> ssio = new HashMap<String,Map<String,String>>();
  
  final HashSet<Column> columns = new HashSet<Column>();
  protected final MockTable table;
  protected final Authorizations auths;
  
  MockScannerBase(MockTable mockTable, Authorizations authorizations) {
    this.table = mockTable;
    this.auths = authorizations;
  }
  
  public void setScanIterators(int priority, String iteratorClass, String iteratorName) throws IOException {
    ssiList.add(new IterInfo(priority, iteratorClass, iteratorName));
  }
  
  public void setScanIteratorOption(String iteratorName, String key, String value) {
    Map<String,String> kv = ssio.get(iteratorName);
    if (kv == null)
      ssio.put(iteratorName, kv = new HashMap<String,String>());
    kv.put(key, value);
  }
  
  public void fetchColumnFamily(Text col) {
    columns.add(new Column(TextUtil.getBytes(col), null, null));
  }
  
  public void fetchColumn(Text colFam, Text colQual) {
    columns.add(new Column(TextUtil.getBytes(colFam), TextUtil.getBytes(colQual), null));
  }
  
  public void clearColumns() {
    columns.clear();
  }
  
  public void clearScanIterators() {
    ssiList.clear();
    ssio.clear();
  }
  
  static HashSet<ByteSequence> createColumnBSS(Collection<Column> columns) {
    HashSet<ByteSequence> columnSet = new HashSet<ByteSequence>();
    for (Column c : columns) {
      columnSet.add(new ArrayByteSequence(c.getColumnFamily()));
    }
    return columnSet;
  }
  
  public SortedKeyValueIterator<Key,Value> createFilter(SortedKeyValueIterator<Key,Value> inner) throws IOException {
    IteratorEnvironment iterEnv = null;
    byte[] defaultLabels = {};
    inner = new ColumnFamilySkippingIterator(new DeletingIterator(inner, false));
    SystemScanIterator systemIter = new SystemScanIterator(inner, auths, defaultLabels, columns);
    AccumuloConfiguration conf = new MockConfiguration(table.settings);
    SortedKeyValueIterator<Key,Value> result = IteratorUtil.loadIterators(IteratorScope.scan, systemIter, null, conf, ssiList, ssio, iterEnv);
    return result;
  }
  
  public void setupRegex(String iteratorName, int iteratorPriority) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public void setRowRegex(String regex) {
    throw new UnsupportedOperationException();
  }
  
  public void setColumnFamilyRegex(String regex) {
    throw new UnsupportedOperationException();
  }
  
  public void setColumnQualifierRegex(String regex) {
    throw new UnsupportedOperationException();
  }
  
  public void setValueRegex(String regex) {
    throw new UnsupportedOperationException();
  }
}
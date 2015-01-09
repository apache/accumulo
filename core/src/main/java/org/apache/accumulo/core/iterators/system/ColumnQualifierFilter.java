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
package org.apache.accumulo.core.iterators.system;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class ColumnQualifierFilter extends Filter {
  private boolean scanColumns;
  private HashSet<ByteSequence> columnFamilies;
  private HashMap<ByteSequence,HashSet<ByteSequence>> columnsQualifiers;

  public ColumnQualifierFilter() {}

  public ColumnQualifierFilter(SortedKeyValueIterator<Key,Value> iterator, Set<Column> columns) {
    setSource(iterator);
    init(columns);
  }

  public ColumnQualifierFilter(SortedKeyValueIterator<Key,Value> iterator, HashSet<ByteSequence> columnFamilies,
      HashMap<ByteSequence,HashSet<ByteSequence>> columnsQualifiers, boolean scanColumns) {
    setSource(iterator);
    this.columnFamilies = columnFamilies;
    this.columnsQualifiers = columnsQualifiers;
    this.scanColumns = scanColumns;
  }

  public boolean accept(Key key, Value v) {
    if (!scanColumns)
      return true;

    if (columnFamilies.contains(key.getColumnFamilyData()))
      return true;

    HashSet<ByteSequence> cfset = columnsQualifiers.get(key.getColumnQualifierData());
    // ensure the columm qualifier goes with a paired column family,
    // it is possible that a column qualifier could occur with a
    // column family it was not paired with
    return cfset != null && cfset.contains(key.getColumnFamilyData());
  }

  public void init(Set<Column> columns) {
    this.columnFamilies = new HashSet<ByteSequence>();
    this.columnsQualifiers = new HashMap<ByteSequence,HashSet<ByteSequence>>();

    for (Iterator<Column> iter = columns.iterator(); iter.hasNext();) {
      Column col = iter.next();
      if (col.columnQualifier != null) {
        ArrayByteSequence cq = new ArrayByteSequence(col.columnQualifier);
        HashSet<ByteSequence> cfset = this.columnsQualifiers.get(cq);
        if (cfset == null) {
          cfset = new HashSet<ByteSequence>();
          this.columnsQualifiers.put(cq, cfset);
        }

        cfset.add(new ArrayByteSequence(col.columnFamily));
      } else {
        // this whole column family should pass
        columnFamilies.add(new ArrayByteSequence(col.columnFamily));
      }
    }

    // only take action when column qualifies are present
    scanColumns = this.columnsQualifiers.size() > 0;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new ColumnQualifierFilter(getSource().deepCopy(env), columnFamilies, columnsQualifiers, scanColumns);
  }
}

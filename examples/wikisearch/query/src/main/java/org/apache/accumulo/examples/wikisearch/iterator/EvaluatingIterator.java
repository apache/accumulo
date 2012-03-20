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
package org.apache.accumulo.examples.wikisearch.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.wikisearch.parser.EventFields;
import org.apache.accumulo.examples.wikisearch.parser.EventFields.FieldValue;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.io.Text;


public class EvaluatingIterator extends AbstractEvaluatingIterator {
  
  public static final String NULL_BYTE_STRING = "\u0000";
  LRUMap visibilityMap = new LRUMap();
  
  public EvaluatingIterator() {
    super();
  }
  
  public EvaluatingIterator(AbstractEvaluatingIterator other, IteratorEnvironment env) {
    super(other, env);
  }
  
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new EvaluatingIterator(this, env);
  }
  
  @Override
  public PartialKey getKeyComparator() {
    return PartialKey.ROW_COLFAM;
  }
  
  @Override
  public Key getReturnKey(Key k) {
    // If we were using column visibility, then we would get the merged visibility here and use it in the key.
    // Remove the COLQ from the key and use the combined visibility
    Key r = new Key(k.getRowData().getBackingArray(), k.getColumnFamilyData().getBackingArray(), NULL_BYTE, k.getColumnVisibility().getBytes(),
        k.getTimestamp(), k.isDeleted(), false);
    return r;
  }
  
  @Override
  public void fillMap(EventFields event, Key key, Value value) {
    // If we were using column visibility, we would have to merge them here.
    
    // Pull the datatype from the colf in case we need to do anything datatype specific.
    // String colf = key.getColumnFamily().toString();
    // String datatype = colf.substring(0, colf.indexOf(NULL_BYTE_STRING));
    
    // For the partitioned table, the field name and field value are stored in the column qualifier
    // separated by a \0.
    String colq = key.getColumnQualifier().toString();// .toLowerCase();
    int idx = colq.indexOf(NULL_BYTE_STRING);
    String fieldName = colq.substring(0, idx);
    String fieldValue = colq.substring(idx + 1);
    
    event.put(fieldName, new FieldValue(getColumnVisibility(key), fieldValue.getBytes()));
  }

  /**
   * @param key
   * @return
   */
  public ColumnVisibility getColumnVisibility(Key key) {
    ColumnVisibility result = (ColumnVisibility) visibilityMap.get(key.getColumnVisibility());
    if (result != null) 
      return result;
    result = new ColumnVisibility(key.getColumnVisibility().getBytes());
    visibilityMap.put(key.getColumnVisibility(), result);
    return result;
  }
  
  /**
   * Don't accept this key if the colf starts with 'fi'
   */
  @Override
  public boolean isKeyAccepted(Key key) throws IOException {
    if (key.getColumnFamily().toString().startsWith("fi")) {
      Key copy = new Key(key.getRow(), new Text("fi\01"));
      Collection<ByteSequence> columnFamilies = Collections.emptyList();
      this.iterator.seek(new Range(copy, copy), columnFamilies, true);
      if (this.iterator.hasTop())
        return isKeyAccepted(this.iterator.getTopKey());
      return true;
    }
    return true;
  }
  
}

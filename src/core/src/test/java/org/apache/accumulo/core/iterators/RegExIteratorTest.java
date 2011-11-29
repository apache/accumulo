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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.filter.RegExFilter;
import org.apache.hadoop.io.Text;

/**
 * @deprecated since 1.4
 */
public class RegExIteratorTest extends TestCase {
  
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
  
  private Key nkv(TreeMap<Key,Value> tm, String row, String cf, String cq, String val) {
    Key k = nk(row, cf, cq);
    tm.put(k, new Value(val.getBytes()));
    return k;
  }
  
  private Key nk(String row, String cf, String cq) {
    return new Key(new Text(row), new Text(cf), new Text(cq));
  }
  
  public void test1() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    
    Key k1 = nkv(tm, "boo1", "yup", "20080201", "dog");
    Key k2 = nkv(tm, "boo1", "yap", "20080202", "cat");
    Key k3 = nkv(tm, "boo2", "yip", "20080203", "hamster");
    
    RegExIterator rei = new RegExIterator();
    rei.describeOptions();
    
    HashMap<String,String> options = new HashMap<String,String>();
    
    options.put(RegExFilter.ROW_REGEX, ".*2");
    rei.validateOptions(options);
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k3));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put(RegExFilter.COLF_REGEX, "ya.*");
    rei.validateOptions(options);
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put(RegExFilter.COLQ_REGEX, ".*01");
    rei.validateOptions(options);
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k1));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put(RegExFilter.VALUE_REGEX, ".*at");
    rei.validateOptions(options);
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put(RegExFilter.VALUE_REGEX, ".*ap");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put(RegExFilter.COLF_REGEX, "ya.*");
    options.put(RegExFilter.VALUE_REGEX, ".*at");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put(RegExFilter.COLF_REGEX, "ya.*");
    options.put(RegExFilter.VALUE_REGEX, ".*ap");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put(RegExFilter.ROW_REGEX, "boo1");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k1));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k1));
    rei.next();
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k3));
    rei.next();
    assertFalse(rei.hasTop());
  }
  
  public void test2() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    
    Key k1 = nkv(tm, "boo1", "yup", "20080201", "dog");
    Key k2 = nkv(tm, "boo1", "yap", "20080202", "cat");
    Key k3 = nkv(tm, "boo2", "yip", "20080203", "hamster");
    
    FilteringIterator rei = new FilteringIterator();
    
    HashMap<String,String> options = new HashMap<String,String>();
    
    options.put("0", RegExFilter.class.getName());
    options.put("0." + RegExFilter.ROW_REGEX, ".*2");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k3));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put("0", RegExFilter.class.getName());
    options.put("0." + RegExFilter.COLF_REGEX, "ya.*");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put("0", RegExFilter.class.getName());
    options.put("0." + RegExFilter.COLQ_REGEX, ".*01");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k1));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put("0", RegExFilter.class.getName());
    options.put("0." + RegExFilter.VALUE_REGEX, ".*at");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put("0", RegExFilter.class.getName());
    options.put("0." + RegExFilter.VALUE_REGEX, ".*ap");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put("0", RegExFilter.class.getName());
    options.put("0." + RegExFilter.COLF_REGEX, "ya.*");
    options.put("0." + RegExFilter.VALUE_REGEX, ".*at");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put("0", RegExFilter.class.getName());
    options.put("0." + RegExFilter.COLF_REGEX, "ya.*");
    options.put("0." + RegExFilter.VALUE_REGEX, ".*ap");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put("0", RegExFilter.class.getName());
    options.put("0." + RegExFilter.ROW_REGEX, "boo1");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k1));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k1));
    rei.next();
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k3));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put("0", RegExFilter.class.getName());
    options.put("0." + RegExFilter.ROW_REGEX, "hamster");
    options.put("0." + RegExFilter.COLQ_REGEX, "hamster");
    options.put("0." + RegExFilter.VALUE_REGEX, "hamster");
    options.put("0." + RegExFilter.OR_FIELDS, "true");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k3));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    options.clear();
    
    options.put("0", RegExFilter.class.getName());
    options.put("0." + RegExFilter.COLF_REGEX, "ya.*");
    options.put("0." + RegExFilter.COLQ_REGEX, "hamster");
    options.put("0." + RegExFilter.OR_FIELDS, "true");
    rei.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertFalse(rei.hasTop());
  }
  
}

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
package org.apache.accumulo.core.iterators.user;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DefaultIteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.hadoop.io.Text;

public class RegExFilterTest extends TestCase {
  
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
    
    RegExFilter rei = new RegExFilter();
    rei.describeOptions();
    
    IteratorSetting is = new IteratorSetting(1, RegExFilter.class);
    RegExFilter.setRegexs(is, ".*2", null, null, null, false);
    
    rei.validateOptions(is.getOptions());
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k3));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    is.clearOptions();
    
    RegExFilter.setRegexs(is, null, "ya.*", null, null, false);
    rei.validateOptions(is.getOptions());
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    is.clearOptions();
    
    RegExFilter.setRegexs(is, null, null, ".*01", null, false);
    rei.validateOptions(is.getOptions());
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k1));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    is.clearOptions();
    
    RegExFilter.setRegexs(is, null, null, null, ".*at", false);
    rei.validateOptions(is.getOptions());
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    is.clearOptions();
    
    RegExFilter.setRegexs(is, null, null, null, ".*ap", false);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    is.clearOptions();
    
    RegExFilter.setRegexs(is, null, "ya.*", null, ".*at", false);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    is.clearOptions();
    
    RegExFilter.setRegexs(is, null, "ya.*", null, ".*ap", false);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    is.clearOptions();
    
    RegExFilter.setRegexs(is, "boo1", null, null, null, false);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k1));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    is.clearOptions();
    
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
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
    is.clearOptions();
    
    RegExFilter.setRegexs(is, "hamster", null, "hamster", "hamster", true);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k3));
    rei.next();
    assertFalse(rei.hasTop());
    
    // -----------------------------------------------------
    is.clearOptions();
    
    RegExFilter.setRegexs(is, null, "ya.*", "hamster", null, true);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    assertTrue(rei.getTopKey().equals(k2));
    rei.next();
    assertFalse(rei.hasTop());
    
    is.clearOptions();
    
    RegExFilter.setRegexs(is, null, "ya.*", "hamster", null, true);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    rei.deepCopy(new DefaultIteratorEnvironment());
    
    // -----------------------------------------------------
    String multiByteText = new String("\u6d67" + "\u6F68" + "\u7067");
    String multiByteRegex = new String(".*" + "\u6F68" + ".*");
    
    Key k4 = new Key("boo4".getBytes(), "hoo".getBytes(), "20080203".getBytes(), "".getBytes(), 1l);
    Value inVal = new Value(multiByteText.getBytes("UTF-8"));
    tm.put(k4, inVal);
    
    is.clearOptions();
    
    RegExFilter.setRegexs(is, null, null, null, multiByteRegex, true);
    rei.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    rei.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(rei.hasTop());
    Value outValue = rei.getTopValue();
    String outVal = new String(outValue.get(), "UTF-8");
    assertTrue(outVal.equals(multiByteText));
    
  }
}

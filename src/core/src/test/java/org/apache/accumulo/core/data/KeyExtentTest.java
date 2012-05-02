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
package org.apache.accumulo.core.data;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;

public class KeyExtentTest extends TestCase {
  KeyExtent nke(String t, String er, String per) {
    return new KeyExtent(new Text(t), er == null ? null : new Text(er), per == null ? null : new Text(per));
  }
  
  public void testDecodingMetadataRow() {
    Text flattenedExtent = new Text("foo;bar");
    
    KeyExtent ke = new KeyExtent(flattenedExtent, (Text) null);
    
    assertTrue(ke.getEndRow().equals(new Text("bar")));
    assertTrue(ke.getTableId().equals(new Text("foo")));
    assertTrue(ke.getPrevEndRow() == null);
    
    flattenedExtent = new Text("foo<");
    
    ke = new KeyExtent(flattenedExtent, (Text) null);
    
    assertTrue(ke.getEndRow() == null);
    assertTrue(ke.getTableId().equals(new Text("foo")));
    assertTrue(ke.getPrevEndRow() == null);
    
    flattenedExtent = new Text("foo;bar;");
    
    ke = new KeyExtent(flattenedExtent, (Text) null);
    
    assertTrue(ke.getEndRow().equals(new Text("bar;")));
    assertTrue(ke.getTableId().equals(new Text("foo")));
    assertTrue(ke.getPrevEndRow() == null);
    
  }
  
  public void testFindContainingExtents() {
    TreeSet<KeyExtent> set0 = new TreeSet<KeyExtent>();
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", null, null), set0) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("t", "1", "0"), set0) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("t", "1", null), set0) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("t", null, "0"), set0) == null);
    
    TreeSet<KeyExtent> set1 = new TreeSet<KeyExtent>();
    
    set1.add(nke("t", null, null));
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", null, null), set1).equals(nke("t", null, null)));
    assertTrue(KeyExtent.findContainingExtent(nke("t", "1", "0"), set1).equals(nke("t", null, null)));
    assertTrue(KeyExtent.findContainingExtent(nke("t", "1", null), set1).equals(nke("t", null, null)));
    assertTrue(KeyExtent.findContainingExtent(nke("t", null, "0"), set1).equals(nke("t", null, null)));
    
    TreeSet<KeyExtent> set2 = new TreeSet<KeyExtent>();
    
    set2.add(nke("t", "g", null));
    set2.add(nke("t", null, "g"));
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", null, null), set2) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("t", "c", "a"), set2).equals(nke("t", "g", null)));
    assertTrue(KeyExtent.findContainingExtent(nke("t", "c", null), set2).equals(nke("t", "g", null)));
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", "g", "a"), set2).equals(nke("t", "g", null)));
    assertTrue(KeyExtent.findContainingExtent(nke("t", "g", null), set2).equals(nke("t", "g", null)));
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", "h", "a"), set2) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("t", "h", null), set2) == null);
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", "z", "f"), set2) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("t", null, "f"), set2) == null);
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", "z", "g"), set2).equals(nke("t", null, "g")));
    assertTrue(KeyExtent.findContainingExtent(nke("t", null, "g"), set2).equals(nke("t", null, "g")));
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", "z", "h"), set2).equals(nke("t", null, "g")));
    assertTrue(KeyExtent.findContainingExtent(nke("t", null, "h"), set2).equals(nke("t", null, "g")));
    
    TreeSet<KeyExtent> set3 = new TreeSet<KeyExtent>();
    
    set3.add(nke("t", "g", null));
    set3.add(nke("t", "s", "g"));
    set3.add(nke("t", null, "s"));
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", null, null), set3) == null);
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", "g", null), set3).equals(nke("t", "g", null)));
    assertTrue(KeyExtent.findContainingExtent(nke("t", "s", "g"), set3).equals(nke("t", "s", "g")));
    assertTrue(KeyExtent.findContainingExtent(nke("t", null, "s"), set3).equals(nke("t", null, "s")));
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", "t", "g"), set3) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("t", "t", "f"), set3) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("t", "s", "f"), set3) == null);
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", "r", "h"), set3).equals(nke("t", "s", "g")));
    assertTrue(KeyExtent.findContainingExtent(nke("t", "s", "h"), set3).equals(nke("t", "s", "g")));
    assertTrue(KeyExtent.findContainingExtent(nke("t", "r", "g"), set3).equals(nke("t", "s", "g")));
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", null, "t"), set3).equals(nke("t", null, "s")));
    assertTrue(KeyExtent.findContainingExtent(nke("t", null, "r"), set3) == null);
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", "f", null), set3).equals(nke("t", "g", null)));
    assertTrue(KeyExtent.findContainingExtent(nke("t", "h", null), set3) == null);
    
    TreeSet<KeyExtent> set4 = new TreeSet<KeyExtent>();
    
    set4.add(nke("t1", "d", null));
    set4.add(nke("t1", "q", "d"));
    set4.add(nke("t1", null, "q"));
    set4.add(nke("t2", "g", null));
    set4.add(nke("t2", "s", "g"));
    set4.add(nke("t2", null, "s"));
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", null, null), set4) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("z", null, null), set4) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("t11", null, null), set4) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("t1", null, null), set4) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("t2", null, null), set4) == null);
    
    assertTrue(KeyExtent.findContainingExtent(nke("t", "g", null), set4) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("z", "g", null), set4) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("t11", "g", null), set4) == null);
    assertTrue(KeyExtent.findContainingExtent(nke("t1", "g", null), set4) == null);
    
    assertTrue(KeyExtent.findContainingExtent(nke("t2", "g", null), set4).equals(nke("t2", "g", null)));
    assertTrue(KeyExtent.findContainingExtent(nke("t2", "s", "g"), set4).equals(nke("t2", "s", "g")));
    assertTrue(KeyExtent.findContainingExtent(nke("t2", null, "s"), set4).equals(nke("t2", null, "s")));
    
    assertTrue(KeyExtent.findContainingExtent(nke("t1", "d", null), set4).equals(nke("t1", "d", null)));
    assertTrue(KeyExtent.findContainingExtent(nke("t1", "q", "d"), set4).equals(nke("t1", "q", "d")));
    assertTrue(KeyExtent.findContainingExtent(nke("t1", null, "q"), set4).equals(nke("t1", null, "q")));
    
  }
  
  private static boolean overlaps(KeyExtent extent, SortedMap<KeyExtent,Object> extents) {
    return !KeyExtent.findOverlapping(extent, extents).isEmpty();
  }
  
  public void testOverlaps() {
    SortedMap<KeyExtent,Object> set0 = new TreeMap<KeyExtent,Object>();
    set0.put(nke("a", null, null), null);
    
    // Nothing overlaps with the empty set
    assertFalse(overlaps(nke("t", null, null), null));
    assertFalse(overlaps(nke("t", null, null), set0));
    
    SortedMap<KeyExtent,Object> set1 = new TreeMap<KeyExtent,Object>();
    
    // Everything overlaps with the infinite range
    set1.put(nke("t", null, null), null);
    assertTrue(overlaps(nke("t", null, null), set1));
    assertTrue(overlaps(nke("t", "b", "a"), set1));
    assertTrue(overlaps(nke("t", null, "a"), set1));
    
    set1.put(nke("t", "b", "a"), null);
    assertTrue(overlaps(nke("t", null, null), set1));
    assertTrue(overlaps(nke("t", "b", "a"), set1));
    assertTrue(overlaps(nke("t", null, "a"), set1));
    
    // simple overlaps
    SortedMap<KeyExtent,Object> set2 = new TreeMap<KeyExtent,Object>();
    set2.put(nke("a", null, null), null);
    set2.put(nke("t", "m", "j"), null);
    set2.put(nke("z", null, null), null);
    assertTrue(overlaps(nke("t", null, null), set2));
    assertTrue(overlaps(nke("t", "m", "j"), set2));
    assertTrue(overlaps(nke("t", "z", "a"), set2));
    assertFalse(overlaps(nke("t", "j", "a"), set2));
    assertFalse(overlaps(nke("t", "z", "m"), set2));
    
    // non-overlaps
    assertFalse(overlaps(nke("t", "b", "a"), set2));
    assertFalse(overlaps(nke("t", "z", "y"), set2));
    assertFalse(overlaps(nke("t", "b", null), set2));
    assertFalse(overlaps(nke("t", null, "y"), set2));
    assertFalse(overlaps(nke("t", "j", null), set2));
    assertFalse(overlaps(nke("t", null, "m"), set2));
    
    // infinite overlaps
    SortedMap<KeyExtent,Object> set3 = new TreeMap<KeyExtent,Object>();
    set3.put(nke("t", "j", null), null);
    set3.put(nke("t", null, "m"), null);
    assertTrue(overlaps(nke("t", "k", "a"), set3));
    assertTrue(overlaps(nke("t", "k", null), set3));
    assertTrue(overlaps(nke("t", "z", "k"), set3));
    assertTrue(overlaps(nke("t", null, "k"), set3));
    assertTrue(overlaps(nke("t", null, null), set3));
    
    // falls between
    assertFalse(overlaps(nke("t", "l", "k"), set3));
    
    SortedMap<KeyExtent,Object> set4 = new TreeMap<KeyExtent,Object>();
    set4.put(nke("t", null, null), null);
    assertTrue(overlaps(nke("t", "k", "a"), set4));
    assertTrue(overlaps(nke("t", "k", null), set4));
    assertTrue(overlaps(nke("t", "z", "k"), set4));
    assertTrue(overlaps(nke("t", null, "k"), set4));
    assertTrue(overlaps(nke("t", null, null), set4));
    assertTrue(overlaps(nke("t", null, null), set4));
    
    for (String er : new String[] {"z", "y", "r", null}) {
      for (String per : new String[] {"a", "b", "d", null}) {
        assertTrue(nke("t", "y", "b").overlaps(nke("t", er, per)));
        assertTrue(nke("t", "y", null).overlaps(nke("t", er, per)));
        assertTrue(nke("t", null, "b").overlaps(nke("t", er, per)));
        assertTrue(nke("t", null, null).overlaps(nke("t", er, per)));
      }
    }
    
    assertFalse(nke("t", "y", "b").overlaps(nke("t", "z", "y")));
    assertFalse(nke("t", "y", "b").overlaps(nke("t", null, "y")));
    assertFalse(nke("t", "y", null).overlaps(nke("t", "z", "y")));
    assertFalse(nke("t", "y", null).overlaps(nke("t", null, "y")));
    assertFalse(nke("t", "y", "b").overlaps(nke("t", "b", "a")));
    assertFalse(nke("t", "y", "b").overlaps(nke("t", "b", null)));
    assertFalse(nke("t", null, "b").overlaps(nke("t", "b", "a")));
    assertFalse(nke("t", null, "b").overlaps(nke("t", "b", null)));
  }
}

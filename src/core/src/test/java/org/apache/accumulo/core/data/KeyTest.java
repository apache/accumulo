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

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;

public class KeyTest extends TestCase {
  public void testDeletedCompare() {
    Key k1 = new Key("r1".getBytes(), "cf".getBytes(), "cq".getBytes(), new byte[0], 0, false);
    Key k2 = new Key("r1".getBytes(), "cf".getBytes(), "cq".getBytes(), new byte[0], 0, false);
    Key k3 = new Key("r1".getBytes(), "cf".getBytes(), "cq".getBytes(), new byte[0], 0, true);
    Key k4 = new Key("r1".getBytes(), "cf".getBytes(), "cq".getBytes(), new byte[0], 0, true);
    
    assertTrue(k1.compareTo(k2) == 0);
    assertTrue(k3.compareTo(k4) == 0);
    assertTrue(k1.compareTo(k3) > 0);
    assertTrue(k3.compareTo(k1) < 0);
  }
  
  public void testCopyData() {
    byte row[] = "r".getBytes();
    byte cf[] = "cf".getBytes();
    byte cq[] = "cq".getBytes();
    byte cv[] = "cv".getBytes();
    
    Key k1 = new Key(row, cf, cq, cv, 5l, false, false);
    Key k2 = new Key(row, cf, cq, cv, 5l, false, true);
    
    assertSame(row, k1.getRowBytes());
    assertSame(cf, k1.getColFamily());
    assertSame(cq, k1.getColQualifier());
    assertSame(cv, k1.getColVisibility());
    
    assertSame(row, k1.getRowData().getBackingArray());
    assertSame(cf, k1.getColumnFamilyData().getBackingArray());
    assertSame(cq, k1.getColumnQualifierData().getBackingArray());
    assertSame(cv, k1.getColumnVisibilityData().getBackingArray());
    
    assertNotSame(row, k2.getRowBytes());
    assertNotSame(cf, k2.getColFamily());
    assertNotSame(cq, k2.getColQualifier());
    assertNotSame(cv, k2.getColVisibility());
    
    assertNotSame(row, k2.getRowData().getBackingArray());
    assertNotSame(cf, k2.getColumnFamilyData().getBackingArray());
    assertNotSame(cq, k2.getColumnQualifierData().getBackingArray());
    assertNotSame(cv, k2.getColumnVisibilityData().getBackingArray());
    
    assertEquals(k1, k2);
    
  }
  
  public void testString() {
    Key k1 = new Key("r1");
    Key k2 = new Key(new Text("r1"));
    assertEquals(k2, k1);
    
    k1 = new Key("r1", "cf1");
    k2 = new Key(new Text("r1"), new Text("cf1"));
    assertEquals(k2, k1);
    
    k1 = new Key("r1", "cf2", "cq2");
    k2 = new Key(new Text("r1"), new Text("cf2"), new Text("cq2"));
    assertEquals(k2, k1);
    
    k1 = new Key("r1", "cf2", "cq2", "cv");
    k2 = new Key(new Text("r1"), new Text("cf2"), new Text("cq2"), new Text("cv"));
    assertEquals(k2, k1);
    
    k1 = new Key("r1", "cf2", "cq2", "cv", 89);
    k2 = new Key(new Text("r1"), new Text("cf2"), new Text("cq2"), new Text("cv"), 89);
    assertEquals(k2, k1);
    
    k1 = new Key("r1", "cf2", "cq2", 89);
    k2 = new Key(new Text("r1"), new Text("cf2"), new Text("cq2"), 89);
    assertEquals(k2, k1);
    
  }
  
  public void testVisibilityFollowingKey() {
    Key k = new Key("r", "f", "q", "v");
    assertEquals(k.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS).toString(), "r f:q [v%00;] " + Long.MAX_VALUE + " false");
  }
}

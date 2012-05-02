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

public class ColumnTest extends TestCase {
  public void testEquals() {
    Column[] col = createColumns();
    for (int i = 0; i < col.length; i++) {
      for (int j = 0; j < col.length; j++) {
        if (i == j || (i == 0 && j == 1) || (i == 1 && j == 0))
          assertTrue(col[i].equals(col[j]));
        else
          assertFalse(col[i].equals(col[j]));
      }
    }
  }
  
  public void testCompare() {
    Column[] col = createColumns();
    for (int i = 0; i < col.length; i++) {
      for (int j = 0; j < col.length; j++) {
        if (i == j || (i == 0 && j == 1) || (i == 1 && j == 0))
          assertTrue(col[i].compareTo(col[j]) == 0);
        else
          assertFalse(col[i].compareTo(col[j]) == 0);
      }
    }
  }
  
  public void testEqualsCompare() {
    Column[] col = createColumns();
    for (int i = 0; i < col.length; i++)
      for (int j = 0; j < col.length; j++)
        assertTrue((col[i].compareTo(col[j]) == 0) == col[i].equals(col[j]));
  }
  
  public Column[] createColumns() {
    Column col[] = new Column[4];
    col[0] = new Column("colfam".getBytes(), "colq".getBytes(), "colv".getBytes());
    col[1] = new Column("colfam".getBytes(), "colq".getBytes(), "colv".getBytes());
    col[2] = new Column(new byte[0], new byte[0], new byte[0]);
    col[3] = new Column(null, null, null);
    return col;
  }
}

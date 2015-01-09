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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.data.thrift.TColumn;
import org.junit.BeforeClass;
import org.junit.Test;

public class ColumnTest {
  static Column col[];

  @BeforeClass
  public static void setup() {
    col = new Column[5];
    col[0] = new Column("colfam".getBytes(), "colq".getBytes(), "colv".getBytes());
    col[1] = new Column("colfam".getBytes(), "colq".getBytes(), "colv".getBytes());
    col[2] = new Column(new byte[0], new byte[0], new byte[0]);
    col[3] = new Column(null, null, null);
    col[4] = new Column("colfam".getBytes(), "cq".getBytes(), "cv".getBytes());
  }

  @Test
  public void testEquals() {
    for (int i = 0; i < col.length; i++) {
      for (int j = 0; j < col.length; j++) {
        if (i == j || (i == 0 && j == 1) || (i == 1 && j == 0))
          assertTrue(col[i].equals(col[j]));
        else
          assertFalse(col[i].equals(col[j]));
      }
    }
  }

  @Test
  public void testCompare() {
    for (int i = 0; i < col.length; i++) {
      for (int j = 0; j < col.length; j++) {
        if (i == j || (i == 0 && j == 1) || (i == 1 && j == 0))
          assertEquals(0, col[i].compareTo(col[j]));
        else
          assertNotEquals(0, col[i].compareTo(col[j]));
      }
    }
  }

  @Test
  public void testEqualsCompare() {
    for (int i = 0; i < col.length; i++)
      for (int j = 0; j < col.length; j++)
        assertEquals(col[i].equals(col[j]), col[i].compareTo(col[j]) == 0);
  }

  @Test
  public void testWriteReadFields() throws IOException {
    for (Column c : col) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      c.write(new DataOutputStream(baos));

      Column other = new Column();
      other.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

      assertEquals(c, other);
    }
  }

  @Test
  public void testThriftRoundTrip() {
    for (Column c : col) {
      TColumn tc = c.toThrift();
      assertEquals(c, new Column(tc));
    }
  }
}

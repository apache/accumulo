/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.data;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ColumnTest {
  static Column[] col;

  @BeforeAll
  public static void setup() {
    col = new Column[5];
    col[0] = new Column("colfam".getBytes(UTF_8), "colq".getBytes(UTF_8), "colv".getBytes(UTF_8));
    col[1] = new Column("colfam".getBytes(UTF_8), "colq".getBytes(UTF_8), "colv".getBytes(UTF_8));
    col[2] = new Column(new byte[0], new byte[0], new byte[0]);
    col[3] = new Column(null, null, null);
    col[4] = new Column("colfam".getBytes(UTF_8), "cq".getBytes(UTF_8), "cv".getBytes(UTF_8));
  }

  @Test
  public void testEquals() {
    for (int i = 0; i < col.length; i++) {
      for (int j = 0; j < col.length; j++) {
        if (i == j || (i == 0 && j == 1) || (i == 1 && j == 0)) {
          assertTrue(col[i].equals(col[j]));
        } else {
          assertFalse(col[i].equals(col[j]));
        }
      }
    }
  }

  @Test
  public void testCompare() {
    for (int i = 0; i < col.length; i++) {
      for (int j = 0; j < col.length; j++) {
        if (i == j || (i == 0 && j == 1) || (i == 1 && j == 0)) {
          assertEquals(0, col[i].compareTo(col[j]));
        } else {
          assertNotEquals(0, col[i].compareTo(col[j]));
        }
      }
    }
  }

  @Test
  public void testEqualsCompare() {
    for (Column value : col) {
      for (Column column : col) {
        assertEquals(value.equals(column), value.compareTo(column) == 0);
      }
    }
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

  @Test
  public void testHashCode() {
    // Testing consistency
    for (Column c : col) {
      assertEquals(c.hashCode(), c.hashCode());
    }

    // Testing equality
    Column[] colCopy = new Column[5];
    colCopy[0] =
        new Column("colfam".getBytes(UTF_8), "colq".getBytes(UTF_8), "colv".getBytes(UTF_8));
    colCopy[1] =
        new Column("colfam".getBytes(UTF_8), "colq".getBytes(UTF_8), "colv".getBytes(UTF_8));
    colCopy[2] = new Column(new byte[0], new byte[0], new byte[0]);
    colCopy[3] = new Column(null, null, null);
    colCopy[4] = new Column("colfam".getBytes(UTF_8), "cq".getBytes(UTF_8), "cv".getBytes(UTF_8));

    for (int i = 0; i < col.length; i++) {
      assertEquals(col[i].hashCode(), colCopy[i].hashCode());
    }

    // Testing even distribution
    List<Column> columns = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      columns.add(new Column(("colfam" + i).getBytes(UTF_8), ("colq" + i).getBytes(UTF_8),
          ("colv" + i).getBytes(UTF_8)));
    }
    Set<Integer> hashCodes = new HashSet<>();
    for (Column c : columns) {
      hashCodes.add(c.hashCode());
    }
    assertEquals(columns.size(), hashCodes.size(), 10);

  }

}

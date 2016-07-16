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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.data.thrift.TKey;
import org.apache.accumulo.core.data.thrift.TKeyValue;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class KeyTest {

  @Test
  public void testDeletedCompare() {
    Key k1 = new Key("r1".getBytes(), "cf".getBytes(), "cq".getBytes(), new byte[0], 0, false);
    Key k2 = new Key("r1".getBytes(), "cf".getBytes(), "cq".getBytes(), new byte[0], 0, false);
    Key k3 = new Key("r1".getBytes(), "cf".getBytes(), "cq".getBytes(), new byte[0], 0, true);
    Key k4 = new Key("r1".getBytes(), "cf".getBytes(), "cq".getBytes(), new byte[0], 0, true);

    assertTrue(k1.equals(k2));
    assertTrue(k3.equals(k4));
    assertTrue(k1.compareTo(k3) > 0);
    assertTrue(k3.compareTo(k1) < 0);
  }

  @Test
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

  @Test
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

  @Test
  public void testVisibilityFollowingKey() {
    Key k = new Key("r", "f", "q", "v");
    assertEquals(k.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS).toString(), "r f:q [v%00;] " + Long.MAX_VALUE + " false");
  }

  public void testVisibilityGetters() {
    Key k = new Key("r", "f", "q", "v1|(v2&v3)");

    Text expression = k.getColumnVisibility();
    ColumnVisibility parsed = k.getColumnVisibilityParsed();

    assertEquals(expression, new Text(parsed.getExpression()));
  }

  @Test
  public void testThrift() {
    Key k = new Key("r1", "cf2", "cq2", "cv");
    TKey tk = k.toThrift();
    Key k2 = new Key(tk);
    assertEquals(k, k2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testThrift_Invalid() {
    Key k = new Key("r1", "cf2", "cq2", "cv");
    TKey tk = k.toThrift();
    tk.setRow((byte[]) null);
    new Key(tk);
  }

  @Test
  public void testCompressDecompress() {
    List<KeyValue> kvs = new ArrayList<>();
    kvs.add(new KeyValue(new Key(), new byte[] {}));
    kvs.add(new KeyValue(new Key("r"), new byte[] {}));
    kvs.add(new KeyValue(new Key("r", "cf"), new byte[] {}));
    kvs.add(new KeyValue(new Key("r2", "cf"), new byte[] {}));
    kvs.add(new KeyValue(new Key("r", "cf", "cq"), new byte[] {}));
    kvs.add(new KeyValue(new Key("r2", "cf2", "cq"), new byte[] {}));
    kvs.add(new KeyValue(new Key("r", "cf", "cq", "cv"), new byte[] {}));
    kvs.add(new KeyValue(new Key("r2", "cf2", "cq2", "cv"), new byte[] {}));
    kvs.add(new KeyValue(new Key("r2", "cf2", "cq2", "cv"), new byte[] {}));
    kvs.add(new KeyValue(new Key(), new byte[] {}));

    List<TKeyValue> tkvs = Key.compress(kvs);
    Key.decompress(tkvs);

    assertEquals(kvs.size(), tkvs.size());
    Iterator<KeyValue> kvi = kvs.iterator();
    Iterator<TKeyValue> tkvi = tkvs.iterator();

    while (kvi.hasNext()) {
      KeyValue kv = kvi.next();
      TKeyValue tkv = tkvi.next();
      assertEquals(kv.getKey(), new Key(tkv.getKey()));
    }
  }

  @Test
  public void testBytesText() {
    byte[] row = new byte[] {1};
    Key bytesRowKey = new Key(row);
    Key textRowKey = new Key(new Text(row));
    assertEquals(bytesRowKey, textRowKey);

    byte[] colFamily = new byte[] {0, 1};
    Key bytesColFamilyKey = new Key(row, colFamily);
    Key textColFamilyKey = new Key(new Text(row), new Text(colFamily));
    assertEquals(bytesColFamilyKey, textColFamilyKey);

    byte[] colQualifier = new byte[] {0, 0, 1};
    Key bytesColQualifierKey = new Key(row, colFamily, colQualifier);
    Key textColQualifierKey = new Key(new Text(row), new Text(colFamily), new Text(colQualifier));
    assertEquals(bytesColQualifierKey, textColQualifierKey);

    byte[] colVisibility = new byte[] {0, 0, 0, 1};
    Key bytesColVisibilityKey = new Key(row, colFamily, colQualifier, colVisibility);
    Key textColVisibilityKey = new Key(new Text(row), new Text(colFamily), new Text(colQualifier), new Text(colVisibility));
    assertEquals(bytesColVisibilityKey, textColVisibilityKey);

    long ts = 0L;
    Key bytesTSKey = new Key(row, colFamily, colQualifier, colVisibility, ts);
    Key textTSKey = new Key(new Text(row), new Text(colFamily), new Text(colQualifier), new Text(colVisibility), ts);
    assertEquals(bytesTSKey, textTSKey);

    Key bytesTSKey2 = new Key(row, ts);
    Key textTSKey2 = new Key(new Text(row), ts);
    assertEquals(bytesTSKey2, textTSKey2);

    Key bytesTSKey3 = new Key(row, colFamily, colQualifier, ts);
    Key testTSKey3 = new Key(new Text(row), new Text(colFamily), new Text(colQualifier), ts);
    assertEquals(bytesTSKey3, testTSKey3);

    ColumnVisibility colVisibility2 = new ColumnVisibility("v1");
    Key bytesColVisibilityKey2 = new Key(row, colFamily, colQualifier, colVisibility2, ts);
    Key textColVisibilityKey2 = new Key(new Text(row), new Text(colFamily), new Text(colQualifier), colVisibility2, ts);
    assertEquals(bytesColVisibilityKey2, textColVisibilityKey2);
  }
}

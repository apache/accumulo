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
package org.apache.accumulo.tserver;

import static org.apache.accumulo.tserver.AssignmentHandler.checkTabletMetadata;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class CheckTabletMetadataTest {

  private static Key newKey(String row, ColumnFQ cfq) {
    return new Key(new Text(row), cfq.getColumnFamily(), cfq.getColumnQualifier());
  }

  private static Key newKey(String row, Text cf, String cq) {
    return new Key(row, cf.toString(), cq);
  }

  private static void put(TreeMap<Key,Value> tabletMeta, String row, ColumnFQ cfq, byte[] val) {
    Key k = new Key(new Text(row), cfq.getColumnFamily(), cfq.getColumnQualifier());
    tabletMeta.put(k, new Value(val));
  }

  private static void put(TreeMap<Key,Value> tabletMeta, String row, Text cf, String cq,
      String val) {
    Key k = new Key(new Text(row), cf, new Text(cq));
    tabletMeta.put(k, new Value(val));
  }

  private static void assertFail(TreeMap<Key,Value> tabletMeta, KeyExtent ke, TServerInstance tsi) {
    try {
      TabletMetadata tm = TabletMetadata.convertRow(tabletMeta.entrySet().iterator(),
          EnumSet.allOf(ColumnType.class), true);
      assertFalse(checkTabletMetadata(ke, tsi, tm));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void assertFail(TreeMap<Key,Value> tabletMeta, KeyExtent ke, TServerInstance tsi,
      Key keyToDelete) {
    TreeMap<Key,Value> copy = new TreeMap<>(tabletMeta);
    assertNotNull(copy.remove(keyToDelete));
    try {
      TabletMetadata tm = TabletMetadata.convertRow(copy.entrySet().iterator(),
          EnumSet.allOf(ColumnType.class), true);
      assertFalse(checkTabletMetadata(ke, tsi, tm));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testBadTabletMetadata() throws Exception {

    KeyExtent ke = new KeyExtent(TableId.of("1"), null, null);

    TreeMap<Key,Value> tabletMeta = new TreeMap<>();

    put(tabletMeta, "1<", TabletColumnFamily.PREV_ROW_COLUMN,
        TabletColumnFamily.encodePrevEndRow(null).get());
    put(tabletMeta, "1<", ServerColumnFamily.DIRECTORY_COLUMN, "t1".getBytes());
    put(tabletMeta, "1<", ServerColumnFamily.TIME_COLUMN, "M0".getBytes());
    put(tabletMeta, "1<", FutureLocationColumnFamily.NAME, "4", "127.0.0.1:9997");

    TServerInstance tsi = new TServerInstance("127.0.0.1:9997", 4);

    TabletMetadata tm = TabletMetadata.convertRow(tabletMeta.entrySet().iterator(),
        EnumSet.allOf(ColumnType.class), true);
    assertTrue(checkTabletMetadata(ke, tsi, tm));

    assertFail(tabletMeta, ke, new TServerInstance("127.0.0.1:9998", 4));
    assertFail(tabletMeta, ke, new TServerInstance("127.0.0.1:9998", 5));
    assertFail(tabletMeta, ke, new TServerInstance("127.0.0.1:9997", 5));
    assertFail(tabletMeta, ke, new TServerInstance("127.0.0.2:9997", 4));
    assertFail(tabletMeta, ke, new TServerInstance("127.0.0.2:9997", 5));

    assertFail(tabletMeta, new KeyExtent(TableId.of("1"), null, new Text("m")), tsi);

    assertFail(tabletMeta, new KeyExtent(TableId.of("1"), new Text("r"), new Text("m")), tsi);

    assertFail(tabletMeta, ke, tsi, newKey("1<", TabletColumnFamily.PREV_ROW_COLUMN));

    assertFail(tabletMeta, ke, tsi, newKey("1<", ServerColumnFamily.DIRECTORY_COLUMN));

    assertFail(tabletMeta, ke, tsi, newKey("1<", ServerColumnFamily.TIME_COLUMN));

    assertFail(tabletMeta, ke, tsi, newKey("1<", FutureLocationColumnFamily.NAME, "4"));

    TreeMap<Key,Value> copy = new TreeMap<>(tabletMeta);
    put(copy, "1<", CurrentLocationColumnFamily.NAME, "4", "127.0.0.1:9997");
    assertFail(copy, ke, tsi);
    assertFail(copy, ke, tsi, newKey("1<", FutureLocationColumnFamily.NAME, "4"));

    copy = new TreeMap<>(tabletMeta);
    put(copy, "1<", CurrentLocationColumnFamily.NAME, "5", "127.0.0.1:9998");
    assertFail(copy, ke, tsi);
    put(copy, "1<", CurrentLocationColumnFamily.NAME, "6", "127.0.0.1:9999");
    assertFail(copy, ke, tsi);

    copy = new TreeMap<>(tabletMeta);
    put(copy, "1<", FutureLocationColumnFamily.NAME, "5", "127.0.0.1:9998");
    assertFail(copy, ke, tsi);

    assertFail(new TreeMap<>(), ke, tsi);

  }
}

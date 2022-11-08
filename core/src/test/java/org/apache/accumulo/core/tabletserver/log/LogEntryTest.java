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
package org.apache.accumulo.core.tabletserver.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class LogEntryTest {

  @SuppressWarnings("removal")
  private static void compareLogEntries(LogEntry one, LogEntry two) throws IOException {
    assertNotSame(one, two);
    assertEquals(one.toString(), two.toString());
    assertEquals(one.getColumnFamily(), two.getColumnFamily());
    assertEquals(one.getColumnQualifier(), two.getColumnQualifier());
    assertEquals(one.getRow(), two.getRow());
    assertEquals(one.getUniqueID(), two.getUniqueID());
    assertEquals(one.getValue(), two.getValue());
    // arrays differ in serialized form because of the different prevEndRow, but that shouldn't
    // matter for anything functionality in the LogEntry
    assertFalse(Arrays.equals(one.toBytes(), two.toBytes()));
  }

  @Test
  public void testPrevRowDoesntMatter() throws IOException {
    long ts = 12345678L;
    String filename = "default/foo";

    // with no end row, different prev rows
    LogEntry entry1 =
        new LogEntry(new KeyExtent(TableId.of("1"), null, new Text("A")), ts, filename);
    LogEntry entry2 =
        new LogEntry(new KeyExtent(TableId.of("1"), null, new Text("B")), ts, filename);
    assertEquals("1< default/foo", entry1.toString());
    compareLogEntries(entry1, entry2);

    // with same end row, different prev rows
    LogEntry entry3 =
        new LogEntry(new KeyExtent(TableId.of("2"), new Text("same"), new Text("A")), ts, filename);
    LogEntry entry4 =
        new LogEntry(new KeyExtent(TableId.of("2"), new Text("same"), new Text("B")), ts, filename);
    assertEquals("2;same default/foo", entry3.toString());
    compareLogEntries(entry3, entry4);
  }

  @Test
  public void test() throws Exception {
    KeyExtent extent = new KeyExtent(TableId.of("1"), null, null);
    long ts = 12345678L;
    String filename = "default/foo";
    LogEntry entry = new LogEntry(extent, ts, filename);
    assertEquals(extent.toMetaRow(), entry.getRow());
    assertEquals(filename, entry.filename);
    assertEquals(ts, entry.timestamp);
    assertEquals("1< default/foo", entry.toString());
    assertEquals(new Text("log"), entry.getColumnFamily());
    assertEquals(new Text("-/default/foo"), entry.getColumnQualifier());
    @SuppressWarnings("removal")
    LogEntry copy = LogEntry.fromBytes(entry.toBytes());
    assertEquals(entry.toString(), copy.toString());
    Key key = new Key(new Text("1<"), new Text("log"), new Text("localhost:1234/default/foo"));
    key.setTimestamp(ts);
    var mapEntry = new Entry<Key,Value>() {
      @Override
      public Key getKey() {
        return key;
      }

      @Override
      public Value getValue() {
        return entry.getValue();
      }

      @Override
      public Value setValue(Value value) {
        throw new UnsupportedOperationException();
      }
    };
    LogEntry copy2 = LogEntry.fromMetaWalEntry(mapEntry);
    assertEquals(entry.toString(), copy2.toString());
    assertEquals(entry.timestamp, copy2.timestamp);
    assertEquals("foo", entry.getUniqueID());
    assertEquals("-/default/foo", entry.getColumnQualifier().toString());
    assertEquals(new Value("default/foo"), entry.getValue());
  }

}

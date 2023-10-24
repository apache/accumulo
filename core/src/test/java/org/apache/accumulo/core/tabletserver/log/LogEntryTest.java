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
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class LogEntryTest {

  private static void compareLogEntries(LogEntry one, LogEntry two) throws IOException {
    assertNotSame(one, two);
    assertEquals(one.toString(), two.toString());
    assertEquals(one.getColumnQualifier(), two.getColumnQualifier());
    assertEquals(one.getUniqueID(), two.getUniqueID());
    assertEquals(one.getValue(), two.getValue());
  }

  @Test
  public void test() throws Exception {
    long ts = 12345678L;
    String filename = "default/foo";
    LogEntry entry = new LogEntry(ts, filename);
    assertEquals(filename, entry.getFilePath());
    assertEquals(ts, entry.getTimestamp());
    assertEquals(ts + " " + filename, entry.toString());
    assertEquals(new Text("log"), MetadataSchema.TabletsSection.LogColumnFamily.NAME);
    assertEquals(new Text("-/default/foo"), entry.getColumnQualifier());
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
    assertEquals(entry.getTimestamp(), copy2.getTimestamp());
    assertEquals("foo", entry.getUniqueID());
    assertEquals("-/default/foo", entry.getColumnQualifier().toString());
    assertEquals(new Value("default/foo"), entry.getValue());
  }

}

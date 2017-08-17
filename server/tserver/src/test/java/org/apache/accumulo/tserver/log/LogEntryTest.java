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
package org.apache.accumulo.tserver.log;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class LogEntryTest {

  @Test
  public void test() throws Exception {
    KeyExtent extent = new KeyExtent(Table.ID.of("1"), null, new Text(""));
    long ts = 12345678L;
    String server = "localhost:1234";
    String filename = "default/foo";
    LogEntry entry = new LogEntry(extent, ts, server, filename);
    assertEquals(extent, entry.extent);
    assertEquals(server, entry.server);
    assertEquals(filename, entry.filename);
    assertEquals(ts, entry.timestamp);
    assertEquals("1<; default/foo", entry.toString());
    assertEquals(new Text("log"), entry.getColumnFamily());
    assertEquals(new Text("localhost:1234/default/foo"), entry.getColumnQualifier());
    LogEntry copy = LogEntry.fromBytes(entry.toBytes());
    assertEquals(entry.toString(), copy.toString());
    Key key = new Key(new Text("1<"), new Text("log"), new Text("localhost:1234/default/foo"));
    key.setTimestamp(ts);
    LogEntry copy2 = LogEntry.fromKeyValue(key, entry.getValue());
    assertEquals(entry.toString(), copy2.toString());
    assertEquals(entry.timestamp, copy2.timestamp);
    assertEquals("foo", entry.getUniqueID());
    assertEquals("localhost:1234/default/foo", entry.getName());
    assertEquals(new Value("default/foo".getBytes()), entry.getValue());
  }

}

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
package org.apache.accumulo.core.tabletserver.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;

public class LogEntry {
  public KeyExtent extent;
  public long timestamp;
  public String server;
  public String filename;
  public int tabletId;
  public Collection<String> logSet;

  public LogEntry() {}

  public LogEntry(LogEntry le) {
    this.extent = le.extent;
    this.timestamp = le.timestamp;
    this.server = le.server;
    this.filename = le.filename;
    this.tabletId = le.tabletId;
    this.logSet = new ArrayList<String>(le.logSet);
  }

  public String toString() {
    return extent.toString() + " " + filename + " (" + tabletId + ")";
  }

  public String getName() {
    return server + "/" + filename;
  }

  public byte[] toBytes() throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    extent.write(out);
    out.writeLong(timestamp);
    out.writeUTF(server);
    out.writeUTF(filename);
    out.write(tabletId);
    out.write(logSet.size());
    for (String s : logSet) {
      out.writeUTF(s);
    }
    return Arrays.copyOf(out.getData(), out.getLength());
  }

  public void fromBytes(byte bytes[]) throws IOException {
    DataInputBuffer inp = new DataInputBuffer();
    inp.reset(bytes, bytes.length);
    extent = new KeyExtent();
    extent.readFields(inp);
    timestamp = inp.readLong();
    server = inp.readUTF();
    filename = inp.readUTF();
    tabletId = inp.read();
    int count = inp.read();
    ArrayList<String> logSet = new ArrayList<String>(count);
    for (int i = 0; i < count; i++)
      logSet.add(inp.readUTF());
    this.logSet = logSet;
  }

  static private final Text EMPTY_TEXT = new Text();

  public static LogEntry fromKeyValue(Key key, Value value) {
    LogEntry result = new LogEntry();
    result.extent = new KeyExtent(key.getRow(), EMPTY_TEXT);
    String[] parts = key.getColumnQualifier().toString().split("/", 2);
    result.server = parts[0];
    result.filename = parts[1];
    parts = value.toString().split("\\|");
    result.tabletId = Integer.parseInt(parts[1]);
    result.logSet = Arrays.asList(parts[0].split(";"));
    result.timestamp = key.getTimestamp();
    return result;
  }

  public Text getRow() {
    return extent.getMetadataEntry();
  }

  public Text getColumnFamily() {
    return MetadataSchema.TabletsSection.LogColumnFamily.NAME;
  }

  public Text getColumnQualifier() {
    return new Text(server + "/" + filename);
  }

  public Value getValue() {
    return new Value((StringUtil.join(logSet, ";") + "|" + tabletId).getBytes());
  }
}

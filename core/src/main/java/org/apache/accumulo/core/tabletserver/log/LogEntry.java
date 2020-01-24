/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.tabletserver.log;

import java.io.IOException;
import java.util.Arrays;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;

public class LogEntry {
  public final KeyExtent extent;
  public final long timestamp;
  public final String server;
  public final String filename;

  public LogEntry(LogEntry le) {
    this.extent = le.extent;
    this.timestamp = le.timestamp;
    this.server = le.server;
    this.filename = le.filename;
  }

  public LogEntry(KeyExtent extent, long timestamp, String server, String filename) {
    this.extent = extent;
    this.timestamp = timestamp;
    this.server = server;
    this.filename = filename;
  }

  @Override
  public String toString() {
    return extent + " " + filename;
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
    return Arrays.copyOf(out.getData(), out.getLength());
  }

  public static LogEntry fromBytes(byte[] bytes) throws IOException {
    DataInputBuffer inp = new DataInputBuffer();
    inp.reset(bytes, bytes.length);
    KeyExtent extent = new KeyExtent();
    extent.readFields(inp);
    long timestamp = inp.readLong();
    String server = inp.readUTF();
    String filename = inp.readUTF();
    return new LogEntry(extent, timestamp, server, filename);
  }

  private static final Text EMPTY_TEXT = new Text();

  public static LogEntry fromKeyValue(Key key, String value) {
    String qualifier = key.getColumnQualifierData().toString();
    if (qualifier.indexOf('/') < 1) {
      throw new IllegalArgumentException("Bad key for log entry: " + key);
    }
    KeyExtent extent = new KeyExtent(key.getRow(), EMPTY_TEXT);
    String[] parts = qualifier.split("/", 2);
    String server = parts[0];
    // handle old-style log entries that specify log sets
    parts = value.split("\\|")[0].split(";");
    String filename = parts[parts.length - 1];
    long timestamp = key.getTimestamp();
    return new LogEntry(extent, timestamp, server, filename);
  }

  public static LogEntry fromKeyValue(Key key, Value value) {
    return fromKeyValue(key, value.toString());
  }

  public Text getRow() {
    return extent.getMetadataEntry();
  }

  public Text getColumnFamily() {
    return MetadataSchema.TabletsSection.LogColumnFamily.NAME;
  }

  public String getUniqueID() {
    String[] parts = filename.split("/");
    return parts[parts.length - 1];
  }

  public Text getColumnQualifier() {
    return new Text(server + "/" + filename);
  }

  public Value getValue() {
    return new Value(filename);
  }
}

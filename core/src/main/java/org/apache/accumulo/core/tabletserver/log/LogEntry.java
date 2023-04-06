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

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.hadoop.io.Text;

public class LogEntry {
  private final KeyExtent extent;
  public final long timestamp;
  public final String filename;

  public LogEntry(KeyExtent extent, long timestamp, String filename) {
    // note the prevEndRow in the extent does not matter, and is not used by LogEntry
    this.extent = extent;
    this.timestamp = timestamp;
    this.filename = filename;
  }

  // make copy, but with a different filename
  public LogEntry switchFile(String filename) {
    return new LogEntry(extent, timestamp, filename);
  }

  @Override
  public String toString() {
    return extent.toMetaRow() + " " + filename;
  }

  public static LogEntry fromMetaWalEntry(Entry<Key,Value> entry) {
    final Key key = entry.getKey();
    final Value value = entry.getValue();
    KeyExtent extent = KeyExtent.fromMetaRow(key.getRow());
    // qualifier.split("/")[0] used to store the server, but this is no longer used, and the
    // qualifier can be ignored
    // the following line handles old-style log entry values that specify log sets
    String[] parts = value.toString().split("\\|")[0].split(";");
    String filename = parts[parts.length - 1];
    long timestamp = key.getTimestamp();
    return new LogEntry(extent, timestamp, filename);
  }

  public Text getRow() {
    return extent.toMetaRow();
  }

  public Text getColumnFamily() {
    return LogColumnFamily.NAME;
  }

  public String getUniqueID() {
    String[] parts = filename.split("/");
    return parts[parts.length - 1];
  }

  public Text getColumnQualifier() {
    return new Text("-/" + filename);
  }

  public Value getValue() {
    return new Value(filename);
  }

}

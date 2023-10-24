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
import java.util.Objects;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class LogEntry {

  private final long timestamp;
  private final String filePath;

  public LogEntry(long timestamp, String filePath) {
    validateFilePath(filePath);
    this.timestamp = timestamp;
    this.filePath = filePath;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public String getFilePath() {
    return this.filePath;
  }

  private static void validateFilePath(String filePath) {
    boolean pathIsValid = true; // TODO. check file path
    Preconditions.checkArgument(pathIsValid,
        "Invalid filePath format. Expected format: tserver/UUID");
  }

  /**
   * Make a copy of this LogEntry but replace the file path.
   *
   * @param filePath path to use
   */
  public LogEntry switchFile(String filePath) {
    return new LogEntry(timestamp, filePath);
  }

  @Override
  public String toString() {
    return filePath;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof LogEntry)) {
      return false;
    }
    LogEntry logEntry = (LogEntry) other;
    return this.timestamp == logEntry.timestamp && this.filePath.equals(logEntry.filePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, filePath);
  }

  public static LogEntry fromMetaWalEntry(Entry<Key,Value> entry) {
    final Key key = entry.getKey();
    final Value value = entry.getValue();

    // the older format seems to split on "|", and then on ";".
    // We're only interested in the last part after splitting on ";", which seems to be the filePath
    String[] parts = value.toString().split("\\|")[0].split(";");
    String filePath = parts[parts.length - 1];

    validateFilePath(filePath);

    return new LogEntry(key.getTimestamp(), filePath);
  }

  public String getUniqueID() {
    String[] parts = filePath.split("/");
    return parts[parts.length - 1];
  }

  public Text getColumnQualifier() {
    return new Text("-/" + filePath);
  }

  public Value getValue() {
    return new Value(filePath);
  }

}

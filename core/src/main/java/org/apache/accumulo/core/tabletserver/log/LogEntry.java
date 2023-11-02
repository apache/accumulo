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
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.google.common.net.HostAndPort;

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

  /**
   * Validates the expected format of the file path. We expect the path to contain a tserver
   * (host:port) followed by a UUID as the file name. For example,
   * localhost:1234/927ba659-d109-4bce-b0a5-bcbbcb9942a2 is a valid file path.
   *
   * @param filePath path to validate
   * @throws IllegalArgumentException if the filepath is invalid
   */
  private static void validateFilePath(String filePath) {
    String[] parts = filePath.split("/");

    if (parts.length < 2) {
      throw new IllegalArgumentException(
          "Invalid filePath format. The path should at least contain tserver/UUID.");
    }

    String tserverPart = parts[parts.length - 2];
    String uuidPart = parts[parts.length - 1];

    try {
      var ignored = HostAndPort.fromString(tserverPart);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid tserver format in filePath. Expected format: host:port. Found '" + tserverPart
              + "'");
    }

    try {
      var ignored = UUID.fromString(uuidPart);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Expected valid UUID. Found '" + uuidPart + "'");
    }
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

    String filePath = value.toString();

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

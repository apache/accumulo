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
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

public class LogEntry {

  private final String logReference;

  public LogEntry(String logReference) {
    validateLogReference(logReference);
    this.logReference = logReference;
  }

  public String getLogReference() {
    return this.logReference;
  }

  /**
   * Validates the expected format of the file path. We expect the path to contain a tserver
   * (host:port) followed by a UUID as the file name. For example,
   * localhost:1234/927ba659-d109-4bce-b0a5-bcbbcb9942a2 is a valid file path.
   *
   * @param filePath path to validate
   * @throws IllegalArgumentException if the filePath is invalid
   */
  private static void validateLogReference(String logReference) {
    String[] parts = logReference.split("/");

    if (parts.length != 3) {
      throw new IllegalArgumentException(
          "Invalid logReference format. The reference should contain exactly 3 parts: tserver, port, and UUID.");
    }

    String tserverPart = parts[1];
    String uuidPart = parts[2];

    try {
      HostAndPort.fromString(tserverPart);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid tserver format in logReference. Expected format: host:port. Found '"
              + tserverPart + "'");
    }

    try {
      UUID.fromString(uuidPart);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Expected valid UUID. Found '" + uuidPart + "'");
    }
  }

  /**
   * Add LogEntry information to the provided mutation.
   *
   * @param mutation the mutation to update
   */
  public void addToMutation(Mutation mutation) {
    mutation.at().family(LogColumnFamily.NAME).qualifier(getColumnQualifier()).put(new Value());
  }

  @Override
  public String toString() {
    return logReference;
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
    return this.logReference.equals(logEntry.logReference);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logReference);
  }

  public static LogEntry fromMetaWalEntry(Entry<Key,Value> entry) {
    String qualifier = entry.getKey().getColumnQualifier().toString();
    String[] parts = qualifier.split("/", 3);
    Preconditions.checkArgument(parts.length == 3, "Malformed write-ahead log %s", qualifier);
    return new LogEntry(parts[1] + "/" + parts[2]);
  }

  public String getUniqueID() {
    String[] parts = logReference.split("/");
    return parts[parts.length - 1];
  }

  public Text getColumnQualifier() {
    return new Text(logReference);
  }

}

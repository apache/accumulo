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
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

public final class LogEntry {

  private final String path;
  private final HostAndPort tserver;
  private final UUID uniqueId;

  private LogEntry(String path, HostAndPort tserver, UUID uniqueId) {
    this.path = path;
    this.tserver = tserver;
    this.uniqueId = uniqueId;
  }

  /**
   * Creates a new LogEntry object after validating the expected format of the path. We expect the
   * path to contain a tserver (host+port) followed by a UUID as the file name as the last two
   * components.<br>
   * For example, file:///some/dir/path/localhost+1234/927ba659-d109-4bce-b0a5-bcbbcb9942a2 is a
   * valid path.
   *
   * @param path path to validate
   * @return an object representation of this log entry
   * @throws IllegalArgumentException if the path is invalid
   */
  public static LogEntry fromPath(String path) {
    String[] parts = path.split("/");

    if (parts.length < 2) {
      throw new IllegalArgumentException(
          "Invalid path format. The path should end with tserver/UUID.");
    }

    String tserverPart = parts[parts.length - 2];
    String uuidPart = parts[parts.length - 1];

    String badTServerMsg =
        "Invalid tserver in path. Expected: host+port. Found '" + tserverPart + "'";
    if (tserverPart.contains(":") || !tserverPart.contains("+")) {
      throw new IllegalArgumentException(badTServerMsg);
    }
    HostAndPort tserver;
    try {
      tserver = HostAndPort.fromString(tserverPart.replace("+", ":"));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(badTServerMsg);
    }

    String badUuidMsg = "Expected valid UUID. Found '" + uuidPart + "'";
    UUID uuid;
    try {
      uuid = UUID.fromString(uuidPart);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(badUuidMsg);
    }
    if (!uuid.toString().equals(uuidPart)) {
      throw new IllegalArgumentException(badUuidMsg);
    }

    return new LogEntry(path, tserver, uuid);
  }

  /**
   * Construct a new LogEntry object after deserializing it from a metadata entry.
   *
   * @param entry the metadata entry
   * @return a new LogEntry object constructed from the path stored in the column qualifier
   * @throws IllegalArgumentException if the path stored in the metadata entry is invalid or if the
   *         serialized format of the entry is unrecognized
   */
  public static LogEntry fromMetaWalEntry(Entry<Key,Value> entry) {
    Text fam = entry.getKey().getColumnFamily();
    Preconditions.checkArgument(LogColumnFamily.NAME.equals(fam),
        "The provided metadata entry's column family is %s instead of %s", fam,
        LogColumnFamily.NAME);
    String qualifier = entry.getKey().getColumnQualifier().toString();
    String[] parts = qualifier.split("/", 2);
    Preconditions.checkArgument(parts.length == 2 && parts[0].equals("-"),
        "Malformed write-ahead log %s", qualifier);
    return fromPath(parts[1]);
  }

  @NonNull
  @VisibleForTesting
  HostAndPort getTServer() {
    return tserver;
  }

  @NonNull
  public String getPath() {
    return path;
  }

  @NonNull
  public UUID getUniqueID() {
    return uniqueId;
  }

  @Override
  public String toString() {
    return path;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other instanceof LogEntry) {
      return path.equals(((LogEntry) other).path);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }

  /**
   * Get the Text that should be used as the column qualifier to store this as a metadata entry.
   */
  @VisibleForTesting
  Text getColumnQualifier() {
    return new Text("-/" + getPath());
  }

  /**
   * Put a delete marker in the provided mutation for this LogEntry.
   *
   * @param mutation the mutation to update
   */
  public void deleteFromMutation(Mutation mutation) {
    mutation.putDelete(LogColumnFamily.NAME, getColumnQualifier());
  }

  /**
   * Put this LogEntry into the provided mutation.
   *
   * @param mutation the mutation to update
   */
  public void addToMutation(Mutation mutation) {
    mutation.put(LogColumnFamily.NAME, getColumnQualifier(), new Value());
  }

}

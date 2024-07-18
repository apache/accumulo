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
package org.apache.accumulo.core.metadata;

import java.net.URI;
import java.util.Objects;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ScanServerFileReferenceSection;
import org.apache.accumulo.core.util.UuidUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class ScanServerRefTabletFile extends TabletFile {

  @SuppressWarnings("deprecation")
  private static final String OLD_PREFIX =
      org.apache.accumulo.core.metadata.schema.MetadataSchema.OldScanServerFileReferenceSection
          .getRowPrefix();
  private final String prefix;
  private final Value NULL_VALUE = new Value(new byte[0]);
  private final Text serverAddress;
  private final Text uuid;

  public ScanServerRefTabletFile(UUID serverLockUUID, String serverAddress, String file) {
    super(new Path(URI.create(file)));
    // For new data, always use the current prefix
    prefix = ScanServerFileReferenceSection.getRowPrefix();
    this.serverAddress = new Text(serverAddress);
    uuid = new Text(serverLockUUID.toString());
  }

  public ScanServerRefTabletFile(Key k) {
    super(new Path(URI.create(extractFile(k))));
    serverAddress = k.getColumnFamily();
    if (isOldPrefix(k)) {
      prefix = OLD_PREFIX;
      uuid = new Text(k.getColumnQualifier().toString());
    } else {
      prefix = ScanServerFileReferenceSection.getRowPrefix();
      var row = k.getRow().toString();
      Preconditions.checkArgument(row.startsWith(prefix), "Unexpected row prefix %s ", row);
      var uuidStr = row.substring(prefix.length());
      Preconditions.checkArgument(UuidUtil.isUUID(uuidStr, 0), "Row suffix is not uuid %s", row);
      uuid = new Text(uuidStr);
    }
  }

  public Mutation putMutation() {
    // Only write scan refs in the new format
    Mutation mutation = new Mutation(prefix + uuid.toString());
    mutation.put(serverAddress, getFilePath(), getValue());
    return mutation;
  }

  public Mutation putDeleteMutation() {
    Mutation mutation;
    if (Objects.equals(prefix, OLD_PREFIX)) {
      mutation = new Mutation(prefix + this.getPath().toString());
      mutation.putDelete(serverAddress, uuid);
    } else {
      mutation = new Mutation(prefix + uuid.toString());
      mutation.putDelete(serverAddress, getFilePath());
    }
    return mutation;
  }

  private static String extractFile(Key k) {
    if (isOldPrefix(k)) {
      return k.getRow().toString().substring(OLD_PREFIX.length());
    } else {
      return k.getColumnQualifier().toString();
    }
  }

  /**
   * Returns the correctly formatted range for a unique uuid
   *
   * @param uuid ServerLockUUID of a Scan Server
   * @return Range for a single scan server
   */
  public static Range getRange(UUID uuid) {
    Objects.requireNonNull(uuid);
    return new Range(MetadataSchema.ScanServerFileReferenceSection.getRowPrefix() + uuid);
  }

  private static boolean isOldPrefix(Key k) {
    return k.getRow().toString().startsWith(OLD_PREFIX);
  }

  public UUID getServerLockUUID() {
    return UUID.fromString(uuid.toString());
  }

  public Text getFilePath() {
    return new Text(this.getPath().toString());
  }

  public Value getValue() {
    return NULL_VALUE;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((serverAddress == null) ? 0 : serverAddress.hashCode());
    result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ScanServerRefTabletFile other = (ScanServerRefTabletFile) obj;
    return Objects.equals(serverAddress, other.serverAddress) && Objects.equals(uuid, other.uuid);
  }

  @Override
  public String toString() {
    return "ScanServerRefTabletFile [file=" + this.getPath().toString() + ", server address="
        + serverAddress + ", server lock uuid=" + uuid + "]";
  }

}

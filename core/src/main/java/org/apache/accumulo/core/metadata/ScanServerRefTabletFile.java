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
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.OldScanServerFileReferenceSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ScanServerFileReferenceSection;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class ScanServerRefTabletFile extends TabletFile {

  @SuppressWarnings("deprecation")
  private static final String oldPrefix = OldScanServerFileReferenceSection.getRowPrefix();
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
      prefix = oldPrefix;
      uuid = new Text(k.getColumnQualifier().toString());
    } else {
      prefix = ScanServerFileReferenceSection.getRowPrefix();
      uuid = new Text(k.getRow().toString().substring(prefix.length()));
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
    if (Objects.equals(prefix, oldPrefix)) {
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
      return k.getRow().toString().substring(oldPrefix.length());
    } else {
      return k.getColumnQualifier().toString();
    }
  }

  private static boolean isOldPrefix(Key k) {
    return k.getRow().toString().startsWith(oldPrefix);
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

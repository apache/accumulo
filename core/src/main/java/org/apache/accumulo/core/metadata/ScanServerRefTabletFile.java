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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class ScanServerRefTabletFile extends ReferencedTabletFile {

  private final Value NULL_VALUE = new Value(new byte[0]);
  private final Text serverAddress;
  private final Text uuid;

  public ScanServerRefTabletFile(UUID serverLockUUID, String serverAddress, String file) {
    super(new Path(URI.create(file)));
    this.serverAddress = new Text(serverAddress);
    uuid = new Text(serverLockUUID.toString());
  }

  public ScanServerRefTabletFile(String file, String serverAddress, UUID serverLockUUID) {
    super(new Path(URI.create(file)));
    this.serverAddress = new Text(serverAddress);
    this.uuid = new Text(serverLockUUID.toString());
  }

  public ScanServerRefTabletFile(Key k) {
    super(new Path(URI.create(k.getColumnQualifier().toString())));
    serverAddress = k.getColumnFamily();
    uuid = k.getRow();
  }

  public Mutation putMutation() {
    Mutation mutation = new Mutation(uuid.toString());
    mutation.put(serverAddress, getFilePath(), getValue());
    return mutation;
  }

  public Mutation putDeleteMutation() {
    Mutation mutation = new Mutation(uuid.toString());
    mutation.putDelete(serverAddress, getFilePath());
    return mutation;
  }

  public Text getFilePath() {
    return new Text(this.getNormalizedPathStr());
  }

  public UUID getServerLockUUID() {
    return UUID.fromString(uuid.toString());
  }

  public Value getValue() {
    return NULL_VALUE;
  }

  public Text getServerAddress() {
    return serverAddress;
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
    return "ScanServerRefTabletFile [file=" + this.getNormalizedPathStr() + ", server address="
        + serverAddress + ", server lock uuid=" + uuid + "]";
  }

}

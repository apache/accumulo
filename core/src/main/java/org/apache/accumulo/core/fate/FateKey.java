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
package org.apache.accumulo.core.fate;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.hadoop.io.DataInputBuffer;

public class FateKey {

  private final FateKeyType type;
  private final Optional<KeyExtent> keyExtent;
  private final Optional<ExternalCompactionId> compactionId;
  private final byte[] serialized;

  private FateKey(FateKeyType type, KeyExtent keyExtent) {
    this.type = Objects.requireNonNull(type);
    this.keyExtent = Optional.of(keyExtent);
    this.compactionId = Optional.empty();
    this.serialized = serialize(type, keyExtent);
  }

  private FateKey(FateKeyType type, ExternalCompactionId compactionId) {
    this.type = Objects.requireNonNull(type);
    this.keyExtent = Optional.empty();
    this.compactionId = Optional.of(compactionId);
    this.serialized = serialize(type, compactionId);
  }

  private FateKey(byte[] serialized) {
    try (DataInputBuffer buffer = new DataInputBuffer()) {
      buffer.reset(serialized, serialized.length);
      this.type = FateKeyType.valueOf(buffer.readUTF());
      this.keyExtent = deserializeKeyExtent(type, buffer);
      this.compactionId = deserializeCompactionId(type, buffer);
      this.serialized = serialized;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public FateKeyType getType() {
    return type;
  }

  public Optional<KeyExtent> getKeyExtent() {
    return keyExtent;
  }

  public Optional<ExternalCompactionId> getCompactionId() {
    return compactionId;
  }

  public byte[] getSerialized() {
    return serialized;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FateKey fateKey = (FateKey) o;
    return Arrays.equals(serialized, fateKey.serialized);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(serialized);
  }

  public static FateKey deserialize(byte[] serialized) {
    return new FateKey(serialized);
  }

  public static FateKey forSplit(KeyExtent extent) {
    return new FateKey(FateKeyType.SPLIT, extent);
  }

  public static FateKey forCompactionCommit(ExternalCompactionId compactionId) {
    return new FateKey(FateKeyType.COMPACTION_COMMIT, compactionId);
  }

  public enum FateKeyType {
    SPLIT, COMPACTION_COMMIT
  }

  private static byte[] serialize(FateKeyType type, KeyExtent ke) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      dos.writeUTF(type.toString());
      ke.writeTo(dos);
      dos.close();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static byte[] serialize(FateKeyType type, ExternalCompactionId compactionId) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      dos.writeUTF(type.toString());
      dos.writeUTF(compactionId.canonical());
      dos.close();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Optional<KeyExtent> deserializeKeyExtent(FateKeyType type, DataInputBuffer buffer)
      throws IOException {
    switch (type) {
      case SPLIT:
        return Optional.of(KeyExtent.readFrom(buffer));
      case COMPACTION_COMMIT:
        return Optional.empty();
      default:
        throw new IllegalStateException("Unexpected FateInstanceType found " + type);
    }
  }

  private static Optional<ExternalCompactionId> deserializeCompactionId(FateKeyType type,
      DataInputBuffer buffer) throws IOException {
    switch (type) {
      case SPLIT:
        return Optional.empty();
      case COMPACTION_COMMIT:
        return Optional.of(ExternalCompactionId.of(buffer.readUTF()));
      default:
        throw new IllegalStateException("Unexpected FateInstanceType found " + type);
    }
  }
}

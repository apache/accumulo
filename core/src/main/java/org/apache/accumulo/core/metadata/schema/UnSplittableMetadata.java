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
package org.apache.accumulo.core.metadata.schema;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

public class UnSplittableMetadata {

  private final HashCode hashOfSplitParameters;

  private UnSplittableMetadata(KeyExtent keyExtent, long splitThreshold, long maxEndRowSize,
      int maxFilesToOpen, Set<StoredTabletFile> files) {
    this(calculateSplitParamsHash(keyExtent, splitThreshold, maxEndRowSize, maxFilesToOpen, files));
  }

  private UnSplittableMetadata(HashCode hashOfSplitParameters) {
    this.hashOfSplitParameters = Objects.requireNonNull(hashOfSplitParameters);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UnSplittableMetadata that = (UnSplittableMetadata) o;
    return Objects.equals(hashOfSplitParameters, that.hashOfSplitParameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hashOfSplitParameters);
  }

  @Override
  public String toString() {
    return toBase64();
  }

  public String toBase64() {
    return Base64.getEncoder().encodeToString(hashOfSplitParameters.asBytes());
  }

  @SuppressWarnings("UnstableApiUsage")
  private static HashCode calculateSplitParamsHash(KeyExtent keyExtent, long splitThreshold,
      long maxEndRowSize, int maxFilesToOpen, Set<StoredTabletFile> files) {
    Preconditions.checkArgument(splitThreshold > 0, "splitThreshold must be greater than 0");
    Preconditions.checkArgument(maxEndRowSize > 0, "maxEndRowSize must be greater than 0");
    Preconditions.checkArgument(maxFilesToOpen > 0, "maxFilesToOpen must be greater than 0");

    // Use static call to murmur3_128() so the seed is always the same
    // Hashing.goodFastHash will seed with the current time, and we need the seed to be
    // the same across restarts and instances
    var hasher = Hashing.murmur3_128().newHasher();
    hasher.putBytes(serializeKeyExtent(keyExtent)).putLong(splitThreshold).putLong(maxEndRowSize)
        .putInt(maxFilesToOpen);
    files.stream().map(StoredTabletFile::getMetadata).sorted()
        .forEach(path -> hasher.putString(path, UTF_8));
    return hasher.hash();
  }

  public static UnSplittableMetadata toUnSplittable(KeyExtent keyExtent, long splitThreshold,
      long maxEndRowSize, int maxFilesToOpen, Set<StoredTabletFile> files) {
    return new UnSplittableMetadata(keyExtent, splitThreshold, maxEndRowSize, maxFilesToOpen,
        files);
  }

  public static UnSplittableMetadata toUnSplittable(String base64HashOfSplitParameters) {
    return new UnSplittableMetadata(
        HashCode.fromBytes(Base64.getDecoder().decode(base64HashOfSplitParameters)));
  }

  private static byte[] serializeKeyExtent(KeyExtent keyExtent) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      keyExtent.writeTo(dos);
      dos.close();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}

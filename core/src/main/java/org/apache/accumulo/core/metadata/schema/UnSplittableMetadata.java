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
import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.util.json.ByteArrayToBase64TypeAdapter;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;

public class UnSplittableMetadata {

  private static final Gson GSON = ByteArrayToBase64TypeAdapter.createBase64Gson();

  private final long splitThreshold;
  private final long maxEndRowSize;
  private final int maxFilesToOpen;
  private final HashCode filesHash;

  public UnSplittableMetadata(long splitThreshold, long maxEndRowSize, int maxFilesToOpen,
      Set<StoredTabletFile> files) {
    this(splitThreshold, maxEndRowSize, maxFilesToOpen, caclulateFilesHash(files));
  }

  public UnSplittableMetadata(long splitThreshold, long maxEndRowSize, int maxFilesToOpen,
      HashCode filesHash) {
    this.splitThreshold = splitThreshold;
    this.maxEndRowSize = maxEndRowSize;
    this.maxFilesToOpen = maxFilesToOpen;
    this.filesHash = Objects.requireNonNull(filesHash);

    Preconditions.checkState(splitThreshold > 0, "splitThreshold must be greater than 0");
    Preconditions.checkState(maxEndRowSize > 0, "maxEndRowSize must be greater than 0");
    Preconditions.checkState(maxFilesToOpen > 0, "maxFilesToOpen must be greater than 0");
  }

  public long getSplitThreshold() {
    return splitThreshold;
  }

  public long getMaxEndRowSize() {
    return maxEndRowSize;
  }

  public int getMaxFilesToOpen() {
    return maxFilesToOpen;
  }

  public HashCode getFilesHash() {
    return filesHash;
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
    return splitThreshold == that.splitThreshold && maxEndRowSize == that.maxEndRowSize
        && maxFilesToOpen == that.maxFilesToOpen && Objects.equals(filesHash, that.filesHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(splitThreshold, maxEndRowSize, maxFilesToOpen, filesHash);
  }

  private static HashCode caclulateFilesHash(Set<StoredTabletFile> files) {
    // Use static call to murmur3_128() so the seed is always the same
    // Hashing.goodFastHash will seed with the current time, and we need the seed to be
    // the same across restarts and instances
    var hasher = Hashing.murmur3_128().newHasher();
    files.stream().map(StoredTabletFile::getNormalizedPathStr).sorted()
        .forEach(path -> hasher.putString(path, UTF_8));
    return hasher.hash();
  }

  // This class is used to serialize and deserialize this class using GSon. Any changes to this
  // class must consider persisted data.
  private static class GSonData {
    long splitThreshold;
    long maxEndRowSize;
    int maxFilesToOpen;
    byte[] filesHash;
  }

  public String toJson() {
    GSonData jData = new GSonData();

    jData.splitThreshold = splitThreshold;
    jData.maxEndRowSize = maxEndRowSize;
    jData.maxFilesToOpen = maxFilesToOpen;
    jData.filesHash = filesHash.asBytes();

    return GSON.toJson(jData);
  }

  public static UnSplittableMetadata fromJson(String json) {
    GSonData jData = GSON.fromJson(json, GSonData.class);

    return new UnSplittableMetadata(jData.splitThreshold, jData.maxEndRowSize, jData.maxFilesToOpen,
        HashCode.fromBytes(jData.filesHash));
  }

  @Override
  public String toString() {
    return toJson();
  }

  public static UnSplittableMetadata toUnSplittable(long splitThreshold, long maxEndRowSize,
      int maxFilesToOpen, Set<StoredTabletFile> files) {
    return new UnSplittableMetadata(splitThreshold, maxEndRowSize, maxFilesToOpen, files);
  }

}

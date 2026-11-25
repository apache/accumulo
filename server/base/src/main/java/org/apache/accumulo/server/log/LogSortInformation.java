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
package org.apache.accumulo.server.log;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.util.Base64;
import java.util.UUID;

import org.apache.accumulo.core.spi.wal.WalProvider;

/**
 * The information needed to sort a write-ahead log, and its versioned representation in the
 * recovery work queue.
 */
public record LogSortInformation(UUID sortId, String providerId, String source,
    String destination) {

  public LogSortInformation {
    requireNonNull(sortId, "sortId is null");
    requireNonNull(providerId, "providerId is null");
    requireNonNull(source, "source is null");
    requireNonNull(destination, "destination is null");
    checkArgument(providerId.matches(WalProvider.PROVIDER_ID_REGEX), "Invalid WAL provider ID: %s",
        providerId);
    checkArgument(!source.isBlank() && !destination.isBlank(),
        "Recovery source and destination must not be blank");
  }

  /** Serialize this work item for storage in ZooKeeper. */
  public String serialize() {
    return "v2|" + sortId + "|" + providerId + "|" + encode(source) + "|" + encode(destination);
  }

  /** Parse a serialized recovery work item. */
  public static LogSortInformation deserialize(String serialized) {
    requireNonNull(serialized, "serialized sort information is null");
    if (serialized.startsWith("v2|")) {
      String[] parts = serialized.split("\\|", -1);
      if (parts.length != 5) {
        throw new IllegalArgumentException(
            "serialized sort information is not in the expected v2 format");
      }
      return new LogSortInformation(UUID.fromString(parts[1]), parts[2], decode(parts[3]),
          decode(parts[4]));
    }
    String[] parts = serialized.split("\\|", -1);
    if (parts.length != 4) {
      throw new IllegalArgumentException("serialized sort information " + serialized
          + " is not in the expected 4-part format sortId|providerId|source|dest");
    }
    return new LogSortInformation(UUID.fromString(parts[0]), parts[1], parts[2], parts[3]);
  }

  private static String encode(String value) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(UTF_8));
  }

  private static String decode(String value) {
    try {
      return new String(Base64.getUrlDecoder().decode(value), UTF_8);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid base64 field in recovery sort information", e);
    }
  }
}

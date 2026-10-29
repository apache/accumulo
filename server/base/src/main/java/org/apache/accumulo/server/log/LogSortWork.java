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

import static java.util.Objects.requireNonNull;

import java.util.UUID;

/**
 * The information needed to sort a write-ahead log, and its representation in the recovery work
 * queue.
 */
public record LogSortWork(UUID sortId, String source, String destination) {

  public LogSortWork {
    requireNonNull(sortId, "sortId is null");
    requireNonNull(source, "source is null");
    requireNonNull(destination, "destination is null");
  }

  /** Serialize this work item for storage in ZooKeeper. */
  public String serialize() {
    return sortId + "|" + source + "|" + destination;
  }

  /** Parse a serialized recovery work item. */
  public static LogSortWork fromString(String serialized) {
    requireNonNull(serialized, "serialized work is null");
    String[] parts = serialized.split("\\|", -1);
    if (parts.length != 3) {
      throw new IllegalArgumentException("Malformed log sort work: " + serialized);
    }
    return new LogSortWork(UUID.fromString(parts[0]), parts[1], parts[2]);
  }
}

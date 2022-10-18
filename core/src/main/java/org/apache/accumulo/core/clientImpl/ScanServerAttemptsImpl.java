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
package org.apache.accumulo.core.clientImpl;

import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.scan.ScanServerAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to track scan attempts for the ScanServerSelector. Its designed to accept
 * updates concurrently (useful for the batch scanner) and offers a snapshot. When a snapshot is
 * obtained it will not change, this class will still accept updates after generating a snapshot.
 * Snapshots are useful for ensuring that authors of ScanServerSelector plugins do not have to
 * consider strange concurrency issues when writing a plugin.
 */
public class ScanServerAttemptsImpl {

  private static final Logger LOG = LoggerFactory.getLogger(ScanServerAttemptsImpl.class);

  private final Map<TabletId,Collection<ScanServerAttemptImpl>> attempts = new HashMap<>();

  ScanServerAttemptReporter createReporter(String server, TabletId tablet) {
    return result -> {
      LOG.trace("Received result: {}", result);
      synchronized (attempts) {
        attempts.computeIfAbsent(tablet, k -> new ArrayList<>())
            .add(new ScanServerAttemptImpl(result, server));
      }
    };
  }

  /**
   * Creates and returns a snapshot of {@link ScanServerAttempt} objects that were added before this
   * call
   *
   * @return TabletIds mapped to a collection of {@link ScanServerAttempt} objects associated with
   *         that TabletId
   */
  Map<TabletId,Collection<ScanServerAttemptImpl>> snapshot() {
    synchronized (attempts) {
      return attempts.entrySet().stream()
          .collect(toUnmodifiableMap(Entry::getKey, entry -> List.copyOf(entry.getValue())));
    }
  }
}

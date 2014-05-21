/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.replication;

import org.apache.accumulo.core.client.replication.ReplicaSystem;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.TextFormat;

/**
 * Fake ReplicaSystem which returns that the data was fully replicated after some sleep period (in milliseconds)
 * <p>
 * Default sleep amount is 0ms
 */
public class MockReplicaSystem implements ReplicaSystem {
  private static final Logger log = LoggerFactory.getLogger(MockReplicaSystem.class);

  private long sleep = 0;

  @Override
  public Status replicate(Path p, Status status, ReplicationTarget target) {
    Status.Builder builder = Status.newBuilder(status);
    if (status.getInfiniteEnd()) {
      builder.setBegin(Long.MAX_VALUE);
    } else {
      builder.setBegin(status.getEnd());
    }

    try {
      Thread.sleep(sleep);
    } catch (InterruptedException e) {
      log.error("Interrupted while sleeping, will report no progress", e);
      Thread.currentThread().interrupt();
      return status;
    }

    Status newStatus = builder.build();
    log.info("Received {} returned {}", TextFormat.shortDebugString(status), TextFormat.shortDebugString(newStatus));
    return newStatus;
  }

  @Override
  public void configure(String configuration) {
    try {
      sleep = Long.parseLong(configuration);
    } catch (NumberFormatException e) {
      log.warn("Could not parse {} as an integer, using default sleep of {}", configuration, sleep, e);
    }
  }
}

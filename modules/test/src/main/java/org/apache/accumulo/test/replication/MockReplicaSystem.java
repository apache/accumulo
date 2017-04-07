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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.server.replication.ReplicaSystem;
import org.apache.accumulo.server.replication.ReplicaSystemHelper;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fake ReplicaSystem which returns that the data was fully replicated after some sleep period (in milliseconds)
 * <p>
 * Default sleep amount is 0ms
 */
public class MockReplicaSystem implements ReplicaSystem {
  private static final Logger log = LoggerFactory.getLogger(MockReplicaSystem.class);

  private long sleep = 0;

  @Override
  public Status replicate(Path p, Status status, ReplicationTarget target, ReplicaSystemHelper helper) {
    Status newStatus;
    if (status.getClosed() && status.getInfiniteEnd()) {
      Status.Builder builder = Status.newBuilder(status);
      if (status.getInfiniteEnd()) {
        builder.setBegin(Long.MAX_VALUE);
      } else {
        builder.setBegin(status.getEnd());
      }
      newStatus = builder.build();
    } else {
      log.info("{} with status {} is not closed and with infinite length, ignoring", p, status);
      newStatus = status;
    }

    log.debug("Sleeping for {}ms before finishing replication on {}", sleep, p);
    try {
      Thread.sleep(sleep);
    } catch (InterruptedException e) {
      log.error("Interrupted while sleeping, will report no progress", e);
      Thread.currentThread().interrupt();
      return status;
    }

    log.info("For {}, received {}, returned {}", p, ProtobufUtil.toString(status), ProtobufUtil.toString(newStatus));
    try {
      helper.recordNewStatus(p, newStatus, target);
    } catch (TableNotFoundException e) {
      log.error("Tried to update status in replication table for {} as {}, but the table did not exist", p, ProtobufUtil.toString(newStatus), e);
      return status;
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("Tried to record new status in replication table for {} as {}, but got an error", p, ProtobufUtil.toString(newStatus), e);
      return status;
    }

    return newStatus;
  }

  @Override
  public void configure(String configuration) {
    if (StringUtils.isBlank(configuration)) {
      log.debug("No configuration, using default sleep of {}", sleep);
      return;
    }

    try {
      sleep = Long.parseLong(configuration);
    } catch (NumberFormatException e) {
      log.warn("Could not parse {} as an integer, using default sleep of {}", configuration, sleep, e);
    }
  }
}

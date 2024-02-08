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

import java.time.Duration;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.fate.FateStore.FateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Removes Repos, in the Fate store it tracks, that are in a finished or new state for more than a
 * configurable time period. This class stores data in the Fate store under the
 * {@link org.apache.accumulo.core.fate.Fate.TxInfo#TX_AGEOFF} field. The data stored under this
 * field is used to track fate transactions that are candidates for cleanup.
 *
 * <p>
 * No external time source is used. It starts tracking idle time when its created.
 *
 * <p>
 * The {@link #ageOff()} method on this class must be periodically called inorder to cleanup to
 * happen.
 */
public class FateCleaner<T> {

  public interface TimeSource {
    long currentTimeNanos();
  }

  // Statuses that can be aged off if idle for a prolonged period.
  private static final EnumSet<TStatus> AGE_OFF_STATUSES =
      EnumSet.of(TStatus.NEW, TStatus.FAILED, TStatus.SUCCESSFUL);

  // This is used to determine if age off data was persisted by another instance of this object.
  private final UUID instanceId = UUID.randomUUID();

  private static final Logger log = LoggerFactory.getLogger(FateCleaner.class);

  private final FateStore<T> store;

  private final long ageOffTime;
  private final TimeSource timeSource;

  private static class AgeOffInfo {
    final UUID instanceId;
    final long setTime;
    final TStatus status;

    public AgeOffInfo(String ageOffStr) {
      var tokens = ageOffStr.split(":");
      Preconditions.checkArgument(tokens.length == 3, "Malformed input %s", ageOffStr);
      instanceId = UUID.fromString(tokens[0]);
      setTime = Long.parseLong(tokens[1]);
      status = TStatus.valueOf(tokens[2]);
    }

    public AgeOffInfo(UUID instanceId, long time, TStatus status) {
      this.instanceId = instanceId;
      this.setTime = time;
      this.status = status;
    }

    @Override
    public String toString() {
      return instanceId + ":" + setTime + ":" + status;
    }
  }

  private AgeOffInfo readAgeOffInfo(FateTxStore<T> txStore) {
    String ageOffStr = (String) txStore.getTransactionInfo(Fate.TxInfo.TX_AGEOFF);
    if (ageOffStr == null) {
      return null;
    }

    return new AgeOffInfo(ageOffStr);
  }

  private boolean shouldAgeOff(TStatus currStatus, AgeOffInfo ageOffInfo) {
    return AGE_OFF_STATUSES.contains(currStatus) && currStatus == ageOffInfo.status
        && ageOffInfo.instanceId.equals(instanceId)
        && timeSource.currentTimeNanos() - ageOffInfo.setTime >= ageOffTime;
  }

  public void ageOff() {
    store.list().filter(ids -> AGE_OFF_STATUSES.contains(ids.getStatus()))
        .forEach(idStatus -> store.tryReserve(idStatus.getFateId()).ifPresent(txStore -> {
          try {
            AgeOffInfo ageOffInfo = readAgeOffInfo(txStore);
            TStatus currStatus = txStore.getStatus();
            if (ageOffInfo == null || !ageOffInfo.instanceId.equals(instanceId)
                || currStatus != ageOffInfo.status) {
              // set or reset the age off info because it does not exists or it exists but is no
              // longer valid
              var newAgeOffInfo =
                  new AgeOffInfo(instanceId, timeSource.currentTimeNanos(), currStatus);
              txStore.setTransactionInfo(Fate.TxInfo.TX_AGEOFF, newAgeOffInfo.toString());
              log.trace("Set age off data {} {}", idStatus.getFateId(), newAgeOffInfo);
            } else if (shouldAgeOff(currStatus, ageOffInfo)) {
              txStore.delete();
              log.debug("Aged off FATE tx {}", idStatus.getFateId());
            }
          } finally {
            txStore.unreserve(0, TimeUnit.MILLISECONDS);
          }
        }));
  }

  public FateCleaner(FateStore<T> store, Duration duration, TimeSource timeSource) {
    this.store = store;
    this.ageOffTime = duration.toNanos();
    this.timeSource = timeSource;
  }
}

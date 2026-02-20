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

import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.FAILED;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.FAILED_IN_PROGRESS;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.NEW;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUBMITTED;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUCCESSFUL;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.UNKNOWN;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.accumulo.core.logging.FateLogger;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.thrift.TApplicationException;

/**
 * Supports initiating and checking status of fate operations.
 *
 */
public class FateClient<T> {

  private final FateStore<T> store;

  private static final EnumSet<ReadOnlyFateStore.TStatus> FINISHED_STATES =
      EnumSet.of(FAILED, SUCCESSFUL, UNKNOWN);

  public FateClient(FateStore<T> store, Function<Repo<T>,String> toLogStrFunc) {
    this.store = FateLogger.wrap(store, toLogStrFunc, false);
  }

  // get a transaction id back to the requester before doing any work
  public FateId startTransaction() {
    return store.create();
  }

  public FateStore.Seeder<T> beginSeeding() {
    return store.beginSeeding();
  }

  public void seedTransaction(Fate.FateOperation fateOp, FateKey fateKey, Repo<T> repo,
      boolean autoCleanUp) {
    try (var seeder = store.beginSeeding()) {
      @SuppressWarnings("unused")
      var unused = seeder.attemptToSeedTransaction(fateOp, fateKey, repo, autoCleanUp);
    }
  }

  // start work in the transaction.. it is safe to call this
  // multiple times for a transaction... but it will only seed once
  public void seedTransaction(Fate.FateOperation fateOp, FateId fateId, Repo<T> repo,
      boolean autoCleanUp, String goalMessage) {
    Fate.log.info("[{}] Seeding {} {} {}", store.type(), fateOp, fateId, goalMessage);
    store.seedTransaction(fateOp, fateId, repo, autoCleanUp);
  }

  // check on the transaction
  public ReadOnlyFateStore.TStatus waitForCompletion(FateId fateId) {
    return store.read(fateId).waitForStatusChange(FINISHED_STATES);
  }

  /**
   * Attempts to cancel a running Fate transaction
   *
   * @param fateId fate transaction id
   * @return true if transaction transitioned to a failed state or already in a completed state,
   *         false otherwise
   */
  public boolean cancel(FateId fateId) {
    for (int retries = 0; retries < 5; retries++) {
      Optional<FateStore.FateTxStore<T>> optionalTxStore = store.tryReserve(fateId);
      if (optionalTxStore.isPresent()) {
        var txStore = optionalTxStore.orElseThrow();
        try {
          ReadOnlyFateStore.TStatus status = txStore.getStatus();
          Fate.log.info("[{}] status is: {}", store.type(), status);
          if (status == NEW || status == SUBMITTED) {
            txStore.setTransactionInfo(Fate.TxInfo.EXCEPTION, new TApplicationException(
                TApplicationException.INTERNAL_ERROR, "Fate transaction cancelled by user"));
            txStore.setStatus(FAILED_IN_PROGRESS);
            Fate.log.info(
                "[{}] Updated status for {} to FAILED_IN_PROGRESS because it was cancelled by user",
                store.type(), fateId);
            return true;
          } else {
            Fate.log.info("[{}] {} cancelled by user but already in progress or finished state",
                store.type(), fateId);
            return false;
          }
        } finally {
          txStore.unreserve(Duration.ZERO);
        }
      } else {
        // reserved, lets retry.
        UtilWaitThread.sleep(500);
      }
    }
    Fate.log.info("[{}] Unable to reserve transaction {} to cancel it", store.type(), fateId);
    return false;
  }

  // resource cleanup
  public void delete(FateId fateId) {
    FateStore.FateTxStore<T> txStore = store.reserve(fateId);
    try {
      switch (txStore.getStatus()) {
        case NEW:
        case SUBMITTED:
        case FAILED:
        case SUCCESSFUL:
          txStore.delete();
          break;
        case FAILED_IN_PROGRESS:
        case IN_PROGRESS:
          throw new IllegalStateException("Can not delete in progress transaction " + fateId);
        case UNKNOWN:
          // nothing to do, it does not exist
          break;
      }
    } finally {
      txStore.unreserve(Duration.ZERO);
    }
  }

  public String getReturn(FateId fateId) {
    FateStore.FateTxStore<T> txStore = store.reserve(fateId);
    try {
      if (txStore.getStatus() != SUCCESSFUL) {
        throw new IllegalStateException(
            "Tried to get exception when transaction " + fateId + " not in successful state");
      }
      return (String) txStore.getTransactionInfo(Fate.TxInfo.RETURN_VALUE);
    } finally {
      txStore.unreserve(Duration.ZERO);
    }
  }

  // get reportable failures
  public Exception getException(FateId fateId) {
    FateStore.FateTxStore<T> txStore = store.reserve(fateId);
    try {
      if (txStore.getStatus() != FAILED) {
        throw new IllegalStateException(
            "Tried to get exception when transaction " + fateId + " not in failed state");
      }
      return (Exception) txStore.getTransactionInfo(Fate.TxInfo.EXCEPTION);
    } finally {
      txStore.unreserve(Duration.ZERO);
    }
  }

  /**
   * Lists transctions for a given fate key type.
   */
  public Stream<FateKey> list(FateKey.FateKeyType type) {
    return store.list(type);
  }
}

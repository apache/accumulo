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

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Transaction Store: a place to save transactions
 *
 * A transaction consists of a number of operations. To use, first create a fate transaction id, and
 * then seed the transaction with an initial operation. An executor service can then execute the
 * transaction's operation, possibly pushing more operations onto the transaction as each step
 * successfully completes. If a step fails, the stack can be unwound, undoing each operation.
 */
public interface FateStore<T> extends ReadOnlyFateStore<T> {

  /**
   * Create a new fate transaction id
   *
   * @return a new FateId
   */
  FateId create();

  /**
   * An interface that allows read/write access to the data related to a single fate operation.
   */
  interface FateTxStore<T> extends ReadOnlyFateTxStore<T> {
    @Override
    Repo<T> top();

    /**
     * Update the given transaction with the next operation
     *
     * @param repo the operation
     */
    void push(Repo<T> repo) throws StackOverflowException;

    /**
     * Remove the last pushed operation from the given transaction.
     */
    void pop();

    /**
     * Update the state of a given transaction
     *
     * @param status execution status
     */
    void setStatus(TStatus status);

    /**
     * Set transaction-specific information.
     *
     * @param txInfo name of attribute of a transaction to set.
     * @param val transaction data to store
     */
    void setTransactionInfo(Fate.TxInfo txInfo, Serializable val);

    /**
     * Remove the transaction from the store.
     *
     */
    void delete();

    /**
     * Return the given transaction to the store.
     *
     * upon successful return the store now controls the referenced transaction id. caller should no
     * longer interact with it.
     *
     * @param deferTime time in millis to keep this transaction from being returned by
     *        {@link #runnable(java.util.concurrent.atomic.AtomicBoolean, java.util.function.Consumer)}.
     *        Must be non-negative.
     */
    void unreserve(long deferTime, TimeUnit timeUnit);
  }

  /**
   * Attempt to reserve the fate transaction.
   *
   * @param fateId The FateId
   * @return true if reserved by this call, false if already reserved
   */
  Optional<FateTxStore<T>> tryReserve(FateId fateId);

  /**
   * Reserve the fate transaction.
   *
   * Reserving a fate transaction ensures that nothing else in-process interacting via the same
   * instance will be operating on that fate transaction.
   *
   */
  FateTxStore<T> reserve(FateId fateId);

}

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
import java.util.EnumSet;
import java.util.List;

/**
 * Read only access to a Transaction Store.
 *
 * A transaction consists of a number of operations. Instances of this class may check on the queue
 * of outstanding transactions but may neither modify them nor create new ones.
 */
public interface ReadOnlyTStore<T> {

  /**
   * Possible operational status codes. Serialized by name within stores.
   */
  enum TStatus {
    /** Unseeded transaction */
    NEW,
    /** Transaction that is executing */
    IN_PROGRESS,
    /** Transaction has failed, and is in the process of being rolled back */
    FAILED_IN_PROGRESS,
    /** Transaction has failed and has been fully rolled back */
    FAILED,
    /** Transaction has succeeded */
    SUCCESSFUL,
    /** Unrecognized or unknown transaction state */
    UNKNOWN,
    /** Transaction that is eligible to be executed */
    SUBMITTED
  }

  /**
   * Reserve a transaction that is IN_PROGRESS or FAILED_IN_PROGRESS.
   *
   * Reserving a transaction id ensures that nothing else in-process interacting via the same
   * instance will be operating on that transaction id.
   *
   * @return a transaction id that is safe to interact with, chosen by the store.
   */
  long reserve();

  /**
   * Reserve the specific tid.
   *
   * Reserving a transaction id ensures that nothing else in-process interacting via the same
   * instance will be operating on that transaction id.
   *
   */
  void reserve(long tid);

  /**
   * Return the given transaction to the store.
   *
   * upon successful return the store now controls the referenced transaction id. caller should no
   * longer interact with it.
   *
   * @param tid transaction id, previously reserved.
   * @param deferTime time in millis to keep this transaction out of the pool used in the
   *        {@link #reserve() reserve} method. must be non-negative.
   */
  void unreserve(long tid, long deferTime);

  /**
   * Get the current operation for the given transaction id.
   *
   * Caller must have already reserved tid.
   *
   * @param tid transaction id, previously reserved.
   * @return a read-only view of the operation
   */
  ReadOnlyRepo<T> top(long tid);

  /**
   * Get all operations on a transactions stack. Element 0 contains the most recent operation pushed
   * or the top.
   */
  List<ReadOnlyRepo<T>> getStack(long tid);

  /**
   * Get the state of a given transaction.
   *
   * Caller must have already reserved tid.
   *
   * @param tid transaction id, previously reserved.
   * @return execution status
   */
  TStatus getStatus(long tid);

  /**
   * Wait for the status of a transaction to change
   *
   * @param tid transaction id, need not have been reserved.
   * @param expected a set of possible statuses we are interested in being notified about. may not
   *        be null.
   * @return execution status.
   */
  TStatus waitForStatusChange(long tid, EnumSet<TStatus> expected);

  /**
   * Retrieve transaction-specific information.
   *
   * Caller must have already reserved tid.
   *
   * @param tid transaction id, previously reserved.
   * @param txInfo name of attribute of a transaction to retrieve.
   */
  Serializable getTransactionInfo(long tid, Fate.TxInfo txInfo);

  /**
   * list all transaction ids in store.
   *
   * @return all outstanding transactions, including those reserved by others.
   */
  List<Long> list();

  /**
   * Retrieve the creation time of a FaTE transaction.
   *
   * @param tid Transaction id, previously reserved.
   * @return creation time of transaction.
   */
  long timeCreated(long tid);
}

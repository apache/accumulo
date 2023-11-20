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
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.manager.PartitionData;

/**
 * Read only access to a Transaction Store.
 *
 * A transaction consists of a number of operations. Instances of this class may check on the queue
 * of outstanding transactions but may neither modify them nor create new ones.
 */
public interface ReadOnlyFatesStore<T> {

  /**
   * Possible operational status codes. Serialized by name within stores.
   */
  enum FateStatus {
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

  interface ReadOnlyFateStore<T> {
    /**
     * Get the current operation for the given transaction id.
     *
     * Caller must have already reserved tid.
     *
     * @return a read-only view of the operation
     */
    ReadOnlyRepo<T> top();

    /**
     * Get all operations on a transactions stack. Element 0 contains the most recent operation
     * pushed or the top.
     */
    List<ReadOnlyRepo<T>> getStack();

    /**
     * Get the state of a given transaction.
     *
     * Caller must have already reserved tid.
     *
     * @return execution status
     */
    FateStatus getStatus();

    /**
     * Wait for the status of a transaction to change
     *
     * @param expected a set of possible statuses we are interested in being notified about. may not
     *        be null.
     * @return execution status.
     */
    FateStatus waitForStatusChange(EnumSet<FateStatus> expected);

    /**
     * Retrieve transaction-specific information.
     *
     * Caller must have already reserved tid.
     *
     * @param txInfo name of attribute of a transaction to retrieve.
     */
    Serializable getTransactionInfo(Fate.TxInfo txInfo);

    /**
     * Retrieve the creation time of a FaTE transaction.
     *
     * @return creation time of transaction.
     */
    long timeCreated();

    long getID();
  }

  ReadOnlyFateStore<T> read(long tid);

  /**
   * list all transaction ids in store.
   *
   * @return all outstanding transactions, including those reserved by others.
   */
  List<Long> list();

  /**
   * @return an iterator over fate op ids that are (IN_PROGRESS or FAILED_IN_PROGRESS) and
   *         unreserved. Also filter the transaction using the partitioning data so that each fate
   *         instance sees a different subset of all fate transactions.
   */
  Iterator<Long> runnable(PartitionData partitionData);
}

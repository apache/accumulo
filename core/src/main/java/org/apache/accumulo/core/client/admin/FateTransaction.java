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
package org.apache.accumulo.core.client.admin;

import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.data.FateTxId;

/**
 * A single transaction of the Fault Tolerant Executor (FaTE) system. A FateTransaction is made up
 * of multiple operations. When a FateTransaction is created, a unique id is generated and stored in
 * Zookeeper, represented here has the {@link FateTxId}.
 */
public interface FateTransaction {
  enum Status {
    /** Unseeded transaction */
    NEW,
    /** Transaction that is eligible to be executed */
    SUBMITTED,
    /** Transaction that is executing */
    IN_PROGRESS,
    /** Transaction has failed, and is in the process of being rolled back */
    FAILED_IN_PROGRESS,
    /** Transaction has failed and has been fully rolled back */
    FAILED,
    /** Transaction has succeeded */
    SUCCESSFUL,
    /** Unrecognized or unknown transaction state */
    UNKNOWN
  }

  /**
   * @return This fate transaction id,
   */
  FateTxId getId();

  /**
   * @return The transaction's operation status code
   */
  Status getStatus();

  /**
   * @return The debug info for the operation on the top of the stack for this Fate operation.
   */
  String getDebug();

  /**
   * @return list of namespace and table ids locked
   */
  List<String> getHeldLocks();

  /**
   * @return list of namespace and table ids locked
   */
  List<String> getWaitingLocks();

  /**
   * @return The operation on the top of the stack for this Fate operation.
   */
  String getTop();

  /**
   * @return The timestamp of when the operation was created in ISO format with UTC timezone.
   */
  String getTimeCreatedFormatted();

  /**
   * @return The unformatted form of the timestamp.
   */
  long getTimeCreated();

  /**
   * @return The stackInfo for a transaction
   */
  String getStackInfo();

  /**
   * Fails the fate transaction.
   *
   * @since 2.1.0
   */
  void fail();

  /**
   * Deletes the fate transaction.
   *
   * @since 2.1.0
   */
  void delete() throws AccumuloException;

}

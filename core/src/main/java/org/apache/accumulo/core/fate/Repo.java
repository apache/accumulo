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

/**
 * Repeatable persisted operation
 */
public interface Repo<T> extends ReadOnlyRepo<T>, Serializable {

  /**
   * Fate.TransactionRunner.run by default will attempt to make progress on as many transactions as
   * possible by interleaving the execution of transactions. For example, when
   * {@link #isReady(long, Object)} returns a value greater than 0, it will move on to the next
   * transaction returned by ZooStore.reserve(). Also, when {@link #call(long, Object)} completes
   * and returns the next operation it does not execute it immediately. It calls ZooStore.reserve to
   * try and make progress on another transaction. When this method returns true then
   * Fate.TransactionRunner.run will process this Repo to completion instead of swapping out other
   * transactions.
   */
  default boolean interleave() {
    return true;
  }

  Repo<T> call(long tid, T environment) throws Exception;

  void undo(long tid, T environment) throws Exception;

  // this allows the last fate op to return something to the user
  String getReturn();
}

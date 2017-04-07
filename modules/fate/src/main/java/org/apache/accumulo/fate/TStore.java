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
package org.apache.accumulo.fate;

import java.io.Serializable;

/**
 * Transaction Store: a place to save transactions
 *
 * A transaction consists of a number of operations. To use, first create a transaction id, and then seed the transaction with an initial operation. An executor
 * service can then execute the transaction's operation, possibly pushing more operations onto the transaction as each step successfully completes. If a step
 * fails, the stack can be unwound, undoing each operation.
 */
public interface TStore<T> extends ReadOnlyTStore<T> {

  /**
   * Create a new transaction id
   *
   * @return a transaction id
   */
  long create();

  @Override
  Repo<T> top(long tid);

  /**
   * Update the given transaction with the next operation
   *
   * @param tid
   *          the transaction id
   * @param repo
   *          the operation
   */
  void push(long tid, Repo<T> repo) throws StackOverflowException;

  /**
   * Remove the last pushed operation from the given transaction.
   */
  void pop(long tid);

  /**
   * Update the state of a given transaction
   *
   * @param tid
   *          transaction id
   * @param status
   *          execution status
   */
  void setStatus(long tid, TStatus status);

  void setProperty(long tid, String prop, Serializable val);

  /**
   * Remove the transaction from the store.
   *
   * @param tid
   *          the transaction id
   */
  void delete(long tid);

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * FATE transaction status, including lock information.
 */
public interface TransactionStatus {

  /**
   * @return This fate operations transaction id, formatted in the same way as FATE transactions are
   *         in the Accumulo logs.
   */
  String getTxid();

  /**
   * @return This fate operations transaction id, in its original long form.
   */
  long getTxidLong();

  /**
   * @return The transaction's operation status code.
   */
  String getStatus();

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

}

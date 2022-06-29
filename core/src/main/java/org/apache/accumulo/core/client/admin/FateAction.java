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

/**
 * An administrative action to be taken against the Fault Tolerant Executor (FaTE) System. Actions
 * can only be taken against an existing {@link FateTransaction}.
 */
public enum FateAction {
  /**
   * Cancel a newly submitted transaction. Once a transaction is running (IN_PROGRESS), it cannot be
   * cancelled.
   */
  CANCEL,

  /**
   * Delete the record of the transaction from Zookeeper, including any locks.
   */
  DELETE,

  /**
   * Fails a running (IN_PROGRESS) transaction and attempts to roll back the current operation.
   */
  FAIL
}

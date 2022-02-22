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
package org.apache.accumulo.fate;

/**
 * Possible operational status codes. Serialized by name within stores.
 */
public enum FateTransactionStatus {
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

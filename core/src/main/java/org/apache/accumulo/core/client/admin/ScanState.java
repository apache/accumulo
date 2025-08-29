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

public enum ScanState {
  /**
   * Indicates no work is currently queued or running to fetch the next batch of key/values for a
   * scan. This could be because the server is waiting for a client to retrieve a batch of
   * key/values its has already fetched and is buffering.
   */
  IDLE,
  /**
   * Indicates a task is running in a server side thread pool to fetch the next batch of key/values
   * for a scan.
   */
  RUNNING,
  /**
   * Indicates a task is queued in a server side thread pool to fetch the next bach of key/values
   * for a scan.
   */
  QUEUED
}

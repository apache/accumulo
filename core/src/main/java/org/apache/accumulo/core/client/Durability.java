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
package org.apache.accumulo.core.client;

/**
 * The value for the durability of a BatchWriter or ConditionalWriter.
 *
 * @since 1.7.0
 */
public enum Durability {
  // Note, the order of these is important; the "highest" Durability is used in group commits.
  /**
   * Use the durability as specified by the table or system configuration.
   */
  DEFAULT,
  /**
   * Don't bother writing mutations to the write-ahead log.
   */
  NONE,
  /**
   * Write mutations the the write-ahead log. Data may be sitting the the servers output buffers, and not replicated anywhere.
   */
  LOG,
  /**
   * Write mutations to the write-ahead log, and ensure the data is stored on remote servers, but perhaps not on persistent storage.
   */
  FLUSH,
  /**
   * Write mutations to the write-ahead log, and ensure the data is saved to persistent storage.
   */
  SYNC
}

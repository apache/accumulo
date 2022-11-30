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
package org.apache.accumulo.core.client;

/**
 * This class enables efficient batch writing to multiple tables. When creating a batch writer for
 * each table, each has its own memory and network resources. Using this class these resources may
 * be shared among multiple tables.
 */
public interface MultiTableBatchWriter extends AutoCloseable {

  /**
   * Returns a BatchWriter for a particular table.
   *
   * @param table the name of a table whose batch writer you wish to retrieve
   * @return an instance of a batch writer for the specified table
   * @throws AccumuloException when a general exception occurs with accumulo
   * @throws AccumuloSecurityException when the user is not allowed to insert data into that table
   * @throws TableNotFoundException when the table does not exist
   */
  BatchWriter getBatchWriter(String table)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  /**
   * Send mutations for all tables to accumulo.
   *
   * @throws MutationsRejectedException when queued mutations are unable to be inserted
   */
  void flush() throws MutationsRejectedException;

  /**
   * Flush and release all resources.
   *
   * @throws MutationsRejectedException when queued mutations are unable to be inserted
   *
   */
  @Override
  void close() throws MutationsRejectedException;

  /**
   * Returns true if this batch writer has been closed.
   *
   * @return true if this batch writer has been closed
   */
  boolean isClosed();
}

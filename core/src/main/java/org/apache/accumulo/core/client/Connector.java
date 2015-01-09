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

import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Connector connects to an Accumulo instance and allows the user to request readers and writers for the instance as well as various objects that permit
 * administrative operations.
 *
 * The Connector enforces security on the client side by forcing all API calls to be accompanied by user credentials.
 */
public abstract class Connector {

  /**
   * Factory method to create a BatchScanner connected to Accumulo.
   *
   * @param tableName
   *          the name of the table to query
   * @param authorizations
   *          A set of authorization labels that will be checked against the column visibility of each key in order to filter data. The authorizations passed in
   *          must be a subset of the accumulo user's set of authorizations. If the accumulo user has authorizations (A1, A2) and authorizations (A2, A3) are
   *          passed, then an exception will be thrown.
   * @param numQueryThreads
   *          the number of concurrent threads to spawn for querying
   *
   * @return BatchScanner object for configuring and querying
   * @throws TableNotFoundException
   *           when the specified table doesn't exist
   */
  public abstract BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException;

  /**
   * Factory method to create a BatchDeleter connected to Accumulo.
   *
   * @param tableName
   *          the name of the table to query and delete from
   * @param authorizations
   *          A set of authorization labels that will be checked against the column visibility of each key in order to filter data. The authorizations passed in
   *          must be a subset of the accumulo user's set of authorizations. If the accumulo user has authorizations (A1, A2) and authorizations (A2, A3) are
   *          passed, then an exception will be thrown.
   * @param numQueryThreads
   *          the number of concurrent threads to spawn for querying
   * @param maxMemory
   *          size in bytes of the maximum memory to batch before writing
   * @param maxLatency
   *          size in milliseconds; set to 0 or Long.MAX_VALUE to allow the maximum time to hold a batch before writing
   * @param maxWriteThreads
   *          the maximum number of threads to use for writing data to the tablet servers
   *
   * @return BatchDeleter object for configuring and deleting
   * @throws TableNotFoundException
   *           when the specified table doesn't exist
   * @deprecated since 1.5.0; Use {@link #createBatchDeleter(String, Authorizations, int, BatchWriterConfig)} instead.
   */
  @Deprecated
  public abstract BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency,
      int maxWriteThreads) throws TableNotFoundException;

  /**
   *
   * @param tableName
   *          the name of the table to query and delete from
   * @param authorizations
   *          A set of authorization labels that will be checked against the column visibility of each key in order to filter data. The authorizations passed in
   *          must be a subset of the accumulo user's set of authorizations. If the accumulo user has authorizations (A1, A2) and authorizations (A2, A3) are
   *          passed, then an exception will be thrown.
   * @param numQueryThreads
   *          the number of concurrent threads to spawn for querying
   * @param config
   *          configuration used to create batch writer
   * @return BatchDeleter object for configuring and deleting
   * @since 1.5.0
   */

  public abstract BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, BatchWriterConfig config)
      throws TableNotFoundException;

  /**
   * Factory method to create a BatchWriter connected to Accumulo.
   *
   * @param tableName
   *          the name of the table to insert data into
   * @param maxMemory
   *          size in bytes of the maximum memory to batch before writing
   * @param maxLatency
   *          time in milliseconds; set to 0 or Long.MAX_VALUE to allow the maximum time to hold a batch before writing
   * @param maxWriteThreads
   *          the maximum number of threads to use for writing data to the tablet servers
   *
   * @return BatchWriter object for configuring and writing data to
   * @throws TableNotFoundException
   *           when the specified table doesn't exist
   * @deprecated since 1.5.0; Use {@link #createBatchWriter(String, BatchWriterConfig)} instead.
   */
  @Deprecated
  public abstract BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException;

  /**
   * Factory method to create a BatchWriter connected to Accumulo.
   *
   * @param tableName
   *          the name of the table to insert data into
   * @param config
   *          configuration used to create batch writer
   * @return BatchWriter object for configuring and writing data to
   * @since 1.5.0
   */

  public abstract BatchWriter createBatchWriter(String tableName, BatchWriterConfig config) throws TableNotFoundException;

  /**
   * Factory method to create a Multi-Table BatchWriter connected to Accumulo. Multi-table batch writers can queue data for multiple tables, which is good for
   * ingesting data into multiple tables from the same source
   *
   * @param maxMemory
   *          size in bytes of the maximum memory to batch before writing
   * @param maxLatency
   *          size in milliseconds; set to 0 or Long.MAX_VALUE to allow the maximum time to hold a batch before writing
   * @param maxWriteThreads
   *          the maximum number of threads to use for writing data to the tablet servers
   *
   * @return MultiTableBatchWriter object for configuring and writing data to
   * @deprecated since 1.5.0; Use {@link #createMultiTableBatchWriter(BatchWriterConfig)} instead.
   */
  @Deprecated
  public abstract MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency, int maxWriteThreads);

  /**
   * Factory method to create a Multi-Table BatchWriter connected to Accumulo. Multi-table batch writers can queue data for multiple tables. Also data for
   * multiple tables can be sent to a server in a single batch. Its an efficient way to ingest data into multiple tables from a single process.
   *
   * @param config
   *          configuration used to create multi-table batch writer
   * @return MultiTableBatchWriter object for configuring and writing data to
   * @since 1.5.0
   */

  public abstract MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config);

  /**
   * Factory method to create a Scanner connected to Accumulo.
   *
   * @param tableName
   *          the name of the table to query data from
   * @param authorizations
   *          A set of authorization labels that will be checked against the column visibility of each key in order to filter data. The authorizations passed in
   *          must be a subset of the accumulo user's set of authorizations. If the accumulo user has authorizations (A1, A2) and authorizations (A2, A3) are
   *          passed, then an exception will be thrown.
   *
   * @return Scanner object for configuring and querying data with
   * @throws TableNotFoundException
   *           when the specified table doesn't exist
   */
  public abstract Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException;

  /**
   * Factory method to create a ConditionalWriter connected to Accumulo.
   *
   * @param tableName
   *          the name of the table to query data from
   * @param config
   *          configuration used to create conditional writer
   *
   * @return ConditionalWriter object for writing ConditionalMutations
   * @throws TableNotFoundException
   *           when the specified table doesn't exist
   * @since 1.6.0
   */
  public abstract ConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config) throws TableNotFoundException;

  /**
   * Accessor method for internal instance object.
   *
   * @return the internal instance object
   */
  public abstract Instance getInstance();

  /**
   * Get the current user for this connector
   *
   * @return the user name
   */
  public abstract String whoami();

  /**
   * Retrieves a TableOperations object to perform table functions, such as create and delete.
   *
   * @return an object to manipulate tables
   */
  public abstract TableOperations tableOperations();

  /**
   * Retrieves a NamespaceOperations object to perform namespace functions, such as create and delete.
   *
   * @return an object to manipulate namespaces
   */
  public abstract NamespaceOperations namespaceOperations();

  /**
   * Retrieves a SecurityOperations object to perform user security operations, such as creating users.
   *
   * @return an object to modify users and permissions
   */
  public abstract SecurityOperations securityOperations();

  /**
   * Retrieves an InstanceOperations object to modify instance configuration.
   *
   * @return an object to modify instance configuration
   */
  public abstract InstanceOperations instanceOperations();
}

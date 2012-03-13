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
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Connector connects to an Accumulo instance and allows the user to request readers and writers for the instance as well as various objects that permit
 * administrative operations.
 * 
 * The Connector enforces security on the client side by forcing all API calls to be accompanied by user credentials.
 */
public class Connector {
  final Connector impl;
  
  /**
   * Construct a Connector from an {@link Instance}
   * 
   * @deprecated Not for client use
   * @param instance
   *          contains the precise connection information to identify the running accumulo instance
   * @param user
   *          a valid accumulo user
   * @param password
   *          the password for the user
   * @throws AccumuloException
   *           when a generic exception occurs
   * @throws AccumuloSecurityException
   *           when a user's credentials are invalid
   * @see Instance#getConnector(String user, byte[] password)
   */
  public Connector(Instance instance, String user, byte[] password) throws AccumuloException, AccumuloSecurityException {
    impl = instance.getConnector(user, password);
  }
  
  /**
   * @see Instance#getConnector(String user, byte[] password)
   * @deprecated Not for client use
   */
  public Connector() {
    impl = null;
  }
  
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
  public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
    return impl.createBatchScanner(tableName, authorizations, numQueryThreads);
  }
  
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
   */
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency,
      int maxWriteThreads) throws TableNotFoundException {
    return impl.createBatchDeleter(tableName, authorizations, numQueryThreads, maxMemory, maxLatency, maxWriteThreads);
  }
  
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
   */
  public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
    return impl.createBatchWriter(tableName, maxMemory, maxLatency, maxWriteThreads);
  }
  
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
   */
  public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency, int maxWriteThreads) {
    return impl.createMultiTableBatchWriter(maxMemory, maxLatency, maxWriteThreads);
  }
  
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
  public Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
    return impl.createScanner(tableName, authorizations);
  }
  
  /**
   * Accessor method for internal instance object.
   * 
   * @return the internal instance object
   */
  public Instance getInstance() {
    return impl.getInstance();
  }
  
  /**
   * Get the current user for this connector
   * 
   * @return the user name
   */
  public String whoami() {
    return impl.whoami();
  }
  
  /**
   * Retrieves a TableOperations object to perform table functions, such as create and delete.
   * 
   * @return an object to manipulate tables
   */
  public synchronized TableOperations tableOperations() {
    return impl.tableOperations();
  }
  
  /**
   * Retrieves a SecurityOperations object to perform user security operations, such as creating users.
   * 
   * @return an object to modify users and permissions
   */
  public synchronized SecurityOperations securityOperations() {
    return impl.securityOperations();
  }
  
  /**
   * Retrieves an InstanceOperations object to modify instance configuration.
   * 
   * @return an object to modify instance configuration
   */
  public synchronized InstanceOperations instanceOperations() {
    return impl.instanceOperations();
  }
}

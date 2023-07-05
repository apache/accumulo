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

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URL;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Client connection to an Accumulo instance. Allows the user to request a scanner, deleter or
 * writer for the instance as well as various objects that permit administrative operations.
 * Enforces security on the client side by requiring user credentials.
 *
 * <p>
 * Supports fluent API for creation. Various options can be provided to {@link Accumulo#newClient()}
 * and when finished a call to build() will return the AccumuloClient object. For example:
 *
 * <pre>
 * <code>
 * try (AccumuloClient client = Accumulo.newClient()
 *        .to(instanceName, zookeepers)
 *        .as(user, password).build())
 * {
 *   // use the client
 * }
 * </code>
 * </pre>
 *
 * <p>
 * An important difference with the legacy Connector to consider is that Connector reused global
 * static resources. AccumuloClient, however, attempts to clean up its resources on close. So,
 * creating many AccumuloClient objects will perform worse than creating many Connectors did.
 * Therefore, it is suggested to reuse AccumuloClient instances where possible, rather than create
 * many of them.
 *
 * <p>
 * AccumuloClient objects are intended to be thread-safe, and can be used by multiple threads.
 * However, care should be taken to ensure that the client is eventually closed, to clean up any
 * resources in use in the client application when all threads are finished with the AccumuloClient
 * object. Additionally, while the client itself is thread-safe, it is not necessarily true that all
 * objects produced from the client (such as Scanners) are thread-safe.
 *
 * @since 2.0.0
 * @see <a href="https://accumulo.apache.org/docs/2.x/getting-started/clients">Accumulo Client
 *      Documentation</a>
 */
public interface AccumuloClient extends AutoCloseable {

  /**
   * Factory method to create a BatchScanner connected to Accumulo.
   *
   * @param tableName the name of the table to query
   * @param authorizations A set of authorization labels that will be checked against the column
   *        visibility of each key in order to filter data. The authorizations passed in must be a
   *        subset of the accumulo user's set of authorizations. If the accumulo user has
   *        authorizations (A1, A2) and authorizations (A2, A3) are passed, then an exception will
   *        be thrown.
   * @param numQueryThreads the number of concurrent threads to spawn for querying
   *
   * @return BatchScanner object for configuring and querying
   * @throws TableNotFoundException when the specified table doesn't exist
   */
  BatchScanner createBatchScanner(String tableName, Authorizations authorizations,
      int numQueryThreads) throws TableNotFoundException;

  /**
   * Factory method to create a BatchScanner connected to Accumulo. This method uses the number of
   * query threads configured when AccumuloClient was created. If none were configured, defaults
   * will be used.
   *
   * @param tableName the name of the table to query
   * @param authorizations A set of authorization labels that will be checked against the column
   *        visibility of each key in order to filter data. The authorizations passed in must be a
   *        subset of the accumulo user's set of authorizations. If the accumulo user has
   *        authorizations (A1, A2) and authorizations (A2, A3) are passed, then an exception will
   *        be thrown.
   *
   * @return BatchScanner object for configuring and querying
   * @throws TableNotFoundException when the specified table doesn't exist
   */
  BatchScanner createBatchScanner(String tableName, Authorizations authorizations)
      throws TableNotFoundException;

  /**
   * Factory method to create a BatchScanner with all of user's authorizations and the number of
   * query threads configured when AccumuloClient was created. If no query threads were configured,
   * defaults will be used.
   *
   * @param tableName the name of the table to query
   *
   * @return BatchScanner object for configuring and querying
   * @throws TableNotFoundException when the specified table doesn't exist
   */
  BatchScanner createBatchScanner(String tableName)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException;

  /**
   * Factory method to create BatchDeleter
   *
   * @param tableName the name of the table to query and delete from
   * @param authorizations A set of authorization labels that will be checked against the column
   *        visibility of each key in order to filter data. The authorizations passed in must be a
   *        subset of the accumulo user's set of authorizations. If the accumulo user has
   *        authorizations (A1, A2) and authorizations (A2, A3) are passed, then an exception will
   *        be thrown.
   * @param numQueryThreads the number of concurrent threads to spawn for querying
   * @param config configuration used to create batch writer. This config takes precedence. Any
   *        unset values will be merged with config set when the AccumuloClient was created. If no
   *        config was set during AccumuloClient creation, BatchWriterConfig defaults will be used.
   * @return BatchDeleter object for configuring and deleting
   */

  BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads, BatchWriterConfig config) throws TableNotFoundException;

  /**
   * Factory method to create BatchDeleter. This method uses BatchWriterConfig set when
   * AccumuloClient was created. If none was set, BatchWriterConfig defaults will be used.
   *
   * @param tableName the name of the table to query and delete from
   * @param authorizations A set of authorization labels that will be checked against the column
   *        visibility of each key in order to filter data. The authorizations passed in must be a
   *        subset of the accumulo user's set of authorizations. If the accumulo user has
   *        authorizations (A1, A2) and authorizations (A2, A3) are passed, then an exception will
   *        be thrown.
   * @param numQueryThreads the number of concurrent threads to spawn for querying
   * @return BatchDeleter object
   * @throws TableNotFoundException if table not found
   */
  BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads) throws TableNotFoundException;

  /**
   * Factory method to create a BatchWriter connected to Accumulo.
   *
   * @param tableName the name of the table to insert data into
   * @param config configuration used to create batch writer. This config will take precedence. Any
   *        unset values will be merged with the config set when the AccumuloClient was created. If
   *        no config was set during AccumuloClient creation, BatchWriterConfig defaults will be
   *        used.
   * @return BatchWriter object for configuring and writing data to
   */
  BatchWriter createBatchWriter(String tableName, BatchWriterConfig config)
      throws TableNotFoundException;

  /**
   * Factory method to create a BatchWriter. This method uses BatchWriterConfig set when
   * AccumuloClient was created. If none was set, BatchWriterConfig defaults will be used.
   *
   * @param tableName the name of the table to insert data into
   * @return BatchWriter object
   * @throws TableNotFoundException if table not found
   */
  BatchWriter createBatchWriter(String tableName) throws TableNotFoundException;

  /**
   * Factory method to create a Multi-Table BatchWriter connected to Accumulo. Multi-table batch
   * writers can queue data for multiple tables. Also data for multiple tables can be sent to a
   * server in a single batch. It's an efficient way to ingest data into multiple tables from a
   * single process.
   *
   * @param config configuration used to create multi-table batch writer. This config will take
   *        precedence. Any unset values will be merged with the config set when the AccumuloClient
   *        was created. If no config was set during AccumuloClient creation, BatchWriterConfig
   *        defaults will be used.
   * @return MultiTableBatchWriter object for configuring and writing data to
   */
  MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config);

  /**
   * Factory method to create a Multi-Table BatchWriter. This method uses BatchWriterConfig set when
   * AccumuloClient was created. If none was set, BatchWriterConfig defaults will be used.
   *
   * @return MultiTableBatchWriter object
   */
  MultiTableBatchWriter createMultiTableBatchWriter();

  /**
   * Factory method to create a Scanner connected to Accumulo.
   *
   * @param tableName the name of the table to query data from
   * @param authorizations A set of authorization labels that will be checked against the column
   *        visibility of each key in order to filter data. The authorizations passed in must be a
   *        subset of the accumulo user's set of authorizations. If the accumulo user has
   *        authorizations (A1, A2) and authorizations (A2, A3) are passed, then an exception will
   *        be thrown.
   *
   * @return Scanner object for configuring and querying data with
   * @throws TableNotFoundException when the specified table doesn't exist
   *
   * @see IsolatedScanner
   */
  Scanner createScanner(String tableName, Authorizations authorizations)
      throws TableNotFoundException;

  /**
   * Factory method to create a Scanner with all of the user's authorizations.
   *
   * @param tableName the name of the table to query data from
   *
   * @return Scanner object for configuring and querying data with
   * @throws TableNotFoundException when the specified table doesn't exist
   *
   * @see IsolatedScanner
   */
  Scanner createScanner(String tableName)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException;

  /**
   * Factory method to create a ConditionalWriter connected to Accumulo.
   *
   * @param tableName the name of the table to query data from
   * @param config configuration used to create conditional writer
   *
   * @return ConditionalWriter object for writing ConditionalMutations
   * @throws TableNotFoundException when the specified table doesn't exist
   */
  ConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config)
      throws TableNotFoundException;

  /**
   * Factory method to create a ConditionalWriter connected to Accumulo.
   *
   * @param tableName the name of the table to query data from
   *
   * @return ConditionalWriter object for writing ConditionalMutations
   * @throws TableNotFoundException when the specified table doesn't exist
   *
   * @since 2.1.0
   */
  ConditionalWriter createConditionalWriter(String tableName) throws TableNotFoundException;

  /**
   * Get the current user for this AccumuloClient
   *
   * @return the user name
   */
  String whoami();

  /**
   * Retrieves a TableOperations object to perform table functions, such as create and delete.
   *
   * @return an object to manipulate tables
   */
  TableOperations tableOperations();

  /**
   * Retrieves a NamespaceOperations object to perform namespace functions, such as create and
   * delete.
   *
   * @return an object to manipulate namespaces
   */
  NamespaceOperations namespaceOperations();

  /**
   * Retrieves a SecurityOperations object to perform user security operations, such as creating
   * users.
   *
   * @return an object to modify users and permissions
   */
  SecurityOperations securityOperations();

  /**
   * Retrieves an InstanceOperations object to modify instance configuration.
   *
   * @return an object to modify instance configuration
   */
  InstanceOperations instanceOperations();

  /**
   * @return All {@link Properties} used to create client except 'auth.token'
   */
  Properties properties();

  /**
   * Cleans up any resources created by an AccumuloClient like threads and sockets. Anything created
   * from this client will likely not work after calling this method. For example a Scanner created
   * using this client will likely fail after close is called.
   */
  @Override
  void close();

  /**
   * Builds AccumuloClient or client Properties after all options have been specified
   *
   * @since 2.0.0
   */
  interface ClientFactory<T> {

    /**
     * Override default handling of uncaught exceptions in client threads
     *
     * @param ueh UncaughtExceptionHandler implementation
     * @return AccumuloClient or Properties
     * @since 2.1.0
     */
    ClientFactory<T> withUncaughtExceptionHandler(UncaughtExceptionHandler ueh);

    /**
     * Builds AccumuloClient or client Properties
     *
     * @return AccumuloClient or Properties
     */
    T build();
  }

  /**
   * Builder method for setting Accumulo instance and zookeepers
   *
   * @since 2.0.0
   */
  interface InstanceArgs<T> {
    AuthenticationArgs<T> to(CharSequence instanceName, CharSequence zookeepers);
  }

  /**
   * Builder methods for creating AccumuloClient using properties
   *
   * @since 2.0.0
   */
  interface PropertyOptions<T> extends InstanceArgs<T> {

    /**
     * Build using properties file. An example properties file can be found at
     * conf/accumulo-client.properties in the Accumulo tarball distribution.
     *
     * @param propertiesFilePath Path to properties file
     * @return this builder
     * @see <a href="https://accumulo.apache.org/docs/2.x/configuration/client-properties">Client
     *      properties documentation</a>
     */
    FromOptions<T> from(String propertiesFilePath);

    /**
     * Build using properties file. An example properties file can be found at
     * conf/accumulo-client.properties in the Accumulo tarball distribution.
     *
     * @param propertiesFile Path to properties file
     * @return this builder
     * @see <a href="https://accumulo.apache.org/docs/2.x/configuration/client-properties">Client
     *      properties documentation</a>
     */
    FromOptions<T> from(Path propertiesFile);

    /**
     * Build using Java properties object. An example properties file can be found at
     * conf/accumulo-client.properties in the Accumulo tarball distribution.
     *
     * @param propertiesURL URL path to properties file
     * @return this builder
     * @see <a href="https://accumulo.apache.org/docs/2.x/configuration/client-properties">Client
     *      properties documentation</a>
     */
    FromOptions<T> from(URL propertiesURL);

    /**
     * Build using Java properties object. An example properties file can be found at
     * conf/accumulo-client.properties in the Accumulo tarball distribution.
     *
     * @param properties Properties object
     * @return this builder
     * @see <a href="https://accumulo.apache.org/docs/2.x/configuration/client-properties">Client
     *      properties documentation</a>
     */
    FromOptions<T> from(Properties properties);
  }

  /**
   * Builder methods for authentication
   *
   * @since 2.0.0
   */
  interface AuthenticationArgs<T> {

    /**
     * Build using password-based credentials
     *
     * @param username User name
     * @param password Password
     * @return this builder
     */
    ConnectionOptions<T> as(CharSequence username, CharSequence password);

    /**
     * Build using Kerberos credentials
     *
     * @param principal Principal
     * @param keyTabFile Path to keytab file
     * @return this builder
     */
    ConnectionOptions<T> as(CharSequence principal, Path keyTabFile);

    /**
     * Build using specified credentials
     *
     * @param principal Principal/username
     * @param token Authentication token
     * @return this builder
     */
    ConnectionOptions<T> as(CharSequence principal, AuthenticationToken token);
  }

  /**
   * Build methods for SSL/TLS
   *
   * @since 2.0.0
   */
  interface SslOptions<T> extends ClientFactory<T> {

    /**
     * Build with SSL trust store
     *
     * @param path Path to trust store
     * @return this builder
     */
    SslOptions<T> truststore(CharSequence path);

    /**
     * Build with SSL trust store
     *
     * @param path Path to trust store
     * @param password Password used to encrypt trust store
     * @param type Trust store type
     * @return this builder
     */
    SslOptions<T> truststore(CharSequence path, CharSequence password, CharSequence type);

    /**
     * Build with SSL key store
     *
     * @param path Path to SSL key store
     * @return this builder
     */
    SslOptions<T> keystore(CharSequence path);

    /**
     * Build with SSL key store
     *
     * @param path Path to keystore
     * @param password Password used to encrypt key store
     * @param type Key store type
     * @return this builder
     */
    SslOptions<T> keystore(CharSequence path, CharSequence password, CharSequence type);

    /**
     * Use JSSE system properties to configure SSL
     *
     * @return this builder
     */
    SslOptions<T> useJsse();
  }

  /**
   * Build methods for SASL
   *
   * @since 2.0.0
   */
  interface SaslOptions<T> extends ClientFactory<T> {

    /**
     * Build with Kerberos Server Primary
     *
     * @param kerberosServerPrimary Kerberos server primary
     * @return this builder
     */
    SaslOptions<T> primary(CharSequence kerberosServerPrimary);

    /**
     * Build with SASL quality of protection
     *
     * @param qualityOfProtection Quality of protection
     * @return this builder
     */
    SaslOptions<T> qop(CharSequence qualityOfProtection);
  }

  /**
   * Build methods for connection options
   *
   * @since 2.0.0
   */
  interface ConnectionOptions<T> extends ClientFactory<T> {

    /**
     * Build using Zookeeper timeout
     *
     * @param timeout Zookeeper timeout (in milliseconds)
     * @return this builder
     */
    ConnectionOptions<T> zkTimeout(int timeout);

    /**
     * Build with SSL/TLS options
     *
     * @return this builder
     */
    SslOptions<T> useSsl();

    /**
     * Build with SASL options
     *
     * @return this builder
     */
    SaslOptions<T> useSasl();

    /**
     * Build with BatchWriterConfig defaults for BatchWriter, MultiTableBatchWriter &amp;
     * BatchDeleter
     *
     * @param batchWriterConfig BatchWriterConfig
     * @return this builder
     */
    ConnectionOptions<T> batchWriterConfig(BatchWriterConfig batchWriterConfig);

    /**
     * Build with default number of query threads for BatchScanner
     */
    ConnectionOptions<T> batchScannerQueryThreads(int numQueryThreads);

    /**
     * Build with default batch size for Scanner
     */
    ConnectionOptions<T> scannerBatchSize(int batchSize);
  }

  /**
   * @since 2.0.0
   */
  interface FromOptions<T> extends ConnectionOptions<T>, AuthenticationArgs<T> {

  }
}

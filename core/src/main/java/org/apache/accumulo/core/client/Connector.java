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

import java.util.Properties;

import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.ReplicationOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ConnectorImpl;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Connector connects to an Accumulo instance and allows the user to request readers and writers for
 * the instance as well as various objects that permit administrative operations.
 *
 * The Connector enforces security on the client side by forcing all API calls to be accompanied by
 * user credentials.
 */
public abstract class Connector {

  /**
   * Factory method to create a BatchScanner connected to Accumulo.
   *
   * @param tableName
   *          the name of the table to query
   * @param authorizations
   *          A set of authorization labels that will be checked against the column visibility of
   *          each key in order to filter data. The authorizations passed in must be a subset of the
   *          accumulo user's set of authorizations. If the accumulo user has authorizations (A1,
   *          A2) and authorizations (A2, A3) are passed, then an exception will be thrown.
   * @param numQueryThreads
   *          the number of concurrent threads to spawn for querying
   *
   * @return BatchScanner object for configuring and querying
   * @throws TableNotFoundException
   *           when the specified table doesn't exist
   */
  public abstract BatchScanner createBatchScanner(String tableName, Authorizations authorizations,
      int numQueryThreads) throws TableNotFoundException;

  /**
   * Factory method to create a BatchDeleter connected to Accumulo.
   *
   * @param tableName
   *          the name of the table to query and delete from
   * @param authorizations
   *          A set of authorization labels that will be checked against the column visibility of
   *          each key in order to filter data. The authorizations passed in must be a subset of the
   *          accumulo user's set of authorizations. If the accumulo user has authorizations (A1,
   *          A2) and authorizations (A2, A3) are passed, then an exception will be thrown.
   * @param numQueryThreads
   *          the number of concurrent threads to spawn for querying
   * @param maxMemory
   *          size in bytes of the maximum memory to batch before writing
   * @param maxLatency
   *          size in milliseconds; set to 0 or Long.MAX_VALUE to allow the maximum time to hold a
   *          batch before writing
   * @param maxWriteThreads
   *          the maximum number of threads to use for writing data to the tablet servers
   *
   * @return BatchDeleter object for configuring and deleting
   * @throws TableNotFoundException
   *           when the specified table doesn't exist
   * @deprecated since 1.5.0; Use
   *             {@link #createBatchDeleter(String, Authorizations, int, BatchWriterConfig)}
   *             instead.
   */
  @Deprecated
  public abstract BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads, long maxMemory, long maxLatency, int maxWriteThreads)
      throws TableNotFoundException;

  /**
   * Factory method to create BatchDeleter
   *
   * @param tableName
   *          the name of the table to query and delete from
   * @param authorizations
   *          A set of authorization labels that will be checked against the column visibility of
   *          each key in order to filter data. The authorizations passed in must be a subset of the
   *          accumulo user's set of authorizations. If the accumulo user has authorizations (A1,
   *          A2) and authorizations (A2, A3) are passed, then an exception will be thrown.
   * @param numQueryThreads
   *          the number of concurrent threads to spawn for querying
   * @param config
   *          configuration used to create batch writer. This config takes precedence. Any unset
   *          values will be merged with config set when the Connector was created. If no config was
   *          set during Connector creation, BatchWriterConfig defaults will be used.
   * @return BatchDeleter object for configuring and deleting
   * @since 1.5.0
   */

  public abstract BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads, BatchWriterConfig config) throws TableNotFoundException;

  /**
   * Factory method to create BatchDeleter. This method uses BatchWriterConfig set when Connector
   * was created. If none was set, BatchWriterConfig defaults will be used.
   *
   * @param tableName
   *          the name of the table to query and delete from
   * @param authorizations
   *          A set of authorization labels that will be checked against the column visibility of
   *          each key in order to filter data. The authorizations passed in must be a subset of the
   *          accumulo user's set of authorizations. If the accumulo user has authorizations (A1,
   *          A2) and authorizations (A2, A3) are passed, then an exception will be thrown.
   * @param numQueryThreads
   *          the number of concurrent threads to spawn for querying
   * @return BatchDeleter object
   * @throws TableNotFoundException
   *           if table not found
   */
  public abstract BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations,
      int numQueryThreads) throws TableNotFoundException;

  /**
   * Factory method to create a BatchWriter connected to Accumulo.
   *
   * @param tableName
   *          the name of the table to insert data into
   * @param maxMemory
   *          size in bytes of the maximum memory to batch before writing
   * @param maxLatency
   *          time in milliseconds; set to 0 or Long.MAX_VALUE to allow the maximum time to hold a
   *          batch before writing
   * @param maxWriteThreads
   *          the maximum number of threads to use for writing data to the tablet servers
   *
   * @return BatchWriter object for configuring and writing data to
   * @throws TableNotFoundException
   *           when the specified table doesn't exist
   * @deprecated since 1.5.0; Use {@link #createBatchWriter(String, BatchWriterConfig)} instead.
   */
  @Deprecated
  public abstract BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency,
      int maxWriteThreads) throws TableNotFoundException;

  /**
   * Factory method to create a BatchWriter connected to Accumulo.
   *
   * @param tableName
   *          the name of the table to insert data into
   * @param config
   *          configuration used to create batch writer. This config will take precedence. Any unset
   *          values will merged with config set when the Connector was created. If no config was
   *          set during Connector creation, BatchWriterConfig defaults will be used.
   * @return BatchWriter object for configuring and writing data to
   * @since 1.5.0
   */

  public abstract BatchWriter createBatchWriter(String tableName, BatchWriterConfig config)
      throws TableNotFoundException;

  /**
   * Factory method to create a BatchWriter. This method uses BatchWriterConfig set when Connector
   * was created. If none was set, BatchWriterConfig defaults will be used.
   *
   * @param tableName
   *          the name of the table to insert data into
   * @return BatchWriter object
   * @throws TableNotFoundException
   *           if table not found
   * @since 2.0.0
   */
  public abstract BatchWriter createBatchWriter(String tableName) throws TableNotFoundException;

  /**
   * Factory method to create a Multi-Table BatchWriter connected to Accumulo. Multi-table batch
   * writers can queue data for multiple tables, which is good for ingesting data into multiple
   * tables from the same source
   *
   * @param maxMemory
   *          size in bytes of the maximum memory to batch before writing
   * @param maxLatency
   *          size in milliseconds; set to 0 or Long.MAX_VALUE to allow the maximum time to hold a
   *          batch before writing
   * @param maxWriteThreads
   *          the maximum number of threads to use for writing data to the tablet servers
   *
   * @return MultiTableBatchWriter object for configuring and writing data to
   * @deprecated since 1.5.0; Use {@link #createMultiTableBatchWriter(BatchWriterConfig)} instead.
   */
  @Deprecated
  public abstract MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency,
      int maxWriteThreads);

  /**
   * Factory method to create a Multi-Table BatchWriter connected to Accumulo. Multi-table batch
   * writers can queue data for multiple tables. Also data for multiple tables can be sent to a
   * server in a single batch. Its an efficient way to ingest data into multiple tables from a
   * single process.
   *
   * @param config
   *          configuration used to create multi-table batch writer. This config will take
   *          precedence. Any unset values will merged with config set when the Connector was
   *          created. If no config was set during Connector creation, BatchWriterConfig defaults
   *          will be used.
   * @return MultiTableBatchWriter object for configuring and writing data to
   * @since 1.5.0
   */
  public abstract MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config);

  /**
   * Factory method to create a Multi-Table BatchWriter. This method uses BatchWriterConfig set when
   * Connector was created. If none was set, BatchWriterConfig defaults will be used.
   *
   * @return MultiTableBatchWriter object
   * @since 2.0.0
   */
  public abstract MultiTableBatchWriter createMultiTableBatchWriter();

  /**
   * Factory method to create a Scanner connected to Accumulo.
   *
   * @param tableName
   *          the name of the table to query data from
   * @param authorizations
   *          A set of authorization labels that will be checked against the column visibility of
   *          each key in order to filter data. The authorizations passed in must be a subset of the
   *          accumulo user's set of authorizations. If the accumulo user has authorizations (A1,
   *          A2) and authorizations (A2, A3) are passed, then an exception will be thrown.
   *
   * @return Scanner object for configuring and querying data with
   * @throws TableNotFoundException
   *           when the specified table doesn't exist
   */
  public abstract Scanner createScanner(String tableName, Authorizations authorizations)
      throws TableNotFoundException;

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
  public abstract ConditionalWriter createConditionalWriter(String tableName,
      ConditionalWriterConfig config) throws TableNotFoundException;

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
   * Retrieves a NamespaceOperations object to perform namespace functions, such as create and
   * delete.
   *
   * @return an object to manipulate namespaces
   */
  public abstract NamespaceOperations namespaceOperations();

  /**
   * Retrieves a SecurityOperations object to perform user security operations, such as creating
   * users.
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

  /**
   * Retrieves a ReplicationOperations object to manage replication configuration.
   *
   * @return an object to modify replication configuration
   * @since 1.7.0
   */
  public abstract ReplicationOperations replicationOperations();

  /**
   * @return {@link ConnectionInfo} which contains information about Connection to Accumulo
   * @since 2.0.0
   */
  public abstract ConnectionInfo info();

  /**
   * Builds ConnectionInfo after all options have been specified
   *
   * @since 2.0.0
   */
  public interface ConnInfoFactory {

    /**
     * Builds ConnectionInfo after all options have been specified
     *
     * @return ConnectionInfo
     */
    ConnectionInfo info();
  }

  /**
   * Builds Connector
   *
   * @since 2.0.0
   */
  public interface ConnectorFactory extends ConnInfoFactory {

    /**
     * Builds Connector after all options have been specified
     *
     * @return Connector
     */
    Connector build() throws AccumuloException, AccumuloSecurityException;

  }

  /**
   * Builder method for setting Accumulo instance and zookeepers
   *
   * @since 2.0.0
   */
  public interface InstanceArgs {
    AuthenticationArgs forInstance(String instanceName, String zookeepers);
  }

  /**
   * Builder methods for creating Connector using properties
   *
   * @since 2.0.0
   */
  public interface PropertyOptions extends InstanceArgs {

    /**
     * Build using properties file. An example properties file can be found at
     * conf/accumulo-client.properties in the Accumulo tarball distribution.
     *
     * @param propertiesFile
     *          Path to properties file
     * @return this builder
     */
    ConnectorFactory usingProperties(String propertiesFile);

    /**
     * Build using Java properties object. A list of available properties can be found in the
     * documentation on the project website (http://accumulo.apache.org) under 'Development' -&gt;
     * 'Client Properties'
     *
     * @param properties
     *          Properties object
     * @return this builder
     */
    ConnectorFactory usingProperties(Properties properties);
  }

  public interface ConnectionInfoOptions extends PropertyOptions {

    /**
     * Build using connection information
     *
     * @param connectionInfo
     *          ConnectionInfo object
     * @return this builder
     */
    FromOptions usingConnectionInfo(ConnectionInfo connectionInfo);
  }

  /**
   * Build methods for authentication
   *
   * @since 2.0.0
   */
  public interface AuthenticationArgs {

    /**
     * Build using password-based credentials
     *
     * @param username
     *          User name
     * @param password
     *          Password
     * @return this builder
     */
    ConnectionOptions usingPassword(String username, CharSequence password);

    /**
     * Build using Kerberos credentials
     *
     * @param principal
     *          Principal
     * @param keyTabFile
     *          Path to keytab file
     * @return this builder
     */
    ConnectionOptions usingKerberos(String principal, String keyTabFile);

    /**
     * Build using credentials from a CredentialProvider
     *
     * @param username
     *          Accumulo user name
     * @param name
     *          Alias to extract Accumulo user password from CredentialProvider
     * @param providerUrls
     *          Comma seperated list of URLs defining CredentialProvider(s)
     * @return this builder
     */
    ConnectionOptions usingProvider(String username, String name, String providerUrls);

    /**
     * Build using specified credentials
     *
     * @param principal
     *          Principal/username
     * @param token
     *          Authentication token
     * @return this builder
     */
    ConnectionOptions usingToken(String principal, AuthenticationToken token);
  }

  /**
   * Build methods for SSL/TLS
   *
   * @since 2.0.0
   */
  public interface SslOptions extends ConnectorFactory {

    /**
     * Build with SSL trust store
     *
     * @param path
     *          Path to trust store
     * @return this builder
     */
    SslOptions withTruststore(String path);

    /**
     * Build with SSL trust store
     *
     * @param path
     *          Path to trust store
     * @param password
     *          Password used to encrypt trust store
     * @param type
     *          Trust store type
     * @return this builder
     */
    SslOptions withTruststore(String path, String password, String type);

    /**
     * Build with SSL key store
     *
     * @param path
     *          Path to SSL key store
     * @return this builder
     */
    SslOptions withKeystore(String path);

    /**
     * Build with SSL key store
     *
     * @param path
     *          Path to keystore
     * @param password
     *          Password used to encyrpt key store
     * @param type
     *          Key store type
     * @return this builder
     */
    SslOptions withKeystore(String path, String password, String type);

    /**
     * Use JSSE system properties to configure SSL
     *
     * @return this builder
     */
    SslOptions useJsse();
  }

  /**
   * Build methods for SASL
   *
   * @since 2.0.0
   */
  public interface SaslOptions extends ConnectorFactory {

    /**
     * Build with Kerberos Server Primary
     *
     * @param kerberosServerPrimary
     *          Kerberos server primary
     * @return this builder
     */
    SaslOptions withPrimary(String kerberosServerPrimary);

    /**
     * Build with SASL quality of protection
     *
     * @param qualityOfProtection
     *          Quality of protection
     * @return this builder
     */
    SaslOptions withQop(String qualityOfProtection);
  }

  /**
   * Build methods for connection options
   *
   * @since 2.0.0
   */
  public interface ConnectionOptions extends ConnectorFactory {

    /**
     * Build using Zookeeper timeout
     *
     * @param timeout
     *          Zookeeper timeout
     * @return this builder
     */
    ConnectionOptions withZkTimeout(int timeout);

    /**
     * Build with SSL/TLS options
     *
     * @return this builder
     */
    SslOptions withSsl();

    /**
     * Build with SASL options
     *
     * @return this builder
     */
    SaslOptions withSasl();

    /**
     * Build with BatchWriterConfig defaults for BatchWriter, MultiTableBatchWriter &amp;
     * BatchDeleter
     *
     * @param batchWriterConfig
     *          BatchWriterConfig
     * @return this builder
     */
    ConnectionOptions withBatchWriterConfig(BatchWriterConfig batchWriterConfig);
  }

  public interface FromOptions extends ConnectionOptions, PropertyOptions, AuthenticationArgs {

  }

  /**
   * Creates builder for Connector
   *
   * @return this builder
   * @since 2.0.0
   */
  public static ConnectionInfoOptions builder() {
    return new ConnectorImpl.ConnectorBuilderImpl();
  }
}

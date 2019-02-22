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
package org.apache.accumulo.core.client.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.AuthenticationTokenSerializer;
import org.apache.accumulo.core.client.security.tokens.DelegationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.DelegationTokenImpl;
import org.apache.accumulo.core.clientImpl.mapreduce.lib.OutputConfigurator;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This class allows MapReduce jobs to use Accumulo as the sink for data. This {@link OutputFormat}
 * accepts keys and values of type {@link Text} (for a table name) and {@link Mutation} from the Map
 * and Reduce functions.
 *
 * The user must specify the following via static configurator methods:
 *
 * <ul>
 * <li>{@link AccumuloOutputFormat#setConnectorInfo(JobConf, String, AuthenticationToken)}
 * <li>{@link AccumuloOutputFormat#setConnectorInfo(JobConf, String, String)}
 * <li>{@link AccumuloOutputFormat#setZooKeeperInstance(JobConf, org.apache.accumulo.core.client.ClientConfiguration)}
 * </ul>
 *
 * Other static methods are optional.
 *
 * @deprecated since 2.0.0; Use org.apache.accumulo.hadoop.mapred instead from the
 *             accumulo-hadoop-mapreduce.jar
 */
@Deprecated
public class AccumuloOutputFormat implements OutputFormat<Text,Mutation> {

  private static final Class<?> CLASS = AccumuloOutputFormat.class;
  protected static final Logger log = Logger.getLogger(CLASS);

  /**
   * Sets the connector information needed to communicate with Accumulo in this job.
   *
   * <p>
   * <b>WARNING:</b> Some tokens, when serialized, divulge sensitive information in the
   * configuration as a means to pass the token to MapReduce tasks. This information is BASE64
   * encoded to provide a charset safe conversion to a string, but this conversion is not intended
   * to be secure. {@link PasswordToken} is one example that is insecure in this way; however
   * {@link DelegationToken}s, acquired using
   * {@link SecurityOperations#getDelegationToken(DelegationTokenConfig)}, is not subject to this
   * concern.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param principal
   *          a valid Accumulo user name (user must have Table.CREATE permission if
   *          {@link #setCreateTables(JobConf, boolean)} is set to true)
   * @param token
   *          the user's password
   * @since 1.5.0
   */
  public static void setConnectorInfo(JobConf job, String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    if (token instanceof KerberosToken) {
      log.info("Received KerberosToken, attempting to fetch DelegationToken");
      try {
        ClientContext client = OutputConfigurator.client(CLASS, job);
        token = client.securityOperations().getDelegationToken(new DelegationTokenConfig());
      } catch (Exception e) {
        log.warn("Failed to automatically obtain DelegationToken, "
            + "Mappers/Reducers will likely fail to communicate with Accumulo", e);
      }
    }
    // DelegationTokens can be passed securely from user to task without serializing insecurely in
    // the configuration
    if (token instanceof DelegationTokenImpl) {
      DelegationTokenImpl delegationToken = (DelegationTokenImpl) token;

      // Convert it into a Hadoop Token
      AuthenticationTokenIdentifier identifier = delegationToken.getIdentifier();
      Token<AuthenticationTokenIdentifier> hadoopToken = new Token<>(identifier.getBytes(),
          delegationToken.getPassword(), identifier.getKind(), delegationToken.getServiceName());

      // Add the Hadoop Token to the Job so it gets serialized and passed along.
      job.getCredentials().addToken(hadoopToken.getService(), hadoopToken);
    }

    OutputConfigurator.setConnectorInfo(CLASS, job, principal, token);
  }

  /**
   * Sets the connector information needed to communicate with Accumulo in this job.
   *
   * <p>
   * Stores the password in a file in HDFS and pulls that into the Distributed Cache in an attempt
   * to be more secure than storing it in the Configuration.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param principal
   *          a valid Accumulo user name (user must have Table.CREATE permission if
   *          {@link #setCreateTables(JobConf, boolean)} is set to true)
   * @param tokenFile
   *          the path to the password file
   * @since 1.6.0
   */
  public static void setConnectorInfo(JobConf job, String principal, String tokenFile)
      throws AccumuloSecurityException {
    OutputConfigurator.setConnectorInfo(CLASS, job, principal, tokenFile);
  }

  /**
   * Determines if the connector has been configured.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return true if the connector has been configured, false otherwise
   * @since 1.5.0
   * @see #setConnectorInfo(JobConf, String, AuthenticationToken)
   */
  protected static Boolean isConnectorInfoSet(JobConf job) {
    return OutputConfigurator.isConnectorInfoSet(CLASS, job);
  }

  /**
   * Gets the principal from the configuration.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return the user name
   * @since 1.5.0
   * @see #setConnectorInfo(JobConf, String, AuthenticationToken)
   */
  protected static String getPrincipal(JobConf job) {
    return OutputConfigurator.getPrincipal(CLASS, job);
  }

  /**
   * Gets the serialized token class from either the configuration or the token file.
   *
   * @since 1.5.0
   * @deprecated since 1.6.0; Use {@link #getAuthenticationToken(JobConf)} instead.
   */
  @Deprecated
  protected static String getTokenClass(JobConf job) {
    return getAuthenticationToken(job).getClass().getName();
  }

  /**
   * Gets the serialized token from either the configuration or the token file.
   *
   * @since 1.5.0
   * @deprecated since 1.6.0; Use {@link #getAuthenticationToken(JobConf)} instead.
   */
  @Deprecated
  protected static byte[] getToken(JobConf job) {
    return AuthenticationTokenSerializer.serialize(getAuthenticationToken(job));
  }

  /**
   * Gets the authenticated token from either the specified token file or directly from the
   * configuration, whichever was used when the job was configured.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @return the principal's authentication token
   * @since 1.6.0
   * @see #setConnectorInfo(JobConf, String, AuthenticationToken)
   * @see #setConnectorInfo(JobConf, String, String)
   */
  protected static AuthenticationToken getAuthenticationToken(JobConf job) {
    AuthenticationToken token = OutputConfigurator.getAuthenticationToken(CLASS, job);
    return OutputConfigurator.unwrapAuthenticationToken(job, token);
  }

  /**
   * Configures a {@link org.apache.accumulo.core.client.ZooKeeperInstance} for this job.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param instanceName
   *          the Accumulo instance name
   * @param zooKeepers
   *          a comma-separated list of zookeeper servers
   * @since 1.5.0
   * @deprecated since 1.6.0; Use
   *             {@link #setZooKeeperInstance(JobConf, org.apache.accumulo.core.client.ClientConfiguration)}
   *             instead.
   */
  @Deprecated
  public static void setZooKeeperInstance(JobConf job, String instanceName, String zooKeepers) {
    setZooKeeperInstance(job, org.apache.accumulo.core.client.ClientConfiguration.create()
        .withInstance(instanceName).withZkHosts(zooKeepers));
  }

  /**
   * Configures a {@link org.apache.accumulo.core.client.ZooKeeperInstance} for this job.
   *
   * @param job
   *          the Hadoop job instance to be configured
   *
   * @param clientConfig
   *          client configuration for specifying connection timeouts, SSL connection options, etc.
   * @since 1.6.0
   */
  public static void setZooKeeperInstance(JobConf job,
      org.apache.accumulo.core.client.ClientConfiguration clientConfig) {
    OutputConfigurator.setZooKeeperInstance(CLASS, job, clientConfig);
  }

  /**
   * Initializes an Accumulo {@link org.apache.accumulo.core.client.Instance} based on the
   * configuration.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return an Accumulo instance
   * @since 1.5.0
   * @see #setZooKeeperInstance(JobConf, org.apache.accumulo.core.client.ClientConfiguration)
   */
  protected static org.apache.accumulo.core.client.Instance getInstance(JobConf job) {
    return OutputConfigurator.getInstance(CLASS, job);
  }

  /**
   * Sets the log level for this job.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param level
   *          the logging level
   * @since 1.5.0
   */
  public static void setLogLevel(JobConf job, Level level) {
    OutputConfigurator.setLogLevel(CLASS, job, level);
  }

  /**
   * Gets the log level from this configuration.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return the log level
   * @since 1.5.0
   * @see #setLogLevel(JobConf, Level)
   */
  protected static Level getLogLevel(JobConf job) {
    return OutputConfigurator.getLogLevel(CLASS, job);
  }

  /**
   * Sets the default table name to use if one emits a null in place of a table name for a given
   * mutation. Table names can only be alpha-numeric and underscores.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param tableName
   *          the table to use when the tablename is null in the write call
   * @since 1.5.0
   */
  public static void setDefaultTableName(JobConf job, String tableName) {
    OutputConfigurator.setDefaultTableName(CLASS, job, tableName);
  }

  /**
   * Gets the default table name from the configuration.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return the default table name
   * @since 1.5.0
   * @see #setDefaultTableName(JobConf, String)
   */
  protected static String getDefaultTableName(JobConf job) {
    return OutputConfigurator.getDefaultTableName(CLASS, job);
  }

  /**
   * Sets the configuration for for the job's {@link BatchWriter} instances. If not set, a new
   * {@link BatchWriterConfig}, with sensible built-in defaults is used. Setting the configuration
   * multiple times overwrites any previous configuration.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param bwConfig
   *          the configuration for the {@link BatchWriter}
   * @since 1.5.0
   */
  public static void setBatchWriterOptions(JobConf job, BatchWriterConfig bwConfig) {
    OutputConfigurator.setBatchWriterOptions(CLASS, job, bwConfig);
  }

  /**
   * Gets the {@link BatchWriterConfig} settings.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return the configuration object
   * @since 1.5.0
   * @see #setBatchWriterOptions(JobConf, BatchWriterConfig)
   */
  protected static BatchWriterConfig getBatchWriterOptions(JobConf job) {
    return OutputConfigurator.getBatchWriterOptions(CLASS, job);
  }

  /**
   * Sets the directive to create new tables, as necessary. Table names can only be alpha-numeric
   * and underscores.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @since 1.5.0
   */
  public static void setCreateTables(JobConf job, boolean enableFeature) {
    OutputConfigurator.setCreateTables(CLASS, job, enableFeature);
  }

  /**
   * Determines whether tables are permitted to be created as needed.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return true if the feature is disabled, false otherwise
   * @since 1.5.0
   * @see #setCreateTables(JobConf, boolean)
   */
  protected static Boolean canCreateTables(JobConf job) {
    return OutputConfigurator.canCreateTables(CLASS, job);
  }

  /**
   * Sets the directive to use simulation mode for this job. In simulation mode, no output is
   * produced. This is useful for testing.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @since 1.5.0
   */
  public static void setSimulationMode(JobConf job, boolean enableFeature) {
    OutputConfigurator.setSimulationMode(CLASS, job, enableFeature);
  }

  /**
   * Determines whether this feature is enabled.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return true if the feature is enabled, false otherwise
   * @since 1.5.0
   * @see #setSimulationMode(JobConf, boolean)
   */
  protected static Boolean getSimulationMode(JobConf job) {
    return OutputConfigurator.getSimulationMode(CLASS, job);
  }

  /**
   * A base class to be used to create {@link RecordWriter} instances that write to Accumulo.
   */
  protected static class AccumuloRecordWriter implements RecordWriter<Text,Mutation> {
    private MultiTableBatchWriter mtbw = null;
    private HashMap<Text,BatchWriter> bws = null;
    private Text defaultTableName = null;

    private boolean simulate = false;
    private boolean createTables = false;

    private long mutCount = 0;
    private long valCount = 0;

    private AccumuloClient client;

    protected AccumuloRecordWriter(JobConf job)
        throws AccumuloException, AccumuloSecurityException, IOException {
      Level l = getLogLevel(job);
      if (l != null)
        log.setLevel(getLogLevel(job));
      this.simulate = getSimulationMode(job);
      this.createTables = canCreateTables(job);

      if (simulate)
        log.info("Simulating output only. No writes to tables will occur");

      this.bws = new HashMap<>();

      String tname = getDefaultTableName(job);
      this.defaultTableName = (tname == null) ? null : new Text(tname);

      if (!simulate) {
        this.client = OutputConfigurator.client(CLASS, job);
        mtbw = client.createMultiTableBatchWriter(getBatchWriterOptions(job));
      }
    }

    /**
     * Push a mutation into a table. If table is null, the defaultTable will be used. If
     * {@link AccumuloOutputFormat#canCreateTables(JobConf)} is set, the table will be created if it
     * does not exist. The table name must only contain alphanumerics and underscore.
     */
    @Override
    public void write(Text table, Mutation mutation) throws IOException {
      if (table == null || table.toString().isEmpty())
        table = this.defaultTableName;

      if (!simulate && table == null)
        throw new IOException("No table or default table specified. Try simulation mode next time");

      ++mutCount;
      valCount += mutation.size();
      printMutation(table, mutation);

      if (simulate)
        return;

      if (!bws.containsKey(table))
        try {
          addTable(table);
        } catch (final Exception e) {
          log.error("Could not add table '" + table + "'", e);
          throw new IOException(e);
        }

      try {
        bws.get(table).addMutation(mutation);
      } catch (MutationsRejectedException e) {
        throw new IOException(e);
      }
    }

    public void addTable(Text tableName) throws AccumuloException, AccumuloSecurityException {
      if (simulate) {
        log.info("Simulating adding table: " + tableName);
        return;
      }

      log.debug("Adding table: " + tableName);
      BatchWriter bw = null;
      String table = tableName.toString();

      if (createTables && !client.tableOperations().exists(table)) {
        try {
          client.tableOperations().create(table);
        } catch (AccumuloSecurityException e) {
          log.error("Accumulo security violation creating " + table, e);
          throw e;
        } catch (TableExistsException e) {
          // Shouldn't happen
        }
      }

      try {
        bw = mtbw.getBatchWriter(table);
      } catch (TableNotFoundException e) {
        log.error("Accumulo table " + table + " doesn't exist and cannot be created.", e);
        throw new AccumuloException(e);
      } catch (AccumuloException | AccumuloSecurityException e) {
        throw e;
      }

      if (bw != null)
        bws.put(tableName, bw);
    }

    private int printMutation(Text table, Mutation m) {
      if (log.isTraceEnabled()) {
        log.trace(String.format("Table %s row key: %s", table, hexDump(m.getRow())));
        for (ColumnUpdate cu : m.getUpdates()) {
          log.trace(String.format("Table %s column: %s:%s", table, hexDump(cu.getColumnFamily()),
              hexDump(cu.getColumnQualifier())));
          log.trace(String.format("Table %s security: %s", table,
              new ColumnVisibility(cu.getColumnVisibility()).toString()));
          log.trace(String.format("Table %s value: %s", table, hexDump(cu.getValue())));
        }
      }
      return m.getUpdates().size();
    }

    private String hexDump(byte[] ba) {
      StringBuilder sb = new StringBuilder();
      for (byte b : ba) {
        if ((b > 0x20) && (b < 0x7e))
          sb.append((char) b);
        else
          sb.append(String.format("x%02x", b));
      }
      return sb.toString();
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      log.debug("mutations written: " + mutCount + ", values written: " + valCount);
      if (simulate)
        return;

      try {
        mtbw.close();
      } catch (MutationsRejectedException e) {
        if (!e.getSecurityErrorCodes().isEmpty()) {
          HashMap<String,Set<SecurityErrorCode>> tables = new HashMap<>();
          for (Entry<TabletId,Set<SecurityErrorCode>> ke : e.getSecurityErrorCodes().entrySet()) {
            String tableId = ke.getKey().getTableId().toString();
            Set<SecurityErrorCode> secCodes = tables.get(tableId);
            if (secCodes == null) {
              secCodes = new HashSet<>();
              tables.put(tableId, secCodes);
            }
            secCodes.addAll(ke.getValue());
          }

          log.error("Not authorized to write to tables : " + tables);
        }

        if (e.getConstraintViolationSummaries().size() > 0) {
          log.error("Constraint violations : " + e.getConstraintViolationSummaries().size());
        }
        throw new IOException(e);
      } finally {
        client.close();
      }
    }
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    if (!isConnectorInfoSet(job))
      throw new IOException("Connector info has not been set.");
    try {
      AccumuloClient c = OutputConfigurator.client(CLASS, job);
      String principal = getPrincipal(job);
      AuthenticationToken token = getAuthenticationToken(job);
      if (!c.securityOperations().authenticateUser(principal, token))
        throw new IOException("Unable to authenticate user");
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new IOException(e);
    }
  }

  @Override
  public RecordWriter<Text,Mutation> getRecordWriter(FileSystem ignored, JobConf job, String name,
      Progressable progress) throws IOException {
    try {
      return new AccumuloRecordWriter(job);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

}

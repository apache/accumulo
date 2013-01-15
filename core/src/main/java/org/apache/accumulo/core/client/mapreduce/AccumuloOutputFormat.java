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
package org.apache.accumulo.core.client.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This class allows MapReduce jobs to use Accumulo as the sink for data. This {@link OutputFormat} accepts keys and values of type {@link Text} (for a table
 * name) and {@link Mutation} from the Map and Reduce functions.
 * 
 * The user must specify the following via static configurator methods:
 * 
 * <ul>
 * <li>{@link AccumuloOutputFormat#setOutputInfo(Job, String, byte[], boolean, String)}
 * <li>{@link AccumuloOutputFormat#setZooKeeperInstance(Job, String, String)}
 * </ul>
 * 
 * Other static methods are optional.
 */
public class AccumuloOutputFormat extends OutputFormat<Text,Mutation> {
  private static final Logger log = Logger.getLogger(AccumuloOutputFormat.class);
  
  /**
   * Prefix shared by all job configuration property names for this class
   */
  private static final String PREFIX = AccumuloOutputFormat.class.getSimpleName();
  
  /**
   * Used to limit the times a job can be configured with output information to 1
   * 
   * @see #setOutputInfo(Job, String, byte[], boolean, String)
   * @see #getUsername(JobContext)
   * @see #getPassword(JobContext)
   * @see #canCreateTables(JobContext)
   * @see #getDefaultTableName(JobContext)
   */
  private static final String OUTPUT_INFO_HAS_BEEN_SET = PREFIX + ".configured";
  
  /**
   * Used to limit the times a job can be configured with instance information to 1
   * 
   * @see #setZooKeeperInstance(Job, String, String)
   * @see #setMockInstance(Job, String)
   * @see #getInstance(JobContext)
   */
  private static final String INSTANCE_HAS_BEEN_SET = PREFIX + ".instanceConfigured";
  
  /**
   * Key for storing the Accumulo user's name
   * 
   * @see #setOutputInfo(Job, String, byte[], boolean, String)
   * @see #getUsername(JobContext)
   */
  private static final String USERNAME = PREFIX + ".username";
  
  /**
   * Key for storing the Accumulo user's password
   * 
   * @see #setOutputInfo(Job, String, byte[], boolean, String)
   * @see #getPassword(JobContext)
   */
  private static final String PASSWORD = PREFIX + ".password";
  
  /**
   * Key for storing the default table to use when the output key is null
   * 
   * @see #getDefaultTableName(JobContext)
   */
  private static final String DEFAULT_TABLE_NAME = PREFIX + ".defaulttable";
  
  /**
   * Key for storing the Accumulo instance name to connect to
   * 
   * @see #setZooKeeperInstance(Job, String, String)
   * @see #setMockInstance(Job, String)
   * @see #getInstance(JobContext)
   */
  private static final String INSTANCE_NAME = PREFIX + ".instanceName";
  
  /**
   * Key for storing the set of Accumulo zookeeper servers to communicate with
   * 
   * @see #setZooKeeperInstance(Job, String, String)
   * @see #getInstance(JobContext)
   */
  private static final String ZOOKEEPERS = PREFIX + ".zooKeepers";
  
  /**
   * Key for storing the directive to use the mock instance type
   * 
   * @see #setMockInstance(Job, String)
   * @see #getInstance(JobContext)
   */
  private static final String MOCK = ".useMockInstance";
  
  /**
   * Key for storing the directive to create tables that don't exist
   * 
   * @see #setOutputInfo(Job, String, byte[], boolean, String)
   * @see #canCreateTables(JobContext)
   */
  private static final String CREATETABLES = PREFIX + ".createtables";
  
  /**
   * Key for storing the desired logging level
   * 
   * @see #setLogLevel(Job, Level)
   * @see #getLogLevel(JobContext)
   */
  private static final String LOGLEVEL = PREFIX + ".loglevel";
  
  /**
   * Key for storing the directive to simulate output instead of actually writing to a file
   * 
   * @see #setSimulationMode(Job, boolean)
   * @see #getSimulationMode(JobContext)
   */
  private static final String SIMULATE = PREFIX + ".simulate";
  
  /**
   * Sets the minimum information needed to write to Accumulo in this job.
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param user
   *          a valid Accumulo user name (user must have Table.CREATE permission)
   * @param passwd
   *          the user's password
   * @param createTables
   *          if true, the output format will create new tables as necessary. Table names can only be alpha-numeric and underscores.
   * @param defaultTable
   *          the table to use when the tablename is null in the write call
   * @since 1.5.0
   */
  public static void setOutputInfo(Job job, String user, byte[] passwd, boolean createTables, String defaultTable) {
    if (job.getConfiguration().getBoolean(OUTPUT_INFO_HAS_BEEN_SET, false))
      throw new IllegalStateException("Output info can only be set once per job");
    job.getConfiguration().setBoolean(OUTPUT_INFO_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(user, passwd);
    job.getConfiguration().set(USERNAME, user);
    job.getConfiguration().set(PASSWORD, new String(Base64.encodeBase64(passwd)));
    job.getConfiguration().setBoolean(CREATETABLES, createTables);
    if (defaultTable != null)
      job.getConfiguration().set(DEFAULT_TABLE_NAME, defaultTable);
  }
  
  /**
   * Configures a {@link ZooKeeperInstance} for this job.
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param instanceName
   *          the Accumulo instance name
   * @param zooKeepers
   *          a comma-separated list of zookeeper servers
   * @since 1.5.0
   */
  public static void setZooKeeperInstance(Job job, String instanceName, String zooKeepers) {
    if (job.getConfiguration().getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IllegalStateException("Instance info can only be set once per job");
    job.getConfiguration().setBoolean(INSTANCE_HAS_BEEN_SET, true);
    ArgumentChecker.notNull(instanceName, zooKeepers);
    job.getConfiguration().set(INSTANCE_NAME, instanceName);
    job.getConfiguration().set(ZOOKEEPERS, zooKeepers);
    System.out.println("instance set: " + job.getConfiguration().get(INSTANCE_HAS_BEEN_SET));
  }
  
  /**
   * Configures a {@link MockInstance} for this job.
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param instanceName
   *          the Accumulo instance name
   * @since 1.5.0
   */
  public static void setMockInstance(Job job, String instanceName) {
    job.getConfiguration().setBoolean(INSTANCE_HAS_BEEN_SET, true);
    job.getConfiguration().setBoolean(MOCK, true);
    job.getConfiguration().set(INSTANCE_NAME, instanceName);
  }
  
  /**
   * @since 1.5.0
   * @see BatchWriterConfig#setMaxMemory(long)
   */
  public static void setMaxMutationBufferSize(Job job, long numberOfBytes) {
    job.getConfiguration().setLong(MAX_MUTATION_BUFFER_SIZE, numberOfBytes);
  }
  
  /**
   * @since 1.5.0
   * @see BatchWriterConfig#setMaxLatency(long, TimeUnit)
   */
  public static void setMaxLatency(Job job, int numberOfMilliseconds) {
    job.getConfiguration().setInt(MAX_LATENCY, numberOfMilliseconds);
  }
  
  /**
   * @since 1.5.0
   * @see BatchWriterConfig#setMaxWriteThreads(int)
   */
  public static void setMaxWriteThreads(Job job, int numberOfThreads) {
    job.getConfiguration().setInt(NUM_WRITE_THREADS, numberOfThreads);
  }
  
  /**
   * @since 1.5.0
   * @see BatchWriterConfig#setTimeout(long, TimeUnit)
   */
  public static void setTimeout(Job job, long time, TimeUnit timeUnit) {
    job.getConfiguration().setLong(TIMEOUT, timeUnit.toMillis(time));
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
  public static void setLogLevel(Job job, Level level) {
    ArgumentChecker.notNull(level);
    job.getConfiguration().setInt(LOGLEVEL, level.toInt());
  }
  
  /**
   * Sets the directive to use simulation mode for this job. In simulation mode, no output is produced. This is useful for testing.
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
  public static void setSimulationMode(Job job, boolean enableFeature) {
    job.getConfiguration().setBoolean(SIMULATE, enableFeature);
  }
  
  /**
   * Gets the user name from the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the user name
   * @since 1.5.0
   * @see #setOutputInfo(Job, String, byte[], boolean, String)
   */
  protected static String getUsername(JobContext context) {
    return context.getConfiguration().get(USERNAME);
  }
  
  /**
   * Gets the password from the configuration. WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to
   * provide a charset safe conversion to a string, and is not intended to be secure.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the decoded user password
   * @since 1.5.0
   * @see #setOutputInfo(Job, String, byte[], boolean, String)
   */
  protected static byte[] getPassword(JobContext context) {
    return Base64.decodeBase64(context.getConfiguration().get(PASSWORD, "").getBytes());
  }
  
  /**
   * Determines whether tables are permitted to be created as needed.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return true if the feature is disabled, false otherwise
   * @since 1.5.0
   * @see #setOutputInfo(Job, String, byte[], boolean, String)
   */
  protected static boolean canCreateTables(JobContext context) {
    return context.getConfiguration().getBoolean(CREATETABLES, false);
  }
  
  /**
   * Gets the default table name from the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the default table name
   * @since 1.5.0
   * @see #setOutputInfo(Job, String, byte[], boolean, String)
   */
  protected static String getDefaultTableName(JobContext context) {
    return context.getConfiguration().get(DEFAULT_TABLE_NAME);
  }
  
  /**
   * Initializes an Accumulo {@link Instance} based on the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return an Accumulo instance
   * @since 1.5.0
   * @see #setZooKeeperInstance(Job, String, String)
   * @see #setMockInstance(Job, String)
   */
  protected static Instance getInstance(JobContext context) {
    if (context.getConfiguration().getBoolean(MOCK, false))
      return new MockInstance(context.getConfiguration().get(INSTANCE_NAME));
    return new ZooKeeperInstance(context.getConfiguration().get(INSTANCE_NAME), context.getConfiguration().get(ZOOKEEPERS));
  }
  
  /**
   * Gets the corresponding {@link BatchWriterConfig} setting.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the max memory to use (in bytes)
   * @since 1.5.0
   * @see #setMaxMutationBufferSize(Job, long)
   */
  protected static long getMaxMutationBufferSize(JobContext context) {
    return context.getConfiguration().getLong(MAX_MUTATION_BUFFER_SIZE, new BatchWriterConfig().getMaxMemory());
  }
  
  /**
   * Gets the corresponding {@link BatchWriterConfig} setting.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the max latency to use (in millis)
   * @since 1.5.0
   * @see #setMaxLatency(Job, int)
   */
  protected static long getMaxLatency(JobContext context) {
    return context.getConfiguration().getLong(MAX_LATENCY, new BatchWriterConfig().getMaxLatency(TimeUnit.MILLISECONDS));
  }
  
  /**
   * Gets the corresponding {@link BatchWriterConfig} setting.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the max write threads to use
   * @since 1.5.0
   * @see #setMaxWriteThreads(Job, int)
   */
  protected static int getMaxWriteThreads(JobContext context) {
    return context.getConfiguration().getInt(NUM_WRITE_THREADS, new BatchWriterConfig().getMaxWriteThreads());
  }
  
  /**
   * Gets the corresponding {@link BatchWriterConfig} setting.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the timeout for write operations
   * @since 1.5.0
   * @see #setTimeout(Job, long, TimeUnit)
   */
  protected static long getTimeout(JobContext context) {
    return context.getConfiguration().getLong(TIMEOUT, Long.MAX_VALUE);
  }
  
  /**
   * Gets the log level from this configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the log level
   * @since 1.5.0
   * @see #setLogLevel(Job, Level)
   */
  protected static Level getLogLevel(JobContext context) {
    if (context.getConfiguration().get(LOGLEVEL) != null)
      return Level.toLevel(context.getConfiguration().getInt(LOGLEVEL, Level.INFO.toInt()));
    return null;
  }
  
  /**
   * Determines whether this feature is enabled.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return true if the feature is enabled, false otherwise
   * @since 1.5.0
   * @see #setSimulationMode(Job, boolean)
   */
  protected static boolean getSimulationMode(JobContext context) {
    return context.getConfiguration().getBoolean(SIMULATE, false);
  }
  
  /**
   * A base class to be used to create {@link RecordWriter} instances that write to Accumulo.
   */
  protected static class AccumuloRecordWriter extends RecordWriter<Text,Mutation> {
    private MultiTableBatchWriter mtbw = null;
    private HashMap<Text,BatchWriter> bws = null;
    private Text defaultTableName = null;
    
    private boolean simulate = false;
    private boolean createTables = false;
    
    private long mutCount = 0;
    private long valCount = 0;
    
    private Connector conn;
    
    protected AccumuloRecordWriter(TaskAttemptContext context) throws AccumuloException, AccumuloSecurityException, IOException {
      Level l = getLogLevel(context);
      if (l != null)
        log.setLevel(getLogLevel(context));
      this.simulate = getSimulationMode(context);
      this.createTables = canCreateTables(context);
      
      if (simulate)
        log.info("Simulating output only. No writes to tables will occur");
      
      this.bws = new HashMap<Text,BatchWriter>();
      
      String tname = getDefaultTableName(context);
      this.defaultTableName = (tname == null) ? null : new Text(tname);
      
      if (!simulate) {
        this.conn = getInstance(context).getConnector(getUsername(context), getPassword(context));
        mtbw = conn.createMultiTableBatchWriter(new BatchWriterConfig().setMaxMemory(getMaxMutationBufferSize(context))
            .setMaxLatency(getMaxLatency(context), TimeUnit.MILLISECONDS).setMaxWriteThreads(getMaxWriteThreads(context))
            .setTimeout(getTimeout(context), TimeUnit.MILLISECONDS));
      }
    }
    
    /**
     * Push a mutation into a table. If table is null, the defaultTable will be used. If canCreateTable is set, the table will be created if it does not exist.
     * The table name must only contain alphanumerics and underscore.
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
        } catch (Exception e) {
          e.printStackTrace();
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
      
      if (createTables && !conn.tableOperations().exists(table)) {
        try {
          conn.tableOperations().create(table);
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
      } catch (AccumuloException e) {
        throw e;
      } catch (AccumuloSecurityException e) {
        throw e;
      }
      
      if (bw != null)
        bws.put(tableName, bw);
    }
    
    private int printMutation(Text table, Mutation m) {
      if (log.isTraceEnabled()) {
        log.trace(String.format("Table %s row key: %s", table, hexDump(m.getRow())));
        for (ColumnUpdate cu : m.getUpdates()) {
          log.trace(String.format("Table %s column: %s:%s", table, hexDump(cu.getColumnFamily()), hexDump(cu.getColumnQualifier())));
          log.trace(String.format("Table %s security: %s", table, new ColumnVisibility(cu.getColumnVisibility()).toString()));
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
    public void close(TaskAttemptContext attempt) throws IOException, InterruptedException {
      log.debug("mutations written: " + mutCount + ", values written: " + valCount);
      if (simulate)
        return;
      
      try {
        mtbw.close();
      } catch (MutationsRejectedException e) {
        if (e.getAuthorizationFailures().size() >= 0) {
          HashMap<String,Set<SecurityErrorCode>> tables = new HashMap<String,Set<SecurityErrorCode>>();
          for (Entry<KeyExtent,Set<SecurityErrorCode>> ke : e.getAuthorizationFailures().entrySet()) {
            Set<SecurityErrorCode> secCodes = tables.get(ke.getKey().getTableId().toString());
            if (secCodes == null) {
              secCodes = new HashSet<SecurityErrorCode>();
              tables.put(ke.getKey().getTableId().toString(), secCodes);
            }
            secCodes.addAll(ke.getValue());
          }
          
          log.error("Not authorized to write to tables : " + tables);
        }
        
        if (e.getConstraintViolationSummaries().size() > 0) {
          log.error("Constraint violations : " + e.getConstraintViolationSummaries().size());
        }
      }
    }
  }
  
  @Override
  public void checkOutputSpecs(JobContext job) throws IOException {
    if (!job.getConfiguration().getBoolean(OUTPUT_INFO_HAS_BEEN_SET, false))
      throw new IOException("Output info has not been set.");
    if (!job.getConfiguration().getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IOException("Instance info has not been set.");
    try {
      Connector c = getInstance(job).getConnector(getUsername(job), getPassword(job));
      if (!c.securityOperations().authenticateUser(getUsername(job), getPassword(job)))
        throw new IOException("Unable to authenticate user");
    } catch (AccumuloException e) {
      throw new IOException(e);
    } catch (AccumuloSecurityException e) {
      throw new IOException(e);
    }
  }
  
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new NullOutputFormat<Text,Mutation>().getOutputCommitter(context);
  }
  
  @Override
  public RecordWriter<Text,Mutation> getRecordWriter(TaskAttemptContext attempt) throws IOException {
    try {
      return new AccumuloRecordWriter(attempt);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
  
  // ----------------------------------------------------------------------------------------------------
  // Everything below this line is deprecated and should go away in future versions
  // ----------------------------------------------------------------------------------------------------
  
  /**
   * @deprecated since 1.5.0;
   */
  @Deprecated
  private static final String MAX_MUTATION_BUFFER_SIZE = PREFIX + ".maxmemory";
  
  /**
   * @deprecated since 1.5.0;
   */
  @Deprecated
  private static final String MAX_LATENCY = PREFIX + ".maxlatency";
  
  /**
   * @deprecated since 1.5.0;
   */
  @Deprecated
  private static final String NUM_WRITE_THREADS = PREFIX + ".writethreads";
  
  /**
   * @deprecated since 1.5.0;
   */
  @Deprecated
  private static final String TIMEOUT = PREFIX + ".timeout";
  
  /**
   * @deprecated since 1.5.0; Use {@link #setOutputInfo(Job, String, byte[], boolean, String)} instead.
   */
  @Deprecated
  public static void setOutputInfo(Configuration conf, String user, byte[] passwd, boolean createTables, String defaultTable) {
    if (conf.getBoolean(OUTPUT_INFO_HAS_BEEN_SET, false))
      throw new IllegalStateException("Output info can only be set once per job");
    conf.setBoolean(OUTPUT_INFO_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(user, passwd);
    conf.set(USERNAME, user);
    conf.set(PASSWORD, new String(Base64.encodeBase64(passwd)));
    conf.setBoolean(CREATETABLES, createTables);
    if (defaultTable != null)
      conf.set(DEFAULT_TABLE_NAME, defaultTable);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setZooKeeperInstance(Job, String, String)} instead.
   */
  @Deprecated
  public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
    if (conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IllegalStateException("Instance info can only be set once per job");
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(instanceName, zooKeepers);
    conf.set(INSTANCE_NAME, instanceName);
    conf.set(ZOOKEEPERS, zooKeepers);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setMockInstance(Job, String)} instead.
   */
  @Deprecated
  public static void setMockInstance(Configuration conf, String instanceName) {
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    conf.setBoolean(MOCK, true);
    conf.set(INSTANCE_NAME, instanceName);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setMaxMutationBufferSize(Job, long)} instead.
   */
  @Deprecated
  public static void setMaxMutationBufferSize(Configuration conf, long numberOfBytes) {
    conf.setLong(MAX_MUTATION_BUFFER_SIZE, numberOfBytes);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setMaxLatency(Job, int)} instead.
   */
  @Deprecated
  public static void setMaxLatency(Configuration conf, int numberOfMilliseconds) {
    conf.setInt(MAX_LATENCY, numberOfMilliseconds);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setMaxWriteThreads(Job, int)} instead.
   */
  @Deprecated
  public static void setMaxWriteThreads(Configuration conf, int numberOfThreads) {
    conf.setInt(NUM_WRITE_THREADS, numberOfThreads);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setLogLevel(Job, Level)} instead.
   */
  @Deprecated
  public static void setLogLevel(Configuration conf, Level level) {
    ArgumentChecker.notNull(level);
    conf.setInt(LOGLEVEL, level.toInt());
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setSimulationMode(Job, boolean)} instead.
   */
  @Deprecated
  public static void setSimulationMode(Configuration conf) {
    conf.setBoolean(SIMULATE, true);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getUsername(JobContext)} instead.
   */
  @Deprecated
  protected static String getUsername(Configuration conf) {
    return conf.get(USERNAME);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getPassword(JobContext)} instead.
   */
  @Deprecated
  protected static byte[] getPassword(Configuration conf) {
    return Base64.decodeBase64(conf.get(PASSWORD, "").getBytes());
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #canCreateTables(JobContext)} instead.
   */
  @Deprecated
  protected static boolean canCreateTables(Configuration conf) {
    return conf.getBoolean(CREATETABLES, false);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getDefaultTableName(JobContext)} instead.
   */
  @Deprecated
  protected static String getDefaultTableName(Configuration conf) {
    return conf.get(DEFAULT_TABLE_NAME);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getInstance(JobContext)} instead.
   */
  @Deprecated
  protected static Instance getInstance(Configuration conf) {
    if (conf.getBoolean(MOCK, false))
      return new MockInstance(conf.get(INSTANCE_NAME));
    return new ZooKeeperInstance(conf.get(INSTANCE_NAME), conf.get(ZOOKEEPERS));
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getMaxMutationBufferSize(JobContext)} instead.
   */
  @Deprecated
  protected static long getMaxMutationBufferSize(Configuration conf) {
    return conf.getLong(MAX_MUTATION_BUFFER_SIZE, new BatchWriterConfig().getMaxMemory());
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getMaxLatency(JobContext)} instead.
   */
  @Deprecated
  protected static int getMaxLatency(Configuration conf) {
    Long maxLatency = new BatchWriterConfig().getMaxLatency(TimeUnit.MILLISECONDS);
    Integer max = maxLatency >= Integer.MAX_VALUE ? Integer.MAX_VALUE : Integer.parseInt(Long.toString(maxLatency));
    return conf.getInt(MAX_LATENCY, max);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getMaxWriteThreads(JobContext)} instead.
   */
  @Deprecated
  protected static int getMaxWriteThreads(Configuration conf) {
    return conf.getInt(NUM_WRITE_THREADS, new BatchWriterConfig().getMaxWriteThreads());
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getLogLevel(JobContext)} instead.
   */
  @Deprecated
  protected static Level getLogLevel(Configuration conf) {
    if (conf.get(LOGLEVEL) != null)
      return Level.toLevel(conf.getInt(LOGLEVEL, Level.INFO.toInt()));
    return null;
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getSimulationMode(JobContext)} instead.
   */
  @Deprecated
  protected static boolean getSimulationMode(Configuration conf) {
    return conf.getBoolean(SIMULATE, false);
  }
  
}

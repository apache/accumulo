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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
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
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This class allows MapReduce jobs to use Accumulo as the sink of data. This output format accepts keys and values of type Text (for a table name) and Mutation
 * from the Map() and Reduce() functions.
 * 
 * The user must specify the following via static methods:
 * 
 * <ul>
 * <li>AccumuloOutputFormat.setOutputInfo(job, username, password, createTables, defaultTableName)
 * <li>AccumuloOutputFormat.setZooKeeperInstance(job, instanceName, hosts)
 * </ul>
 * 
 * Other static methods are optional
 */
public class AccumuloOutputFormat extends OutputFormat<Text,Mutation> {
  private static final Logger log = Logger.getLogger(AccumuloOutputFormat.class);
  
  private static final String PREFIX = AccumuloOutputFormat.class.getSimpleName();
  private static final String OUTPUT_INFO_HAS_BEEN_SET = PREFIX + ".configured";
  private static final String INSTANCE_HAS_BEEN_SET = PREFIX + ".instanceConfigured";
  private static final String USERNAME = PREFIX + ".username";
  private static final String PASSWORD = PREFIX + ".password";
  private static final String DEFAULT_TABLE_NAME = PREFIX + ".defaulttable";
  
  private static final String INSTANCE_NAME = PREFIX + ".instanceName";
  private static final String ZOOKEEPERS = PREFIX + ".zooKeepers";
  private static final String MOCK = ".useMockInstance";
  
  private static final String CREATETABLES = PREFIX + ".createtables";
  private static final String LOGLEVEL = PREFIX + ".loglevel";
  private static final String SIMULATE = PREFIX + ".simulate";
  
  // BatchWriter options
  private static final String MAX_MUTATION_BUFFER_SIZE = PREFIX + ".maxmemory";
  private static final String MAX_LATENCY = PREFIX + ".maxlatency";
  private static final String NUM_WRITE_THREADS = PREFIX + ".writethreads";
  
  private static final long DEFAULT_MAX_MUTATION_BUFFER_SIZE = 50 * 1024 * 1024; // 50MB
  private static final int DEFAULT_MAX_LATENCY = 60; // 1 minute
  private static final int DEFAULT_NUM_WRITE_THREADS = 2;
  
  /**
   * Configure the output format.
   * 
   * @param job
   *          the Map/Reduce job object
   * @param user
   *          the username, which must have the Table.CREATE permission to create tables
   * @param passwd
   *          the passwd for the username
   * @param createTables
   *          the output format will create new tables as necessary. Table names can only be alpha-numeric and underscores.
   * @param defaultTable
   *          the table to use when the tablename is null in the write call
   * @deprecated Use {@link #setOutputInfo(Configuration,String,byte[],boolean,String)} instead
   */
  public static void setOutputInfo(JobContext job, String user, byte[] passwd, boolean createTables, String defaultTable) {
    setOutputInfo(job.getConfiguration(), user, passwd, createTables, defaultTable);
  }

  /**
   * Configure the output format.
   * 
   * @param conf
   *          the Map/Reduce job object
   * @param user
   *          the username, which must have the Table.CREATE permission to create tables
   * @param passwd
   *          the passwd for the username
   * @param createTables
   *          the output format will create new tables as necessary. Table names can only be alpha-numeric and underscores.
   * @param defaultTable
   *          the table to use when the tablename is null in the write call
   */
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
   * @deprecated Use {@link #setZooKeeperInstance(Configuration,String,String)} instead
   */
  public static void setZooKeeperInstance(JobContext job, String instanceName, String zooKeepers) {
    setZooKeeperInstance(job.getConfiguration(), instanceName, zooKeepers);
  }

  public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
    if (conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IllegalStateException("Instance info can only be set once per job");
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(instanceName, zooKeepers);
    conf.set(INSTANCE_NAME, instanceName);
    conf.set(ZOOKEEPERS, zooKeepers);
  }
  
  /**
   * @deprecated Use {@link #setMockInstance(Configuration,String)} instead
   */
  public static void setMockInstance(JobContext job, String instanceName) {
    setMockInstance(job.getConfiguration(), instanceName);
  }

  public static void setMockInstance(Configuration conf, String instanceName) {
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    conf.setBoolean(MOCK, true);
    conf.set(INSTANCE_NAME, instanceName);
  }
  
  /**
   * @deprecated Use {@link #setMaxMutationBufferSize(Configuration,long)} instead
   */
  public static void setMaxMutationBufferSize(JobContext job, long numberOfBytes) {
    setMaxMutationBufferSize(job.getConfiguration(), numberOfBytes);
  }

  public static void setMaxMutationBufferSize(Configuration conf, long numberOfBytes) {
    conf.setLong(MAX_MUTATION_BUFFER_SIZE, numberOfBytes);
  }
  
  /**
   * @deprecated Use {@link #setMaxLatency(Configuration,int)} instead
   */
  public static void setMaxLatency(JobContext job, int numberOfMilliseconds) {
    setMaxLatency(job.getConfiguration(), numberOfMilliseconds);
  }
  
  public static void setMaxLatency(Configuration conf, int numberOfMilliseconds) {
    conf.setInt(MAX_LATENCY, numberOfMilliseconds);
  }
  
  /**
   * @deprecated Use {@link #setMaxWriteThreads(Configuration,int)} instead
   */
  public static void setMaxWriteThreads(JobContext job, int numberOfThreads) {
    setMaxWriteThreads(job.getConfiguration(), numberOfThreads);
  }
  
  public static void setMaxWriteThreads(Configuration conf, int numberOfThreads) {
    conf.setInt(NUM_WRITE_THREADS, numberOfThreads);
  }
  
  /**
   * @deprecated Use {@link #setLogLevel(Configuration,Level)} instead
   */
  public static void setLogLevel(JobContext job, Level level) {
    setLogLevel(job.getConfiguration(), level);
  }
  
  public static void setLogLevel(Configuration conf, Level level) {
    ArgumentChecker.notNull(level);
    conf.setInt(LOGLEVEL, level.toInt());
  }
  
  /**
   * @deprecated Use {@link #setSimulationMode(Configurtion)} instead
   */
  public static void setSimulationMode(JobContext job) {
    setSimulationMode(job.getConfiguration());
  }
  
  public static void setSimulationMode(Configuration conf) {
    conf.setBoolean(SIMULATE, true);
  }
  
  /**
   * @deprecated Use {@link #getUsername(Configuration)} instead
   */
  protected static String getUsername(JobContext job) {
    return getUsername(job.getConfiguration());
  }
  
  protected static String getUsername(Configuration conf) {
    return conf.get(USERNAME);
  }
  
  /**
   * WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to provide a charset safe conversion to a
   * string, and is not intended to be secure.
   * 
   * @deprecated Use {@link #getPassword(Configuration)} instead
   */
  protected static byte[] getPassword(JobContext job) {
    return getPassword(job.getConfiguration());
  }
  
  /**
   * WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to provide a charset safe conversion to a
   * string, and is not intended to be secure.
   */
  protected static byte[] getPassword(Configuration conf) {
    return Base64.decodeBase64(conf.get(PASSWORD, "").getBytes());
  }
  
  /**
   * @deprecated Use {@link #canCreateTables(Configuration)} instead
   */
  protected static boolean canCreateTables(JobContext job) {
    return canCreateTables(job.getConfiguration());
  }
  
  protected static boolean canCreateTables(Configuration conf) {
    return conf.getBoolean(CREATETABLES, false);
  }
  
  /**
   * @deprecated Use {@link #getDefaultTableName(Configuration)} instead
   */
  protected static String getDefaultTableName(JobContext job) {
    return getDefaultTableName(job.getConfiguration());
  }
  
  protected static String getDefaultTableName(Configuration conf) {
    return conf.get(DEFAULT_TABLE_NAME);
  }
  
  /**
   * @deprecated Use {@link #getInstance(Configuration)} instead
   */
  protected static Instance getInstance(JobContext job) {
    return getInstance(job.getConfiguration());
  }
  
  protected static Instance getInstance(Configuration conf) {
    if (conf.getBoolean(MOCK, false))
      return new MockInstance(conf.get(INSTANCE_NAME));
    return new ZooKeeperInstance(conf.get(INSTANCE_NAME), conf.get(ZOOKEEPERS));
  }
  
  /**
   * @deprecated Use {@link #getMaxMutationBufferSize(Configuration)} instead
   */
  protected static long getMaxMutationBufferSize(JobContext job) {
    return getMaxMutationBufferSize(job.getConfiguration());
  }
  
  protected static long getMaxMutationBufferSize(Configuration conf) {
    return conf.getLong(MAX_MUTATION_BUFFER_SIZE, DEFAULT_MAX_MUTATION_BUFFER_SIZE);
  }
  
  /**
   * @deprecated Use {@link #getMaxLatency(Configuration)} instead
   */
  protected static int getMaxLatency(JobContext job) {
    return getMaxLatency(job.getConfiguration());
  }
  
  protected static int getMaxLatency(Configuration conf) {
    return conf.getInt(MAX_LATENCY, DEFAULT_MAX_LATENCY);
  }
  
  /**
   * @deprecated Use {@link #getMaxWriteThreads(Configuration)} instead
   */
  protected static int getMaxWriteThreads(JobContext job) {
    return getMaxWriteThreads(job.getConfiguration());
  }
  
  protected static int getMaxWriteThreads(Configuration conf) {
    return conf.getInt(NUM_WRITE_THREADS, DEFAULT_NUM_WRITE_THREADS);
  }
  
  /**
   * @deprecated Use {@link #getLogLevel(Configuration)} instead
   */
  protected static Level getLogLevel(JobContext job) {
    return getLogLevel(job.getConfiguration());
  }
  
  protected static Level getLogLevel(Configuration conf) {
    if (conf.get(LOGLEVEL) != null)
      return Level.toLevel(conf.getInt(LOGLEVEL, Level.INFO.toInt()));
    return null;
  }
  
  /**
   * @deprecated Use {@link #getSimulationMode(Configuration)} instead
   */
  protected static boolean getSimulationMode(JobContext job) {
    return getSimulationMode(job.getConfiguration());
  }
  
  protected static boolean getSimulationMode(Configuration conf) {
    return conf.getBoolean(SIMULATE, false);
  }
  
  private static class AccumuloRecordWriter extends RecordWriter<Text,Mutation> {
    private MultiTableBatchWriter mtbw = null;
    private HashMap<Text,BatchWriter> bws = null;
    private Text defaultTableName = null;
    
    private boolean simulate = false;
    private boolean createTables = false;
    
    private long mutCount = 0;
    private long valCount = 0;
    
    private Connector conn;
    
    AccumuloRecordWriter(TaskAttemptContext attempt) throws AccumuloException, AccumuloSecurityException {
      Level l = getLogLevel(attempt);
      if (l != null)
        log.setLevel(getLogLevel(attempt));
      this.simulate = getSimulationMode(attempt);
      this.createTables = canCreateTables(attempt);
      
      if (simulate)
        log.info("Simulating output only. No writes to tables will occur");
      
      this.bws = new HashMap<Text,BatchWriter>();
      
      String tname = getDefaultTableName(attempt);
      this.defaultTableName = (tname == null) ? null : new Text(tname);
      
      if (!simulate) {
        this.conn = getInstance(attempt).getConnector(getUsername(attempt), getPassword(attempt));
        mtbw = conn.createMultiTableBatchWriter(getMaxMutationBufferSize(attempt), getMaxLatency(attempt), getMaxWriteThreads(attempt));
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
          HashSet<String> tables = new HashSet<String>();
          for (KeyExtent ke : e.getAuthorizationFailures()) {
            tables.add(ke.getTableId().toString());
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
    Configuration conf = job.getConfiguration();
    if (!conf.getBoolean(OUTPUT_INFO_HAS_BEEN_SET, false))
      throw new IOException("Output info has not been set.");
    if (!conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
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
}

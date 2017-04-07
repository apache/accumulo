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
package org.apache.accumulo.test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.test.util.SerializationUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that opens a BatchWriter and writes to another table.
 * <p>
 * For each entry passed to this iterator, this writes a certain number of entries with the same key to another table and passes the entry downstream of this
 * iterator with its value replaced by either "{@value #SUCCESS_STRING}" or a description of what failed. Success counts as all entries writing to the result
 * table within a timeout period. Failure counts as one of the entries taking longer than the timeout period.
 * <p>
 * Configure this iterator by calling the static {@link #iteratorSetting} method.
 */
public class BatchWriterIterator extends WrappingIterator {
  private static final Logger log = LoggerFactory.getLogger(BatchWriterIterator.class);

  private Map<String,String> originalOptions; // remembered for deepCopy

  private int sleepAfterFirstWrite = 0;
  private int numEntriesToWritePerEntry = 10;
  private long batchWriterTimeout = 0;
  private long batchWriterMaxMemory = 0;
  private boolean clearCacheAfterFirstWrite = false;
  private boolean splitAfterFirstWrite = false;

  public static final String OPT_sleepAfterFirstWrite = "sleepAfterFirstWrite", OPT_numEntriesToWritePerEntry = "numEntriesToWritePerEntry",
      OPT_batchWriterTimeout = "batchWriterTimeout", OPT_batchWriterMaxMemory = "batchWriterMaxMemory",
      OPT_clearCacheAfterFirstWrite = "clearCacheAfterFirstWrite", OPT_splitAfterFirstWrite = "splitAfterFirstWrite";

  private String instanceName;
  private String tableName;
  private String zookeeperHost;
  private int zookeeperTimeout = -1;
  private String username;
  private AuthenticationToken auth = null;

  public static final String ZOOKEEPERHOST = "zookeeperHost", INSTANCENAME = "instanceName", TABLENAME = "tableName", USERNAME = "username",
      ZOOKEEPERTIMEOUT = "zookeeperTimeout", AUTHENTICATION_TOKEN = "authenticationToken", // base64 encoding of token
      AUTHENTICATION_TOKEN_CLASS = "authenticationTokenClass"; // class of token

  private BatchWriter batchWriter;
  private boolean firstWrite = true;
  private Value topValue = null;
  private Connector connector;

  public static final String SUCCESS_STRING = "success";
  public static final Value SUCCESS_VALUE = new Value(SUCCESS_STRING.getBytes());

  public static IteratorSetting iteratorSetting(int priority, int sleepAfterFirstWrite, long batchWriterTimeout, long batchWriterMaxMemory,
      int numEntriesToWrite, String tableName, Connector connector, AuthenticationToken token, boolean clearCacheAfterFirstWrite, boolean splitAfterFirstWrite) {
    return iteratorSetting(priority, sleepAfterFirstWrite, batchWriterTimeout, batchWriterMaxMemory, numEntriesToWrite, tableName, connector.getInstance()
        .getZooKeepers(), connector.getInstance().getInstanceName(), connector.getInstance().getZooKeepersSessionTimeOut(), connector.whoami(), token,
        clearCacheAfterFirstWrite, splitAfterFirstWrite);
  }

  public static IteratorSetting iteratorSetting(int priority, int sleepAfterFirstWrite, long batchWriterTimeout, long batchWriterMaxMemory,
      int numEntriesToWrite, String tableName, String zookeeperHost, String instanceName, int zookeeperTimeout, String username, AuthenticationToken token,
      boolean clearCacheAfterFirstWrite, boolean splitAfterFirstWrite) {
    IteratorSetting itset = new IteratorSetting(priority, BatchWriterIterator.class);
    itset.addOption(OPT_sleepAfterFirstWrite, Integer.toString(sleepAfterFirstWrite));
    itset.addOption(OPT_numEntriesToWritePerEntry, Integer.toString(numEntriesToWrite));
    itset.addOption(OPT_batchWriterTimeout, Long.toString(batchWriterTimeout));
    itset.addOption(OPT_batchWriterMaxMemory, Long.toString(batchWriterMaxMemory));
    itset.addOption(OPT_clearCacheAfterFirstWrite, Boolean.toString(clearCacheAfterFirstWrite));
    itset.addOption(OPT_splitAfterFirstWrite, Boolean.toString(splitAfterFirstWrite));

    itset.addOption(TABLENAME, tableName);
    itset.addOption(ZOOKEEPERHOST, zookeeperHost);
    itset.addOption(ZOOKEEPERTIMEOUT, Integer.toString(zookeeperTimeout));
    itset.addOption(INSTANCENAME, instanceName);
    itset.addOption(USERNAME, username);
    itset.addOption(AUTHENTICATION_TOKEN_CLASS, token.getClass().getName());
    itset.addOption(AUTHENTICATION_TOKEN, SerializationUtil.serializeWritableBase64(token));

    return itset;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    parseOptions(options);
    initBatchWriter();
  }

  private void parseOptions(Map<String,String> options) {
    this.originalOptions = new HashMap<>(options);

    if (options.containsKey(OPT_numEntriesToWritePerEntry))
      numEntriesToWritePerEntry = Integer.parseInt(options.get(OPT_numEntriesToWritePerEntry));
    if (options.containsKey(OPT_sleepAfterFirstWrite))
      sleepAfterFirstWrite = Integer.parseInt(options.get(OPT_sleepAfterFirstWrite));
    if (options.containsKey(OPT_batchWriterTimeout))
      batchWriterTimeout = Long.parseLong(options.get(OPT_batchWriterTimeout));
    if (options.containsKey(OPT_batchWriterMaxMemory))
      batchWriterMaxMemory = Long.parseLong(options.get(OPT_batchWriterMaxMemory));
    if (options.containsKey(OPT_clearCacheAfterFirstWrite))
      clearCacheAfterFirstWrite = Boolean.parseBoolean(options.get(OPT_clearCacheAfterFirstWrite));
    if (options.containsKey(OPT_splitAfterFirstWrite))
      splitAfterFirstWrite = Boolean.parseBoolean(options.get(OPT_splitAfterFirstWrite));

    instanceName = options.get(INSTANCENAME);
    tableName = options.get(TABLENAME);
    zookeeperHost = options.get(ZOOKEEPERHOST);
    zookeeperTimeout = Integer.parseInt(options.get(ZOOKEEPERTIMEOUT));
    username = options.get(USERNAME);
    String authClass = options.get(AUTHENTICATION_TOKEN_CLASS);
    String authString = options.get(AUTHENTICATION_TOKEN);
    auth = SerializationUtil.subclassNewInstance(authClass, AuthenticationToken.class);
    SerializationUtil.deserializeWritableBase64(auth, authString);
  }

  private void initBatchWriter() {
    ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeeperHost).withZkTimeout(zookeeperTimeout);
    Instance instance = new ZooKeeperInstance(cc);
    try {
      connector = instance.getConnector(username, auth);
    } catch (Exception e) {
      log.error("failed to connect to Accumulo instance " + instanceName, e);
      throw new RuntimeException(e);
    }

    BatchWriterConfig bwc = new BatchWriterConfig();
    bwc.setMaxMemory(batchWriterMaxMemory);
    bwc.setTimeout(batchWriterTimeout, TimeUnit.SECONDS);

    try {
      batchWriter = connector.createBatchWriter(tableName, bwc);
    } catch (TableNotFoundException e) {
      log.error(tableName + " does not exist in instance " + instanceName, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Write numEntriesToWritePerEntry. Flush. Set topValue accordingly.
   */
  private void processNext() {
    assert hasTop();
    Key k = getTopKey();
    Text row = k.getRow(), cf = k.getColumnFamily(), cq = k.getColumnQualifier();
    Value v = super.getTopValue();
    String failure = null;
    try {
      for (int i = 0; i < numEntriesToWritePerEntry; i++) {
        Mutation m = new Mutation(row);
        m.put(cf, cq, v);
        batchWriter.addMutation(m);

        if (firstWrite) {
          batchWriter.flush();
          if (clearCacheAfterFirstWrite)
            TabletLocator.clearLocators();
          if (splitAfterFirstWrite) {
            SortedSet<Text> splits = new TreeSet<>();
            splits.add(new Text(row));
            connector.tableOperations().addSplits(tableName, splits);
          }
          if (sleepAfterFirstWrite > 0)
            try {
              Thread.sleep(sleepAfterFirstWrite);
            } catch (InterruptedException ignored) {}
          firstWrite = false;
        }
      }

      batchWriter.flush();
    } catch (Exception e) {
      // in particular: watching for TimedOutException
      log.error("Problem while BatchWriting to target table " + tableName, e);
      failure = e.getClass().getSimpleName() + ": " + e.getMessage();
    }
    topValue = failure == null ? SUCCESS_VALUE : new Value(failure.getBytes());
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    try {
      batchWriter.close();
    } catch (MutationsRejectedException e) {
      log.error("Failed to close BatchWriter; some mutations may not be applied", e);
    }
  }

  @Override
  public void next() throws IOException {
    super.next();
    if (hasTop())
      processNext();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    super.seek(range, columnFamilies, inclusive);
    if (hasTop())
      processNext();
  }

  @Override
  public Value getTopValue() {
    return topValue;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    BatchWriterIterator newInstance;
    try {
      newInstance = this.getClass().newInstance();
      newInstance.init(getSource().deepCopy(env), originalOptions, env);
      return newInstance;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

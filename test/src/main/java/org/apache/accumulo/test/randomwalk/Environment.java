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
package org.apache.accumulo.test.randomwalk;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The test environment that is available for randomwalk tests. This includes configuration properties that are available to any randomwalk test and facilities
 * for creating client-side objects. This class is not thread-safe.
 */
public class Environment {
  /**
   * The configuration property key for a username.
   */
  public static final String KEY_USERNAME = "USERNAME";
  /**
   * The configuration property key for a password.
   */
  public static final String KEY_PASSWORD = "PASSWORD";
  /**
   * The configuration property key for a keytab
   */
  public static final String KEY_KEYTAB = "KEYTAB";
  /**
   * The configuration property key for the instance name.
   */
  public static final String KEY_INSTANCE = "INSTANCE";
  /**
   * The configuration property key for the comma-separated list of ZooKeepers.
   */
  public static final String KEY_ZOOKEEPERS = "ZOOKEEPERS";
  /**
   * The configuration property key for the maximum memory for the multi-table batch writer.
   */
  public static final String KEY_MAX_MEM = "MAX_MEM";
  /**
   * The configuration property key for the maximum latency, in milliseconds, for the multi-table batch writer.
   */
  public static final String KEY_MAX_LATENCY = "MAX_LATENCY";
  /**
   * The configuration property key for the number of write threads for the multi-table batch writer.
   */
  public static final String KEY_NUM_THREADS = "NUM_THREADS";

  private static final Logger log = LoggerFactory.getLogger(Environment.class);

  private final Properties p;
  private Instance instance = null;
  private Connector connector = null;
  private MultiTableBatchWriter mtbw = null;

  /**
   * Creates a new test environment.
   *
   * @param p
   *          configuration properties
   * @throws NullPointerException
   *           if p is null
   */
  public Environment(Properties p) {
    checkNotNull(p);
    this.p = p;
  }

  /**
   * Gets a copy of the configuration properties.
   *
   * @return a copy of the configuration properties
   */
  Properties copyConfigProperties() {
    return new Properties(p);
  }

  /**
   * Gets a configuration property.
   *
   * @param key
   *          key
   * @return property value
   */
  public String getConfigProperty(String key) {
    return p.getProperty(key);
  }

  /**
   * Gets the configured username.
   *
   * @return username
   */
  public String getUserName() {
    return p.getProperty(KEY_USERNAME);
  }

  /**
   * Gets the configured password.
   *
   * @return password
   */
  public String getPassword() {
    return p.getProperty(KEY_PASSWORD);
  }

  /**
   * Gets the configured keytab.
   *
   * @return path to keytab
   */
  public String getKeytab() {
    return p.getProperty(KEY_KEYTAB);
  }

  /**
   * Gets this process's ID.
   *
   * @return pid
   */
  public String getPid() {
    return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
  }

  /**
   * Gets an authentication token based on the configured password.
   *
   * @return authentication token
   */
  public AuthenticationToken getToken() {
    String password = getPassword();
    if (null != password) {
      return new PasswordToken(getPassword());
    }
    String keytab = getKeytab();
    if (null != keytab) {
      File keytabFile = new File(keytab);
      if (!keytabFile.exists() || !keytabFile.isFile()) {
        throw new IllegalArgumentException("Provided keytab is not a normal file: " + keytab);
      }
      try {
        return new KerberosToken(getUserName(), keytabFile, true);
      } catch (IOException e) {
        throw new RuntimeException("Failed to login", e);
      }
    }
    throw new IllegalArgumentException("Must provide password or keytab in configuration");
  }

  /**
   * Gets an Accumulo instance object. The same instance is reused after the first call.
   *
   * @return instance
   */
  public Instance getInstance() {
    if (instance == null) {
      String instance = p.getProperty(KEY_INSTANCE);
      String zookeepers = p.getProperty(KEY_ZOOKEEPERS);
      this.instance = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(instance).withZkHosts(zookeepers));
    }
    return instance;
  }

  /**
   * Gets an Accumulo connector. The same connector is reused after the first call.
   *
   * @return connector
   */
  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    if (connector == null) {
      connector = getInstance().getConnector(getUserName(), getToken());
    }
    return connector;
  }

  /**
   * Gets a multitable batch writer. The same object is reused after the first call unless it is reset.
   *
   * @return multitable batch writer
   * @throws NumberFormatException
   *           if any of the numeric batch writer configuration properties cannot be parsed
   * @throws NumberFormatException
   *           if any configuration property cannot be parsed
   */
  public MultiTableBatchWriter getMultiTableBatchWriter() throws AccumuloException, AccumuloSecurityException {
    if (mtbw == null) {
      long maxMem = Long.parseLong(p.getProperty(KEY_MAX_MEM));
      long maxLatency = Long.parseLong(p.getProperty(KEY_MAX_LATENCY));
      int numThreads = Integer.parseInt(p.getProperty(KEY_NUM_THREADS));
      mtbw = getConnector().createMultiTableBatchWriter(
          new BatchWriterConfig().setMaxMemory(maxMem).setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(numThreads));
    }
    return mtbw;
  }

  /**
   * Checks if a multitable batch writer has been created by this wrapper.
   *
   * @return true if multitable batch writer is already created
   */
  public boolean isMultiTableBatchWriterInitialized() {
    return mtbw != null;
  }

  /**
   * Clears the multitable batch writer previously created and remembered by this wrapper.
   */
  public void resetMultiTableBatchWriter() {
    if (mtbw == null)
      return;
    if (!mtbw.isClosed()) {
      log.warn("Setting non-closed MultiTableBatchWriter to null (leaking resources)");
    }
    mtbw = null;
  }
}

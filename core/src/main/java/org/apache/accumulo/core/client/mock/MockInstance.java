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
package org.apache.accumulo.core.client.mock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

/**
 * Mock Accumulo provides an in memory implementation of the Accumulo client API. It is possible that the behavior of this implementation may differ subtly from
 * the behavior of Accumulo. This could result in unit tests that pass on Mock Accumulo and fail on Accumulo or visa-versa. Documenting the differences would be
 * difficult and is not done.
 *
 * <p>
 * An alternative to Mock Accumulo called MiniAccumuloCluster was introduced in Accumulo 1.5. MiniAccumuloCluster spins up actual Accumulo server processes, can
 * be used for unit testing, and its behavior should match Accumulo. The drawback of MiniAccumuloCluster is that it starts more slowly than Mock Accumulo.
 *
 */

public class MockInstance implements Instance {

  static final String genericAddress = "localhost:1234";
  static final Map<String,MockAccumulo> instances = new HashMap<String,MockAccumulo>();
  MockAccumulo acu;
  String instanceName;

  public MockInstance() {
    acu = new MockAccumulo(getDefaultFileSystem());
    instanceName = "mock-instance";
  }

  static FileSystem getDefaultFileSystem() {
    try {
      Configuration conf = CachedConfiguration.getInstance();
      conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
      conf.set("fs.default.name", "file:///");
      return FileSystem.get(CachedConfiguration.getInstance());
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public MockInstance(String instanceName) {
    this(instanceName, getDefaultFileSystem());
  }

  public MockInstance(String instanceName, FileSystem fs) {
    synchronized (instances) {
      if (instances.containsKey(instanceName))
        acu = instances.get(instanceName);
      else
        instances.put(instanceName, acu = new MockAccumulo(fs));
    }
    this.instanceName = instanceName;
  }

  @Override
  public String getRootTabletLocation() {
    return genericAddress;
  }

  @Override
  public List<String> getMasterLocations() {
    return Collections.singletonList(genericAddress);
  }

  @Override
  public String getInstanceID() {
    return "mock-instance-id";
  }

  @Override
  public String getInstanceName() {
    return instanceName;
  }

  @Override
  public String getZooKeepers() {
    return "localhost";
  }

  @Override
  public int getZooKeepersSessionTimeOut() {
    return 30 * 1000;
  }

  @Override
  @Deprecated
  public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, new PasswordToken(pass));
  }

  @Override
  @Deprecated
  public Connector getConnector(String user, ByteBuffer pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, ByteBufferUtil.toBytes(pass));
  }

  @Override
  @Deprecated
  public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, TextUtil.getBytes(new Text(pass.toString())));
  }

  AccumuloConfiguration conf = null;

  @Deprecated
  @Override
  public AccumuloConfiguration getConfiguration() {
    if (conf == null)
      conf = AccumuloConfiguration.getDefaultConfiguration();
    return conf;
  }

  @Override
  @Deprecated
  public void setConfiguration(AccumuloConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public Connector getConnector(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    Connector conn = new MockConnector(new Credentials(principal, token), acu, this);
    if (!acu.users.containsKey(principal))
      conn.securityOperations().createLocalUser(principal, (PasswordToken) token);
    else if (!acu.users.get(principal).token.equals(token))
      throw new AccumuloSecurityException(principal, SecurityErrorCode.BAD_CREDENTIALS);
    return conn;
  }
}

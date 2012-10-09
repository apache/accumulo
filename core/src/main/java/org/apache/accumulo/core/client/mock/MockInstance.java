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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

public class MockInstance implements Instance {
  
  static final String genericAddress = "localhost:1234";
  static final Map<String,MockAccumulo> instances = new HashMap<String,MockAccumulo>();
  MockAccumulo acu;
  String instanceName;
  
  public MockInstance() {
    acu = new MockAccumulo();
    instanceName = "mock-instance";
  }
  
  public MockInstance(String instanceName) {
    synchronized (instances) {
      if (instances.containsKey(instanceName))
        acu = instances.get(instanceName);
      else
        instances.put(instanceName, acu = new MockAccumulo());
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
  public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
    Connector conn = new MockConnector(user, acu, this);
    conn.securityOperations().createUser(user, pass, new Authorizations());
    return conn;
  }
  
  @Override
  public Connector getConnector(String user, ByteBuffer pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, ByteBufferUtil.toBytes(pass));
  }
  
  @Override
  public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, TextUtil.getBytes(new Text(pass.toString())));
  }
  
  AccumuloConfiguration conf = null;
  
  @Override
  public AccumuloConfiguration getConfiguration() {
    if (conf == null)
      conf = AccumuloConfiguration.getDefaultConfiguration();
    return conf;
  }
  
  @Override
  public void setConfiguration(AccumuloConfiguration conf) {
    this.conf = conf;
  }
  
  @Override
  public Connector getConnector(AuthInfo auth) throws AccumuloException, AccumuloSecurityException {
    return getConnector(auth.user, auth.password);
  }
}

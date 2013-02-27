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
package org.apache.accumulo.core.cli;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.security.thrift.Credential;
import org.apache.accumulo.core.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.tokens.SecurityToken;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

public class ClientOpts extends Help {
  
  public static class TimeConverter implements IStringConverter<Long> {
    @Override
    public Long convert(String value) {
      return AccumuloConfiguration.getTimeInMillis(value);
    }
  }
  
  public static class MemoryConverter implements IStringConverter<Long> {
    @Override
    public Long convert(String value) {
      return AccumuloConfiguration.getMemoryInBytes(value);
    }
  }
  
  public static class AuthConverter implements IStringConverter<Authorizations> {
    @Override
    public Authorizations convert(String value) {
      return new Authorizations(value.split(","));
    }
  }
  
  public static class Password {
    public byte[] value;
    
    public Password(String dfault) {
      value = dfault.getBytes(Charset.forName("UTF-8"));
    }
    
    @Override
    public String toString() {
      return new String(value);
    }
  }
  
  public static class PasswordConverter implements IStringConverter<Password> {
    @Override
    public Password convert(String value) {
      return new Password(value);
    }
  }
  
  public static class VisibilityConverter implements IStringConverter<ColumnVisibility> {
    @Override
    public ColumnVisibility convert(String value) {
      return new ColumnVisibility(value);
    }
  }
  
  @Parameter(names = {"-u", "--user"}, description = "Connection user")
  public String principal = System.getProperty("user.name");
  
  @Parameter(names = "-p", converter = PasswordConverter.class, description = "Connection password")
  public Password password = null;
  
  @Parameter(names = "--password", converter = PasswordConverter.class, description = "Enter the connection password", password = true)
  public Password securePassword = null;
  
  public SecurityToken getToken() {
    PasswordToken pt = new PasswordToken();
    if (securePassword == null) {
      if (password == null)
        return null;
      return pt.setPassword(password.value);
    }
    return pt.setPassword(securePassword.value);
  }
  
  @Parameter(names = {"-z", "--keepers"}, description = "Comma separated list of zookeeper hosts (host:port,host:port)")
  public String zookeepers = "localhost:2181";
  
  @Parameter(names = {"-i", "--instance"}, description = "The name of the accumulo instance")
  public String instance = null;
  
  @Parameter(names = {"-auths", "--auths"}, converter = AuthConverter.class, description = "the authorizations to use when reading or writing")
  public Authorizations auths = Constants.NO_AUTHS;
  
  @Parameter(names = "--debug", description = "turn on TRACE-level log messages")
  public boolean debug = false;
  
  @Parameter(names = {"-fake", "--mock"}, description = "Use a mock Instance")
  public boolean mock = false;
  
  @Parameter(names = "--site-file", description = "Read the given accumulo site file to find the accumulo instance")
  public String siteFile = null;
  
  public void startDebugLogging() {
    if (debug)
      Logger.getLogger(Constants.CORE_PACKAGE_NAME).setLevel(Level.TRACE);
  }
  
  @Parameter(names = "--trace", description = "turn on distributed tracing")
  public boolean trace = false;
  
  public void startTracing(String applicationName) {
    if (trace) {
      Trace.on(applicationName);
    }
  }
  
  public void stopTracing() {
    Trace.off();
  }
  
  @Override
  public void parseArgs(String programName, String[] args, Object... others) {
    super.parseArgs(programName, args, others);
    startDebugLogging();
    startTracing(programName);
  }
  
  protected Instance cachedInstance = null;
  
  @SuppressWarnings("deprecation")
  synchronized public Instance getInstance() {
    if (cachedInstance != null)
      return cachedInstance;
    if (mock)
      return cachedInstance = new MockInstance(instance);
    if (siteFile != null) {
      AccumuloConfiguration config = new AccumuloConfiguration() {
        Configuration xml = new Configuration();
        {
          xml.addResource(new Path(siteFile));
        }
        
        @Override
        public Iterator<Entry<String,String>> iterator() {
          TreeMap<String,String> map = new TreeMap<String,String>();
          for (Entry<String,String> props : DefaultConfiguration.getInstance())
            map.put(props.getKey(), props.getValue());
          for (Entry<String,String> props : xml)
            map.put(props.getKey(), props.getValue());
          return map.entrySet().iterator();
        }
        
        @Override
        public String get(Property property) {
          String value = xml.get(property.getKey());
          if (value != null)
            return value;
          return DefaultConfiguration.getInstance().get(property);
        }
      };
      this.zookeepers = config.get(Property.INSTANCE_ZK_HOST);
      Path instanceDir = new Path(config.get(Property.INSTANCE_DFS_DIR), "instance_id");
      return cachedInstance = new ZooKeeperInstance(UUID.fromString(ZooKeeperInstance.getInstanceIDFromHdfs(instanceDir)), zookeepers);
    }
    return cachedInstance = new ZooKeeperInstance(this.instance, this.zookeepers);
  }
  
  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    return getInstance().getConnector(this.principal, this.getToken());
  }
  
  public Credential getCredentials() throws AccumuloSecurityException {
    return CredentialHelper.create(principal, getToken(), getInstance().getInstanceID());
  }
  
  public void setAccumuloConfigs(Job job) throws AccumuloSecurityException {
    AccumuloInputFormat.setZooKeeperInstance(job, instance, zookeepers);
    AccumuloOutputFormat.setZooKeeperInstance(job, instance, zookeepers);
  }
  
}

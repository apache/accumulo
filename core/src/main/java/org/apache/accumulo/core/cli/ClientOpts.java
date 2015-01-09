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

import static com.google.common.base.Charsets.UTF_8;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.Properties;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.DynamicParameter;
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
      value = dfault.getBytes(UTF_8);
    }

    @Override
    public String toString() {
      return new String(value, UTF_8);
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

  @Parameter(names = {"-tc", "--tokenClass"}, description = "Token class")
  public String tokenClassName = PasswordToken.class.getName();

  @DynamicParameter(names = "-l",
      description = "login properties in the format key=value. Reuse -l for each property (prompt for properties if this option is missing")
  public Map<String,String> loginProps = new LinkedHashMap<String,String>();

  public AuthenticationToken getToken() {
    if (!loginProps.isEmpty()) {
      Properties props = new Properties();
      for (Entry<String,String> loginOption : loginProps.entrySet())
        props.put(loginOption.getKey(), loginOption.getValue());

      try {
        AuthenticationToken token = Class.forName(tokenClassName).asSubclass(AuthenticationToken.class).newInstance();
        token.init(props);
        return token;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }

    if (securePassword != null)
      return new PasswordToken(securePassword.value);

    if (password != null)
      return new PasswordToken(password.value);

    return null;
  }

  @Parameter(names = {"-z", "--keepers"}, description = "Comma separated list of zookeeper hosts (host:port,host:port)")
  public String zookeepers = "localhost:2181";

  @Parameter(names = {"-i", "--instance"}, description = "The name of the accumulo instance")
  public String instance = null;

  @Parameter(names = {"-auths", "--auths"}, converter = AuthConverter.class, description = "the authorizations to use when reading or writing")
  public Authorizations auths = Authorizations.EMPTY;

  @Parameter(names = "--debug", description = "turn on TRACE-level log messages")
  public boolean debug = false;

  @Parameter(names = {"-fake", "--mock"}, description = "Use a mock Instance")
  public boolean mock = false;

  @Parameter(names = "--site-file", description = "Read the given accumulo site file to find the accumulo instance")
  public String siteFile = null;

  @Parameter(names = "--ssl", description = "Connect to accumulo over SSL")
  public boolean sslEnabled = false;

  @Parameter(names = "--config-file", description = "Read the given client config file. "
      + "If omitted, the path searched can be specified with $ACCUMULO_CLIENT_CONF_PATH, "
      + "which defaults to ~/.accumulo/config:$ACCUMULO_CONF_DIR/client.conf:/etc/accumulo/client.conf")
  public String clientConfigFile = null;

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
  protected ClientConfiguration cachedClientConfig = null;

  synchronized public Instance getInstance() {
    if (cachedInstance != null)
      return cachedInstance;
    if (mock)
      return cachedInstance = new MockInstance(instance);
    return cachedInstance = new ZooKeeperInstance(this.getClientConfiguration());
  }

  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    if (this.principal == null || this.getToken() == null)
      throw new AccumuloSecurityException("You must provide a user (-u) and password (-p)", SecurityErrorCode.BAD_CREDENTIALS);
    return getInstance().getConnector(principal, getToken());
  }

  public void setAccumuloConfigs(Job job) throws AccumuloSecurityException {
    AccumuloInputFormat.setZooKeeperInstance(job, this.getClientConfiguration());
    AccumuloOutputFormat.setZooKeeperInstance(job, this.getClientConfiguration());
  }

  protected ClientConfiguration getClientConfiguration() throws IllegalArgumentException {
    if (cachedClientConfig != null)
      return cachedClientConfig;

    ClientConfiguration clientConfig;
    try {
      if (clientConfigFile == null)
        clientConfig = ClientConfiguration.loadDefault();
      else
        clientConfig = new ClientConfiguration(new PropertiesConfiguration(clientConfigFile));
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
    if (sslEnabled)
      clientConfig.setProperty(ClientProperty.INSTANCE_RPC_SSL_ENABLED, "true");
    if (siteFile != null) {
      AccumuloConfiguration config = new AccumuloConfiguration() {
        Configuration xml = new Configuration();
        {
          xml.addResource(new Path(siteFile));
        }

        @Override
        public void getProperties(Map<String,String> props, PropertyFilter filter) {
          for (Entry<String,String> prop : DefaultConfiguration.getInstance())
            if (filter.accept(prop.getKey()))
              props.put(prop.getKey(), prop.getValue());
          for (Entry<String,String> prop : xml)
            if (filter.accept(prop.getKey()))
              props.put(prop.getKey(), prop.getValue());
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

      String volDir = VolumeConfiguration.getVolumeUris(config)[0];
      Path instanceDir = new Path(volDir, "instance_id");
      String instanceIDFromFile = ZooUtil.getInstanceIDFromHdfs(instanceDir, config);
      if (config.getBoolean(Property.INSTANCE_RPC_SSL_ENABLED))
        clientConfig.setProperty(ClientProperty.INSTANCE_RPC_SSL_ENABLED, "true");
      return cachedClientConfig = clientConfig.withInstance(UUID.fromString(instanceIDFromFile)).withZkHosts(zookeepers);
    }
    return cachedClientConfig = clientConfig.withInstance(instance).withZkHosts(zookeepers);
  }

}

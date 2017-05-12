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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.Properties;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.util.DeprecationUtil;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import jline.console.ConsoleReader;

public class ClientOpts extends Help {

  public static class TimeConverter implements IStringConverter<Long> {
    @Override
    public Long convert(String value) {
      return ConfigurationTypeHelper.getTimeInMillis(value);
    }
  }

  public static class MemoryConverter implements IStringConverter<Long> {
    @Override
    public Long convert(String value) {
      return ConfigurationTypeHelper.getFixedMemoryAsBytes(value);
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

    /**
     * Prompts user for a password
     *
     * @return user entered Password object, null if no console exists
     */
    public static Password promptUser() throws IOException {
      if (System.console() == null) {
        throw new IOException("Attempted to prompt user on the console when System.console = null");
      }
      ConsoleReader reader = new ConsoleReader();
      String enteredPass = reader.readLine("Enter password: ", '*');
      return new Password(enteredPass);
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
  private String principal = null;

  @Parameter(names = "-p", converter = PasswordConverter.class, description = "Connection password")
  private Password password = null;

  @Parameter(names = "--password", converter = PasswordConverter.class, description = "Enter the connection password", password = true)
  private Password securePassword = null;

  @Parameter(names = {"-tc", "--tokenClass"}, description = "Token class")
  private String tokenClassName = null;

  @DynamicParameter(names = "-l",
      description = "login properties in the format key=value. Reuse -l for each property (prompt for properties if this option is missing")
  public Map<String,String> loginProps = new LinkedHashMap<>();

  public AuthenticationToken getToken() {
    if (null != tokenClassName) {
      final Properties props = new Properties();
      if (!loginProps.isEmpty()) {
        for (Entry<String,String> loginOption : loginProps.entrySet())
          props.put(loginOption.getKey(), loginOption.getValue());
      }

      // It's expected that the user is already logged in via UserGroupInformation or external to this program (kinit).
      try {
        AuthenticationToken token = Class.forName(tokenClassName).asSubclass(AuthenticationToken.class).newInstance();
        token.init(props);
        return token;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    // other token types should have resolved by this point, so return PasswordToken
    Password pass = null;
    if (securePassword != null) {
      pass = securePassword;
    } else if (password != null) {
      pass = password;
    } else {
      try {
        pass = Password.promptUser();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return new PasswordToken(pass.value);
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

  @Parameter(names = "--sasl", description = "Connecto to Accumulo using SASL (supports Kerberos)")
  public boolean saslEnabled = false;

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

  @Parameter(names = "--keytab", description = "Kerberos keytab on the local filesystem")
  public String keytabPath = null;

  public void startTracing(String applicationName) {
    if (trace) {
      Trace.on(applicationName);
    }
  }

  public void stopTracing() {
    Trace.off();
  }

  /**
   * Automatically update the options to use a KerberosToken when SASL is enabled for RPCs. Don't overwrite the options if the user has provided something
   * specifically.
   */
  public void updateKerberosCredentials() {
    ClientConfiguration clientConfig;
    try {
      if (clientConfigFile == null)
        clientConfig = ClientConfiguration.loadDefault();
      else
        clientConfig = new ClientConfiguration(clientConfigFile);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
    updateKerberosCredentials(clientConfig);
  }

  /**
   * Automatically update the options to use a KerberosToken when SASL is enabled for RPCs. Don't overwrite the options if the user has provided something
   * specifically.
   */
  public void updateKerberosCredentials(ClientConfiguration clientConfig) {
    final boolean clientConfSaslEnabled = Boolean.parseBoolean(clientConfig.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));
    if ((saslEnabled || clientConfSaslEnabled) && null == tokenClassName) {
      tokenClassName = KerberosToken.CLASS_NAME;
      // ACCUMULO-3701 We need to ensure we're logged in before parseArgs returns as the MapReduce Job is going to make a copy of the current user (UGI)
      // when it is instantiated.
      if (null != keytabPath) {
        File keytab = new File(keytabPath);
        if (!keytab.exists() || !keytab.isFile()) {
          throw new IllegalArgumentException("Keytab isn't a normal file: " + keytabPath);
        }
        if (null == principal) {
          throw new IllegalArgumentException("Principal must be provided if logging in via Keytab");
        }
        try {
          UserGroupInformation.loginUserFromKeytab(principal, keytab.getAbsolutePath());
        } catch (IOException e) {
          throw new RuntimeException("Failed to log in with keytab", e);
        }
      }
    }
  }

  @Override
  public void parseArgs(String programName, String[] args, Object... others) {
    super.parseArgs(programName, args, others);
    startDebugLogging();
    startTracing(programName);
    updateKerberosCredentials();
  }

  protected Instance cachedInstance = null;
  protected ClientConfiguration cachedClientConfig = null;

  synchronized public Instance getInstance() {
    if (cachedInstance != null)
      return cachedInstance;
    if (mock)
      return cachedInstance = DeprecationUtil.makeMockInstance(instance);
    return cachedInstance = new ZooKeeperInstance(this.getClientConfiguration());
  }

  public String getPrincipal() throws AccumuloSecurityException {
    if (null == principal) {
      AuthenticationToken token = getToken();
      if (null == token) {
        throw new AccumuloSecurityException("No principal or authentication token was provided", SecurityErrorCode.BAD_CREDENTIALS);
      }

      // In MapReduce, if we create a DelegationToken, the principal is updated from the KerberosToken
      // used to obtain the DelegationToken.
      if (null != principal) {
        return principal;
      }

      // Try to extract the principal automatically from Kerberos
      if (token instanceof KerberosToken) {
        principal = ((KerberosToken) token).getPrincipal();
      } else {
        principal = System.getProperty("user.name");
      }
    }
    return principal;
  }

  public void setPrincipal(String principal) {
    this.principal = principal;
  }

  public Password getPassword() {
    return password;
  }

  public void setPassword(Password password) {
    this.password = password;
  }

  public Password getSecurePassword() {
    return securePassword;
  }

  public void setSecurePassword(Password securePassword) {
    this.securePassword = securePassword;
  }

  public String getTokenClassName() {
    return tokenClassName;
  }

  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    return getInstance().getConnector(getPrincipal(), getToken());
  }

  public ClientConfiguration getClientConfiguration() throws IllegalArgumentException {
    if (cachedClientConfig != null)
      return cachedClientConfig;

    ClientConfiguration clientConfig;
    try {
      if (clientConfigFile == null)
        clientConfig = ClientConfiguration.loadDefault();
      else
        clientConfig = new ClientConfiguration(clientConfigFile);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
    if (sslEnabled)
      clientConfig.setProperty(ClientProperty.INSTANCE_RPC_SSL_ENABLED, "true");

    if (saslEnabled)
      clientConfig.setProperty(ClientProperty.INSTANCE_RPC_SASL_ENABLED, "true");

    if (siteFile != null) {
      AccumuloConfiguration config = new AccumuloConfiguration() {
        Configuration xml = new Configuration();
        {
          xml.addResource(new Path(siteFile));
        }

        @Override
        public void getProperties(Map<String,String> props, Predicate<String> filter) {
          for (Entry<String,String> prop : DefaultConfiguration.getInstance())
            if (filter.test(prop.getKey()))
              props.put(prop.getKey(), prop.getValue());
          for (Entry<String,String> prop : xml)
            if (filter.test(prop.getKey()))
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

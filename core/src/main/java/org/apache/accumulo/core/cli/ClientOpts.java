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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConnectionInfo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.util.DeprecationUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

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

  @Parameter(names = "--password", converter = PasswordConverter.class,
      description = "Enter the connection password", password = true)
  private Password securePassword = null;

  public AuthenticationToken getToken() {
    return getConnectionInfo().getAuthenticationToken();
  }

  @Parameter(names = {"-z", "--keepers"},
      description = "Comma separated list of zookeeper hosts (host:port,host:port)")
  public String zookeepers = "localhost:2181";

  @Parameter(names = {"-i", "--instance"}, description = "The name of the accumulo instance")
  public String instance = null;

  @Parameter(names = {"-auths", "--auths"}, converter = AuthConverter.class,
      description = "the authorizations to use when reading or writing")
  public Authorizations auths = Authorizations.EMPTY;

  @Parameter(names = "--debug", description = "turn on TRACE-level log messages")
  public boolean debug = false;

  @Parameter(names = {"-fake", "--mock"}, description = "Use a mock Instance")
  public boolean mock = false;

  @Parameter(names = "--ssl", description = "Connect to accumulo over SSL")
  public boolean sslEnabled = false;

  @Parameter(names = "--sasl", description = "Connecto to Accumulo using SASL (supports Kerberos)")
  public boolean saslEnabled = false;

  @Parameter(names = "--config-file", description = "Read the given client config file. "
      + "If omitted, the following paths will be searched ~/.accumulo/accumulo-client.properties:"
      + "$ACCUMULO_CONF_DIR/accumulo-client.properties:/etc/accumulo/accumulo-client.properties")
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
   * Automatically update the options to use a KerberosToken when SASL is enabled for RPCs. Don't
   * overwrite the options if the user has provided something specifically.
   */
  public void updateKerberosCredentials(String clientConfigFile) {
    boolean saslEnabled = false;
    if (clientConfigFile != null) {
      saslEnabled = Connector.builder().usingProperties(clientConfigFile).info().saslEnabled();
    }
    updateKerberosCredentials(saslEnabled);
  }

  public void updateKerberosCredentials() {
    updateKerberosCredentials(true);
  }

  /**
   * Automatically update the options to use a KerberosToken when SASL is enabled for RPCs. Don't
   * overwrite the options if the user has provided something specifically.
   */
  public void updateKerberosCredentials(boolean clientSaslEnabled) {
    if (saslEnabled || clientSaslEnabled) {
      // ACCUMULO-3701 We need to ensure we're logged in before parseArgs returns as the MapReduce
      // Job is going to make a copy of the current user (UGI)
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
    updateKerberosCredentials(clientConfigFile);
  }

  private  ConnectionInfo cachedInfo = null;
  private Connector cachedConnector = null;
  protected Instance cachedInstance = null;
  private Properties cachedProps = null;

  synchronized public Instance getInstance() {
    if (cachedInstance == null) {
      if (mock) {
        cachedInstance = DeprecationUtil.makeMockInstance(instance);
      } else {
        try {
          cachedInstance = getConnector().getInstance();
        } catch (AccumuloSecurityException | AccumuloException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return cachedInstance;
  }

  public String getPrincipal() throws AccumuloSecurityException {
    if (null == principal) {
      AuthenticationToken token = getToken();
      if (null == token) {
        throw new AccumuloSecurityException("No principal or authentication token was provided",
            SecurityErrorCode.BAD_CREDENTIALS);
      }

      // In MapReduce, if we create a DelegationToken, the principal is updated from the
      // KerberosToken
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

  public ConnectionInfo getConnectionInfo() {
    if (cachedInfo == null) {
      cachedInfo = Connector.builder().usingProperties(getClientProperties()).info();
    }
    return cachedInfo;
  }

  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    if (cachedConnector == null) {
      cachedConnector = Connector.builder().usingConnectionInfo(getConnectionInfo()).build();
    }
    return cachedConnector;
  }

  public String getClientConfigFile() {
    if (clientConfigFile == null) {
      List<String> searchPaths = new LinkedList<>();
      searchPaths.add(System.getProperty("user.home") + "/.accumulo/accumulo-client.properties");
      if (System.getenv("ACCUMULO_CONF_DIR") != null) {
        searchPaths.add(System.getenv("ACCUMULO_CONF_DIR") + "/accumulo-client.properties");
      }
      searchPaths.add("/etc/accumulo/accumulo-client.properties");
      for (String path : searchPaths) {
        File file = new File(path);
        if (file.isFile() && file.canRead()) {
          clientConfigFile = file.getAbsolutePath();
          break;
        }
      }
    }
    return clientConfigFile;
  }

  public Properties getClientProperties() {
    if (cachedProps == null) {
      cachedProps = new Properties();
      if (getClientConfigFile() != null) {
        try (InputStream is = new FileInputStream(getClientConfigFile())) {
          cachedProps.load(is);
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "Failed to load properties from " + getClientConfigFile());
        }
      }
      if (saslEnabled) {
        cachedProps.setProperty(ClientProperty.SASL_ENABLED.getKey(), "true");
      }
      if (sslEnabled) {
        cachedProps.setProperty(ClientProperty.SSL_ENABLED.getKey(), "true");
      }
      if (principal != null) {
        cachedProps.setProperty(ClientProperty.AUTH_USERNAME.getKey(), principal);
      }

      if (securePassword != null) {
        cachedProps.setProperty(ClientProperty.AUTH_PASSWORD.getKey(), securePassword.toString());
      } else if (password != null) {
        cachedProps.setProperty(ClientProperty.AUTH_PASSWORD.getKey(), password.toString());
      }
    }
    return cachedProps;
  }
}

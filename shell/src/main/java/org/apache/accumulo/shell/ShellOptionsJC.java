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
package org.apache.accumulo.shell;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;

public class ShellOptionsJC {
  private static final Logger log = LoggerFactory.getLogger(ShellOptionsJC.class);

  @Parameter(names = {"-u", "--user"}, description = "username (defaults to your OS user)")
  private String username = null;

  public static class PasswordConverter implements IStringConverter<String> {
    public static final String STDIN = "stdin";

    private enum KeyType {
      PASS("pass:"), ENV("env:") {
        @Override
        String process(String value) {
          return System.getenv(value);
        }
      },
      FILE("file:") {
        @Override
        String process(String value) {
          Scanner scanner = null;
          try {
            scanner = new Scanner(new File(value));
            return scanner.nextLine();
          } catch (FileNotFoundException e) {
            throw new ParameterException(e);
          } finally {
            if (scanner != null) {
              scanner.close();
            }
          }
        }
      },
      STDIN(PasswordConverter.STDIN) {
        @Override
        public boolean matches(String value) {
          return prefix.equals(value);
        }

        @Override
        public String convert(String value) {
          // Will check for this later
          return prefix;
        }
      };

      String prefix;

      private KeyType(String prefix) {
        this.prefix = prefix;
      }

      public boolean matches(String value) {
        return value.startsWith(prefix);
      }

      public String convert(String value) {
        return process(value.substring(prefix.length()));
      }

      String process(String value) {
        return value;
      }
    }

    @Override
    public String convert(String value) {
      for (KeyType keyType : KeyType.values()) {
        if (keyType.matches(value)) {
          return keyType.convert(value);
        }
      }

      return value;
    }
  }

  // Note: Don't use "password = true" because then it will prompt even if we have a token
  @Parameter(names = {"-p", "--password"}, description = "password (can be specified as 'pass:<password>', 'file:<local file containing the password>', "
      + "'env:<variable containing the pass>', or stdin)", converter = PasswordConverter.class)
  private String password;

  public static class TokenConverter implements IStringConverter<AuthenticationToken> {
    @Override
    public AuthenticationToken convert(String value) {
      try {
        return Class.forName(value).asSubclass(AuthenticationToken.class).newInstance();
      } catch (Exception e) {
        // Catching ClassNotFoundException, ClassCastException, InstantiationException and IllegalAccessException
        log.error("Could not instantiate AuthenticationToken {}", value, e);
        throw new ParameterException(e);
      }
    }
  }

  @Parameter(names = {"-tc", "--tokenClass"}, description = "token type to create, use the -l to pass options", converter = TokenConverter.class)
  private AuthenticationToken authenticationToken;

  @DynamicParameter(names = {"-l", "--tokenProperty"}, description = "login properties in the format key=value. Reuse -l for each property")
  private Map<String,String> tokenProperties = new TreeMap<>();

  @Parameter(names = "--disable-tab-completion", description = "disables tab completion (for less overhead when scripting)")
  private boolean tabCompletionDisabled;

  @Parameter(names = "--debug", description = "enables client debugging")
  private boolean debugEnabled;

  @Parameter(names = "--fake", description = "fake a connection to accumulo")
  private boolean fake;

  @Parameter(names = {"-?", "--help"}, help = true, description = "display this help")
  private boolean helpEnabled;

  @Parameter(names = {"-e", "--execute-command"}, description = "executes a command, and then exits")
  private String execCommand;

  @Parameter(names = {"-f", "--execute-file"}, description = "executes commands from a file at startup", converter = FileConverter.class)
  private File execFile;

  @Parameter(names = {"-fv", "--execute-file-verbose"}, description = "executes commands from a file at startup, with commands shown",
      converter = FileConverter.class)
  private File execFileVerbose;

  @Parameter(names = {"-h", "--hdfsZooInstance"}, description = "use hdfs zoo instance")
  private boolean hdfsZooInstance;

  @Parameter(names = {"-z", "--zooKeeperInstance"}, description = "use a zookeeper instance with the given instance name and list of zoo hosts. "
      + "Syntax: -z <zoo-instance-name> <zoo-hosts>. Where <zoo-hosts> is a comma separated list of zookeeper servers.", arity = 2)
  private List<String> zooKeeperInstance = new ArrayList<>();

  @Parameter(names = {"--ssl"}, description = "use ssl to connect to accumulo")
  private boolean useSsl = false;

  @Parameter(names = "--sasl", description = "use SASL to connect to Accumulo (Kerberos)")
  private boolean useSasl = false;

  @Parameter(names = "--config-file", description = "read the given client config file. "
      + "If omitted, the path searched can be specified with $ACCUMULO_CLIENT_CONF_PATH, "
      + "which defaults to ~/.accumulo/config:$ACCUMULO_CONF_DIR/client.conf:/etc/accumulo/client.conf")
  private String clientConfigFile = null;

  @Parameter(names = {"-zi", "--zooKeeperInstanceName"}, description = "use a zookeeper instance with the given instance name. "
      + "This parameter is used in conjunction with -zh.")
  private String zooKeeperInstanceName;

  @Parameter(names = {"-zh", "--zooKeeperHosts"}, description = "use a zookeeper instance with the given comma separated list of zookeeper servers. "
      + "This parameter is used in conjunction with -zi.")
  private String zooKeeperHosts;

  @Parameter(names = "--auth-timeout", description = "minutes the shell can be idle without re-entering a password")
  private int authTimeout = 60; // TODO Add validator for positive number

  @Parameter(names = "--disable-auth-timeout", description = "disables requiring the user to re-type a password after being idle")
  private boolean authTimeoutDisabled;

  @Parameter(hidden = true)
  private List<String> unrecognizedOptions;

  public String getUsername() throws Exception {
    if (null == username) {
      final ClientConfiguration clientConf = getClientConfiguration();
      if (Boolean.parseBoolean(clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED))) {
        if (!UserGroupInformation.isSecurityEnabled()) {
          throw new RuntimeException("Kerberos security is not enabled");
        }
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        username = ugi.getUserName();
      } else {
        username = System.getProperty("user.name", "root");
      }
    }
    return username;
  }

  public String getPassword() {
    return password;
  }

  public AuthenticationToken getAuthenticationToken() throws Exception {
    if (null == authenticationToken) {
      final ClientConfiguration clientConf = getClientConfiguration();
      // Automatically use a KerberosToken if the client conf is configured for SASL
      final boolean saslEnabled = Boolean.parseBoolean(clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));
      if (saslEnabled) {
        authenticationToken = new KerberosToken();
      }
    }
    return authenticationToken;
  }

  public Map<String,String> getTokenProperties() {
    return tokenProperties;
  }

  public boolean isTabCompletionDisabled() {
    return tabCompletionDisabled;
  }

  public boolean isDebugEnabled() {
    return debugEnabled;
  }

  public boolean isFake() {
    return fake;
  }

  public boolean isHelpEnabled() {
    return helpEnabled;
  }

  public String getExecCommand() {
    return execCommand;
  }

  public File getExecFile() {
    return execFile;
  }

  public File getExecFileVerbose() {
    return execFileVerbose;
  }

  public boolean isHdfsZooInstance() {
    return hdfsZooInstance;
  }

  public List<String> getZooKeeperInstance() {
    return zooKeeperInstance;
  }

  public String getZooKeeperInstanceName() {
    return zooKeeperInstanceName;
  }

  public String getZooKeeperHosts() {
    return zooKeeperHosts;
  }

  public int getAuthTimeout() {
    return authTimeout;
  }

  public boolean isAuthTimeoutDisabled() {
    if (useSasl()) {
      return true;
    }
    return authTimeoutDisabled;
  }

  public List<String> getUnrecognizedOptions() {
    return unrecognizedOptions;
  }

  public boolean useSsl() {
    return useSsl;
  }

  public String getClientConfigFile() {
    return clientConfigFile;
  }

  public ClientConfiguration getClientConfiguration() throws ConfigurationException, FileNotFoundException {
    ClientConfiguration clientConfig = clientConfigFile == null ? ClientConfiguration.loadDefault() : new ClientConfiguration(getClientConfigFile());
    if (useSsl()) {
      clientConfig.setProperty(ClientProperty.INSTANCE_RPC_SSL_ENABLED, "true");
    }
    if (useSasl()) {
      clientConfig.setProperty(ClientProperty.INSTANCE_RPC_SASL_ENABLED, "true");
    }
    if (!getZooKeeperInstance().isEmpty()) {
      List<String> zkOpts = getZooKeeperInstance();
      String instanceName = zkOpts.get(0);
      String hosts = zkOpts.get(1);
      clientConfig.setProperty(ClientProperty.INSTANCE_ZK_HOST, hosts);
      clientConfig.setProperty(ClientProperty.INSTANCE_NAME, instanceName);
    }
    // If the user provided the hosts, set the ZK for tracing too
    if (null != zooKeeperHosts && !zooKeeperHosts.isEmpty()) {
      clientConfig.setProperty(ClientProperty.INSTANCE_ZK_HOST, zooKeeperHosts);
    }
    if (null != zooKeeperInstanceName && !zooKeeperInstanceName.isEmpty()) {
      clientConfig.setProperty(ClientProperty.INSTANCE_NAME, zooKeeperInstanceName);
    }

    // Automatically try to add in the proper ZK from accumulo-site for backwards compat.
    if (!clientConfig.containsKey(ClientProperty.INSTANCE_ZK_HOST.getKey())) {
      AccumuloConfiguration siteConf = SiteConfiguration.getInstance();
      clientConfig.withZkHosts(siteConf.get(Property.INSTANCE_ZK_HOST));
    }

    return clientConfig;
  }

  public boolean useSasl() {
    return useSasl;
  }
}

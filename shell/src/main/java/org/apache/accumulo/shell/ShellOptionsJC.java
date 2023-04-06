/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.shell;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.clientImpl.ClientInfoImpl;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.hadoop.security.UserGroupInformation;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ShellOptionsJC {

  @Parameter(names = {"-u", "--user"}, description = "username")
  private String username = null;

  // Note: Don't use "password = true" because then it will prompt even if we have a token
  @Parameter(names = {"-p", "--password"},
      description = "password (can be specified as '<password>', 'pass:<password>',"
          + " 'file:<local file containing the password>', 'env:<variable containing"
          + " the pass>', or stdin)",
      converter = ClientOpts.PasswordConverter.class)
  private String password;

  @DynamicParameter(names = {"-l"},
      description = "command line properties in the format key=value. Reuse -l for each property")
  private Map<String,String> commandLineProperties = new TreeMap<>();

  @Parameter(names = "--disable-tab-completion",
      description = "disables tab completion (for less overhead when scripting)")
  private boolean tabCompletionDisabled;

  @Parameter(names = "--debug", description = "enables client debugging"
      + "; deprecated, configure debugging through your logging configuration file")
  private boolean debugEnabled;

  @Parameter(names = {"-?", "--help"}, help = true, description = "display this help")
  private boolean helpEnabled;

  @Parameter(names = {"-e", "--execute-command"},
      description = "executes a command, and then exits")
  private String execCommand;

  @Parameter(names = {"-f", "--execute-file"},
      description = "executes commands from a file at startup", converter = FileConverter.class)
  private File execFile;

  @Parameter(names = {"-fv", "--execute-file-verbose"},
      description = "executes commands from a file at startup, with commands shown",
      converter = FileConverter.class)
  private File execFileVerbose;

  @Parameter(names = {"-z", "--zooKeeperInstance"},
      description = "use a zookeeper instance with the given instance name and"
          + " list of zoo hosts. Syntax: -z <zoo-instance-name> <zoo-hosts>. Where"
          + " <zoo-hosts> is a comma separated list of zookeeper servers.",
      arity = 2)
  private List<String> zooKeeperInstance = new ArrayList<>();

  @Parameter(names = {"--ssl"}, description = "use ssl to connect to accumulo")
  private boolean useSsl = false;

  @Parameter(names = "--sasl", description = "use SASL to connect to Accumulo (Kerberos)")
  private boolean useSasl = false;

  @Parameter(names = "--config-file",
      description = "Read the given"
          + " accumulo-client.properties file. If omitted, the following locations will be"
          + " searched ~/.accumulo/accumulo-client.properties:"
          + "$ACCUMULO_CONF_DIR/accumulo-client.properties:"
          + "/etc/accumulo/accumulo-client.properties")
  private String clientConfigFile = null;

  @Parameter(names = {"-zi", "--zooKeeperInstanceName"},
      description = "use a zookeeper instance with the given instance name. "
          + "This parameter is used in conjunction with -zh.")
  private String zooKeeperInstanceName;

  @Parameter(names = {"-zh", "--zooKeeperHosts"},
      description = "use a zookeeper instance with the given comma separated"
          + " list of zookeeper servers. This parameter is used in conjunction with -zi.")
  private String zooKeeperHosts;

  @Parameter(names = "--auth-timeout",
      description = "minutes the shell can be idle without re-entering a password",
      validateWith = PositiveInteger.class)
  private int authTimeout = 60;

  @Parameter(names = "--disable-auth-timeout",
      description = "disables requiring the user to re-type a password after being idle")
  private boolean authTimeoutDisabled;

  @Parameter(hidden = true)
  private List<String> unrecognizedOptions;

  public String getUsername() throws Exception {
    if (username == null) {
      username = getClientProperties().getProperty(ClientProperty.AUTH_PRINCIPAL.getKey());
      if (username == null || username.isEmpty()) {
        if (ClientProperty.SASL_ENABLED.getBoolean(getClientProperties())) {
          if (!UserGroupInformation.isSecurityEnabled()) {
            throw new IllegalArgumentException(
                "Kerberos security is not enabled. Run with --sasl or set 'sasl.enabled' in"
                    + " accumulo-client.properties");
          }
          UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
          username = ugi.getUserName();
        } else {
          throw new IllegalArgumentException("Username is not set. Run with '-u"
              + " myuser' or set 'auth.principal' in accumulo-client.properties");
        }
      }
    }
    return username;
  }

  public String getPassword() {
    return password;
  }

  public boolean isTabCompletionDisabled() {
    return tabCompletionDisabled;
  }

  public boolean isDebugEnabled() {
    return debugEnabled;
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

  public int getAuthTimeout() {
    return authTimeout;
  }

  public boolean isAuthTimeoutDisabled() {
    if (useSasl) {
      return true;
    }
    return authTimeoutDisabled;
  }

  public List<String> getUnrecognizedOptions() {
    return unrecognizedOptions;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "user-provided paths intentional")
  public String getClientPropertiesFile() {
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
          System.out.println("Loading configuration from " + clientConfigFile);
          break;
        }
      }
    }
    return clientConfigFile;
  }

  public Properties getClientProperties() {
    Properties props = new Properties();
    if (getClientPropertiesFile() != null) {
      props = ClientInfoImpl.toProperties(getClientPropertiesFile());
    }
    for (Map.Entry<String,String> entry : commandLineProperties.entrySet()) {
      props.setProperty(entry.getKey(), entry.getValue());
    }
    if (useSsl) {
      props.setProperty(ClientProperty.SSL_ENABLED.getKey(), "true");
    }
    if (useSasl) {
      props.setProperty(ClientProperty.SASL_ENABLED.getKey(), "true");
    }
    if (!zooKeeperInstance.isEmpty()) {
      String instanceName = zooKeeperInstance.get(0);
      String hosts = zooKeeperInstance.get(1);
      props.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), hosts);
      props.setProperty(ClientProperty.INSTANCE_NAME.getKey(), instanceName);
    }
    // If the user provided the hosts, set the ZK for tracing too
    if (zooKeeperHosts != null && !zooKeeperHosts.isEmpty()) {
      props.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), zooKeeperHosts);
    }
    if (zooKeeperInstanceName != null && !zooKeeperInstanceName.isEmpty()) {
      props.setProperty(ClientProperty.INSTANCE_NAME.getKey(), zooKeeperInstanceName);
    }
    return props;
  }

  static class PositiveInteger implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      int n = -1;
      try {
        n = Integer.parseInt(value);
      } catch (NumberFormatException e) {
        // ignore, will be handled below
      }
      if (n < 0) {
        throw new ParameterException(
            "Parameter " + name + " should be a positive integer (was " + value + ")");
      }
    }
  }

}

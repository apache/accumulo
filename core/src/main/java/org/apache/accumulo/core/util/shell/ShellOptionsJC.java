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
package org.apache.accumulo.core.util.shell;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;

public class ShellOptionsJC {
  @Parameter(names = {"-u", "--user"}, description = "username (defaults to your OS user)")
  private String username = System.getProperty("user.name", "root");
  
  // Note: Don't use "password = true" because then it will prompt even if we have a token
  @Parameter(names = {"-p", "--password"}, description = "password (prompt for password if this option is missing)")
  private String password;
  
  private static class TokenConverter implements IStringConverter<AuthenticationToken> {
    @Override
    public AuthenticationToken convert(String value) {
      try {
        return Class.forName(value).asSubclass(AuthenticationToken.class).newInstance();
      } catch (Exception e) {
        // Catching ClassNotFoundException, ClassCastException, InstantiationException and IllegalAccessException
        throw new ParameterException(e);
      }
    }
  }
  
  @Parameter(names = {"-tc", "--tokenClass"}, description = "token type to create, use the -l to pass options", converter = TokenConverter.class)
  private AuthenticationToken authenticationToken;
  
  @DynamicParameter(names = {"-l", "--tokenProperty"}, description = "login properties in the format key=value. Reuse -l for each property")
  private Map<String,String> tokenProperties;
  
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
  
  @Parameter(names = {"-z", "--zooKeeperInstance"}, description = "use a zookeeper instance with the given instance name and list of zoo hosts", arity = 2)
  private List<String> zooKeeperInstance = new ArrayList<String>();
  
  @Parameter(names = "--auth-timeout", description = "minutes the shell can be idle without re-entering a password")
  private int authTimeout = 60; // TODO Add validator for positive number
  
  @Parameter(names = "--disable-auth-timeout", description = "disables requiring the user to re-type a password after being idle")
  private boolean authTimeoutDisabled;
  
  @Parameter(hidden = true)
  private List<String> unrecognizedOptions;
  
  public String getUsername() {
    return username;
  }
  
  public String getPassword() {
    return password;
  }
  
  public AuthenticationToken getAuthenticationToken() {
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
  
  public int getAuthTimeout() {
    return authTimeout;
  }
  
  public boolean isAuthTimeoutDisabled() {
    return authTimeoutDisabled;
  }
  
  public List<String> getUnrecognizedOptions() {
    return unrecognizedOptions;
  }
}

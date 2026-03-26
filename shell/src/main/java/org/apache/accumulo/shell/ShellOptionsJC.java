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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.cli.ClientOpts;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ShellOptionsJC extends ClientOpts {

  @DynamicParameter(names = {"-l"},
      description = "command line properties in the format key=value. Reuse -l for each property")
  private Map<String,String> commandLineProperties = new TreeMap<>();

  @Parameter(names = "--disable-tab-completion",
      description = "disables tab completion (for less overhead when scripting)")
  private boolean tabCompletionDisabled;

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

  @Parameter(names = "--auth-timeout",
      description = "minutes the shell can be idle without re-entering a password",
      validateWith = PositiveInteger.class)
  private int authTimeout = 60;

  @Parameter(names = "--disable-auth-timeout",
      description = "disables requiring the user to re-type a password after being idle")
  private boolean authTimeoutDisabled;

  @Parameter(hidden = true)
  private List<String> unrecognizedOptions;

  public boolean isTabCompletionDisabled() {
    return tabCompletionDisabled;
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
    return authTimeoutDisabled;
  }

  public List<String> getUnrecognizedOptions() {
    return unrecognizedOptions;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "user-provided paths intentional")
  @Override
  public String getClientConfigFile() {
    String configFile = super.getClientConfigFile();
    if (configFile == null) {
      List<String> searchPaths = new LinkedList<>();
      searchPaths.add(System.getProperty("user.home") + "/.accumulo/accumulo-client.properties");
      if (System.getenv("ACCUMULO_CONF_DIR") != null) {
        searchPaths.add(System.getenv("ACCUMULO_CONF_DIR") + "/accumulo-client.properties");
      }
      searchPaths.add("/etc/accumulo/accumulo-client.properties");
      for (String path : searchPaths) {
        Path file = Path.of(path);
        if (Files.isRegularFile(file) && Files.isReadable(file)) {
          String clientConfigFile = file.toAbsolutePath().toString();
          Shell.log.info("Loading configuration from {}", clientConfigFile);
          return clientConfigFile;
        }
      }
      return null;
    } else {
      return configFile;
    }
  }

  public static class PositiveInteger implements IParameterValidator {
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

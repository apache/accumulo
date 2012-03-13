/**
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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

/**
 * Abstract class to encompass the Options available on the Accumulo Shell
 */
public abstract class ShellOptions {
  protected static final String DEFAULT_AUTH_TIMEOUT = "60"; // in minutes
  
  // Global options flags
  public static final String userOption = "u";
  public static final String tableOption = "t";
  public static final String helpOption = "?";
  public static final String helpLongOption = "help";
  
  final Options opts = new Options();
  final Option usernameOption = new Option("u", "user", true, "username (defaults to your OS user)");
  final Option passwOption = new Option("p", "password", true, "password (prompt for password if this option is missing)");
  final Option tabCompleteOption = new Option(null, "disable-tab-completion", false, "disables tab completion (for less overhead when scripting)");
  final Option debugOption = new Option(null, "debug", false, "enables client debugging");
  final Option fakeOption = new Option(null, "fake", false, "fake a connection to accumulo");
  final Option helpOpt = new Option(helpOption, helpLongOption, false, "display this help");
  final Option execCommandOpt = new Option("e", "execute-command", true, "executes a command, and then exits");
  final OptionGroup execFileGroup = new OptionGroup();
  final Option execfileOption = new Option("f", "execute-file", true, "executes commands from a file at startup");
  final Option execfileVerboseOption = new Option("fv", "execute-file-verbose", true, "executes commands from a file at startup, with commands shown");
  final OptionGroup instanceOptions = new OptionGroup();
  final Option hdfsZooInstance = new Option("h", "hdfsZooInstance", false, "use hdfs zoo instance");
  final Option zooKeeperInstance = new Option("z", "zooKeeperInstance", true, "use a zookeeper instance with the given instance name and list of zoo hosts");
  final OptionGroup authTimeoutOptions = new OptionGroup();
  final Option authTimeoutOpt = new Option(null, "auth-timeout", true, "minutes the shell can be idle without re-entering a password (default "
      + DEFAULT_AUTH_TIMEOUT + " min)");
  final Option disableAuthTimeoutOpt = new Option(null, "disable-auth-timeout", false, "disables requiring the user to re-type a password after being idle");

  public ShellOptions() {
    usernameOption.setArgName("user");
    opts.addOption(usernameOption);
    
    passwOption.setArgName("pass");
    opts.addOption(passwOption);
    
    opts.addOption(tabCompleteOption);
    
    opts.addOption(debugOption);
    
    opts.addOption(fakeOption);
    
    opts.addOption(helpOpt);
    
    opts.addOption(execCommandOpt);
    
    
    execfileOption.setArgName("file");
    execFileGroup.addOption(execfileOption);
    
    execfileVerboseOption.setArgName("file");
    execFileGroup.addOption(execfileVerboseOption);
    
    opts.addOptionGroup(execFileGroup);
    
    
    instanceOptions.addOption(hdfsZooInstance);
    
    zooKeeperInstance.setArgName("name hosts");
    zooKeeperInstance.setArgs(2);
    instanceOptions.addOption(zooKeeperInstance);
    
    opts.addOptionGroup(instanceOptions);
    
    authTimeoutOpt.setArgName("minutes");
    authTimeoutOptions.addOption(authTimeoutOpt);
    
    authTimeoutOptions.addOption(disableAuthTimeoutOpt);
    
    opts.addOptionGroup(authTimeoutOptions);
  }
}

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
package org.apache.accumulo.cluster;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute a command, leveraging Hadoop's {@link ShellCommandExecutor}, on a remote host. SSH configuration provided by {@link RemoteShellOptions}.
 */
public class RemoteShell extends ShellCommandExecutor {
  private static final Logger log = LoggerFactory.getLogger(RemoteShell.class);

  protected RemoteShellOptions options;
  protected String hostname;

  public RemoteShell(String hostname, String[] execString, File dir, Map<String,String> env, long timeout, RemoteShellOptions options) {
    super(execString, dir, env, timeout);
    this.hostname = hostname;
    setRemoteShellOptions(options);
  }

  public RemoteShell(String hostname, String[] execString, File dir, Map<String,String> env, RemoteShellOptions options) {
    super(execString, dir, env);
    this.hostname = hostname;
    setRemoteShellOptions(options);
  }

  public RemoteShell(String hostname, String[] execString, File dir, RemoteShellOptions options) {
    super(execString, dir);
    this.hostname = hostname;
    setRemoteShellOptions(options);
  }

  public RemoteShell(String hostname, String[] execString, RemoteShellOptions options) {
    super(execString);
    this.hostname = hostname;
    setRemoteShellOptions(options);
  }

  public void setRemoteShellOptions(RemoteShellOptions options) {
    requireNonNull(options);
    this.options = options;
  }

  public RemoteShellOptions getRemoteShellOptions() {
    return options;
  }

  @Override
  public String[] getExecString() {
    String hostWithUser = hostname;
    if (!options.getUserName().isEmpty()) {
      hostWithUser = options.getUserName() + "@" + hostWithUser;
    }

    String remoteCmd = StringUtils.join(super.getExecString(), ' ');

    String cmd = String.format("%1$s %2$s %3$s \"%4$s\"", options.getSshCommand(), options.getSshOptions(), hostWithUser, remoteCmd);

    log.debug("Executing full command [{}]", cmd);

    return new String[] {"/usr/bin/env", "bash", "-c", cmd};
  }

  @Override
  public void execute() throws IOException {
    super.execute();
  }

}

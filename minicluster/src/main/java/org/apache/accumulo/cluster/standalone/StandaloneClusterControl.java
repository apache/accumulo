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
package org.apache.accumulo.cluster.standalone;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.cluster.RemoteShell;
import org.apache.accumulo.cluster.RemoteShellOptions;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.hadoop.util.Shell.ExitCodeException;

import com.google.common.collect.Maps;

/**
 * Use the {@link RemoteShell} to control a standalone (possibly distibuted) Accumulo instance
 */
public class StandaloneClusterControl implements ClusterControl {

  protected String accumuloHome;
  protected RemoteShellOptions options;

  protected String startServerPath;

  public StandaloneClusterControl() {
    this(System.getenv("ACCUMULO_HOME"));
  }

  public StandaloneClusterControl(String accumuloHome) {
    this.options = new RemoteShellOptions();
    this.accumuloHome = accumuloHome;

    File bin = new File(accumuloHome, "bin");
    File startServer = new File(bin, "start-server.sh");
    this.startServerPath = startServer.getAbsolutePath();
  }

  protected Entry<Integer,String> exec(String hostname, String[] command) throws IOException {
    RemoteShell shell = new RemoteShell(hostname, command, options);
    try {
      shell.execute();
    } catch (ExitCodeException e) {
      // capture the stdout of the process as well.
      String output = shell.getOutput();
      // add output for the ExitCodeException.
      throw new ExitCodeException(e.getExitCode(), "stderr: " + e.getMessage() + ", stdout: " + output);
    }

    return Maps.immutableEntry(shell.getExitCode(), shell.getOutput());
  }

  @Override
  public void start(ServerType server, String hostname) throws IOException {
    String[] cmd = new String[] {startServerPath, hostname, getProcessString(server)};
    exec(hostname, cmd);
  }

  @Override
  public void stop(ServerType server, String hostname) throws IOException {
    String pid = getPid(server, accumuloHome, hostname);

    // TODO Use `accumulo admin stop` for tservers, instrument clean stop for GC, monitor, tracer instead kill

    String[] stopCmd = new String[] {"kill", "-9", pid};
    exec(hostname, stopCmd);
  }

  @Override
  public void signal(ServerType server, String hostname, String signal) throws IOException {
    String pid = getPid(server, accumuloHome, hostname);

    boolean isSignalNumber = false;
    try {
      Integer.parseInt(signal);
      isSignalNumber = true;
    } catch (NumberFormatException e) {}

    String[] stopCmd;
    if (isSignalNumber) {
      stopCmd = new String[] {"kill", signal, pid};
    } else {
      stopCmd = new String[] {"kill", "-s", signal, pid};
    }

    exec(hostname, stopCmd);
  }

  @Override
  public void suspend(ServerType server, String hostname) throws IOException {
    String pid = getPid(server, accumuloHome, hostname);

    String[] stopCmd = new String[] {"kill", "-s", "SIGSTOP", pid};
    exec(hostname, stopCmd);
  }

  @Override
  public void resume(ServerType server, String hostname) throws IOException {
    String pid = getPid(server, accumuloHome, hostname);

    String[] stopCmd = new String[] {"kill", "-s", "SIGCONT", pid};
    exec(hostname, stopCmd);
  }

  @Override
  public void kill(ServerType server, String hostname) throws IOException {
    String pid = getPid(server, accumuloHome, hostname);

    String[] stopCmd = new String[] {"kill", "-s", "SIGKILL", pid};
    exec(hostname, stopCmd);
  }

  protected String getPid(ServerType server, String accumuloHome, String hostname) throws IOException {
    String[] getPidCommand = getPidCommand(server, accumuloHome);
    Entry<Integer,String> ret = exec(hostname, getPidCommand);
    if (0 != ret.getKey()) {
      throw new IOException("Could not locate PID for " + getProcessString(server) + " on " + hostname);
    }

    return ret.getValue();
  }

  protected String[] getPidCommand(ServerType server, String accumuloHome) {
    // Lifted from stop-server.sh to get the PID
    return new String[] {"ps", "aux", "|", "fgrep", accumuloHome, "|", "fgrep", getProcessString(server), "|", "fgrep", "-v", "grep", "|", "fgrep", "-v",
        "ssh", "|", "awk", "'{print \\$2}'", "|", "head", "-1", "|", "tr", "-d", "'\\n'"};
  }

  protected String getProcessString(ServerType server) {
    switch (server) {
      case TABLET_SERVER:
        return "tserver";
      case GARBAGE_COLLECTOR:
        return "gc";
      case MASTER:
        return "master";
      case TRACER:
        return "tracer";
      case MONITOR:
        return "monitor";
      default:
        throw new UnsupportedOperationException("Unhandled ServerType " + server);
    }
  }
}

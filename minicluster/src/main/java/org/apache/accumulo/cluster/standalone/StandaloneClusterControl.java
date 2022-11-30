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
package org.apache.accumulo.cluster.standalone;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.cluster.RemoteShell;
import org.apache.accumulo.cluster.RemoteShellOptions;
import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.manager.thrift.ManagerGoalState;
import org.apache.accumulo.manager.state.SetGoalState;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.util.Admin;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Use the {@link RemoteShell} to control a standalone (possibly distributed) Accumulo instance
 */
public class StandaloneClusterControl implements ClusterControl {
  private static final Logger log = LoggerFactory.getLogger(StandaloneClusterControl.class);

  private static final String ACCUMULO_SERVICE_SCRIPT = "accumulo-service",
      ACCUMULO_SCRIPT = "accumulo";
  private static final String MANAGER_HOSTS_FILE = "managers", GC_HOSTS_FILE = "gc",
      TSERVER_HOSTS_FILE = "tservers", MONITOR_HOSTS_FILE = "monitor";

  String accumuloHome;
  String clientAccumuloConfDir;
  String serverAccumuloConfDir;
  private String clientCmdPrefix;
  private String serverCmdPrefix;
  protected RemoteShellOptions options;

  protected String accumuloServicePath, accumuloPath;

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input file name")
  public StandaloneClusterControl(String accumuloHome, String clientAccumuloConfDir,
      String serverAccumuloConfDir, String clientCmdPrefix, String serverCmdPrefix) {
    this.options = new RemoteShellOptions();
    this.accumuloHome = accumuloHome;
    this.clientAccumuloConfDir = clientAccumuloConfDir;
    this.serverAccumuloConfDir = serverAccumuloConfDir;
    this.clientCmdPrefix = clientCmdPrefix;
    this.serverCmdPrefix = serverCmdPrefix;

    File bin = new File(accumuloHome, "bin");
    this.accumuloServicePath = new File(bin, ACCUMULO_SERVICE_SCRIPT).getAbsolutePath();
    this.accumuloPath = new File(bin, ACCUMULO_SCRIPT).getAbsolutePath();
  }

  protected Entry<Integer,String> exec(String hostname, String[] command) throws IOException {
    RemoteShell shell = new RemoteShell(hostname, command, options);
    try {
      shell.execute();
    } catch (ExitCodeException e) {
      // capture the stdout of the process as well.
      String output = shell.getOutput();
      // add output for the ExitCodeException.
      ExitCodeException ece = new ExitCodeException(e.getExitCode(),
          "stderr: " + e.getMessage() + ", stdout: " + output);
      log.error("Failed to run command", ece);
      return Maps.immutableEntry(e.getExitCode(), output);
    }

    return Maps.immutableEntry(shell.getExitCode(), shell.getOutput());
  }

  @Override
  public int exec(Class<?> clz, String[] args) throws IOException {
    return execWithStdout(clz, args).getKey();
  }

  @Override
  public Entry<Integer,String> execWithStdout(Class<?> clz, String[] args) throws IOException {
    String manager = getHosts(MANAGER_HOSTS_FILE).get(0);
    List<String> cmd = new ArrayList<>();
    cmd.add(clientCmdPrefix);
    cmd.add(accumuloPath);
    cmd.add(clz.getName());
    // Quote the arguments to prevent shell expansion
    for (String arg : args) {
      cmd.add("'" + arg + "'");
    }
    log.info("Running: '{}' on {}", sanitize(String.join(" ", cmd)), sanitize(manager));
    return exec(manager, cmd.toArray(new String[cmd.size()]));
  }

  /**
   * Prevent potential CRLF injection into logs from read in user data. See the
   * <a href="https://find-sec-bugs.github.io/bugs.htm#CRLF_INJECTION_LOGS">bug description</a>
   */
  private String sanitize(String msg) {
    return msg.replaceAll("[\r\n]", "");
  }

  @Override
  public void adminStopAll() throws IOException {
    String manager = getHosts(MANAGER_HOSTS_FILE).get(0);
    String[] cmd = {serverCmdPrefix, accumuloPath, Admin.class.getName(), "stopAll"};
    // Directly invoke the RemoteShell
    Entry<Integer,String> pair = exec(manager, cmd);
    if (pair.getKey() != 0) {
      throw new IOException("stopAll did not finish successfully, retcode=" + pair.getKey()
          + ", stdout=" + pair.getValue());
    }
  }

  /**
   * Wrapper around SetGoalState
   *
   * @param goalState The goal state to set
   * @throws IOException If SetGoalState returns a non-zero result
   */
  public void setGoalState(String goalState) throws IOException {
    requireNonNull(goalState, "Goal state must not be null");
    checkArgument(ManagerGoalState.valueOf(goalState) != null, "Unknown goal state: " + goalState);
    String manager = getHosts(MANAGER_HOSTS_FILE).get(0);
    String[] cmd = {serverCmdPrefix, accumuloPath, SetGoalState.class.getName(), goalState};
    Entry<Integer,String> pair = exec(manager, cmd);
    if (pair.getKey() != 0) {
      throw new IOException("SetGoalState did not finish successfully, retcode=" + pair.getKey()
          + ", stdout=" + pair.getValue());
    }
  }

  @Override
  @SuppressWarnings("removal")
  public void startAllServers(ServerType server) throws IOException {
    switch (server) {
      case TABLET_SERVER:
        for (String tserver : getHosts(TSERVER_HOSTS_FILE)) {
          start(server, tserver);
        }
        break;
      case MASTER:
      case MANAGER:
        for (String manager : getHosts(MANAGER_HOSTS_FILE)) {
          start(server, manager);
        }
        break;
      case GARBAGE_COLLECTOR:
        List<String> hosts = getHosts(GC_HOSTS_FILE);
        if (hosts.isEmpty()) {
          hosts = getHosts(MANAGER_HOSTS_FILE);
          if (hosts.isEmpty()) {
            throw new IOException("Found hosts to run garbage collector on");
          }
          hosts = Collections.singletonList(hosts.get(0));
        }
        for (String gc : hosts) {
          start(server, gc);
        }
        break;
      case MONITOR:
        for (String monitor : getHosts(MONITOR_HOSTS_FILE)) {
          start(server, monitor);
        }
        break;
      case ZOOKEEPER:
      default:
        throw new UnsupportedOperationException("Could not start servers for " + server);
    }
  }

  @Override
  public void start(ServerType server, String hostname) throws IOException {
    String[] cmd = {serverCmdPrefix, accumuloServicePath, getProcessString(server), "start"};
    Entry<Integer,String> pair = exec(hostname, cmd);
    if (pair.getKey() != 0) {
      throw new IOException(
          "Start " + server + " on " + hostname + " failed for execute successfully");
    }
  }

  @Override
  @SuppressWarnings("removal")
  public void stopAllServers(ServerType server) throws IOException {
    switch (server) {
      case TABLET_SERVER:
        for (String tserver : getHosts(TSERVER_HOSTS_FILE)) {
          stop(server, tserver);
        }
        break;
      case MASTER:
      case MANAGER:
        for (String manager : getHosts(MANAGER_HOSTS_FILE)) {
          stop(server, manager);
        }
        break;
      case GARBAGE_COLLECTOR:
        for (String gc : getHosts(GC_HOSTS_FILE)) {
          stop(server, gc);
        }
        break;
      case MONITOR:
        for (String monitor : getHosts(MONITOR_HOSTS_FILE)) {
          stop(server, monitor);
        }
        break;
      case ZOOKEEPER:
      default:
        throw new UnsupportedOperationException("Could not start servers for " + server);
    }
  }

  @Override
  public void stop(ServerType server, String hostname) throws IOException {
    // TODO Use `accumulo admin stop` for tservers, instrument clean stop for GC, monitor
    // instead kill

    kill(server, hostname);
  }

  @Override
  public void signal(ServerType server, String hostname, String signal) throws IOException {
    String pid = getPid(server, accumuloHome, hostname);

    if (pid.trim().isEmpty()) {
      log.debug("Found no processes for {} on {}", sanitize(server.prettyPrint()),
          sanitize(hostname));
      return;
    }

    boolean isSignalNumber = false;
    try {
      Integer.parseInt(signal);
      isSignalNumber = true;
    } catch (NumberFormatException e) {}

    String[] stopCmd;
    if (isSignalNumber) {
      stopCmd = new String[] {serverCmdPrefix, "kill", "-" + signal, pid};
    } else {
      stopCmd = new String[] {serverCmdPrefix, "kill", "-s", signal, pid};
    }

    Entry<Integer,String> pair = exec(hostname, stopCmd);
    if (pair.getKey() != 0) {
      throw new IOException("Signal " + signal + " to " + server + " on " + hostname
          + " failed for execute successfully. stdout=" + pair.getValue());
    }
  }

  @Override
  public void suspend(ServerType server, String hostname) throws IOException {
    signal(server, hostname, "SIGSTOP");
  }

  @Override
  public void resume(ServerType server, String hostname) throws IOException {
    signal(server, hostname, "SIGCONT");
  }

  @Override
  public void kill(ServerType server, String hostname) throws IOException {
    signal(server, hostname, "SIGKILL");
  }

  protected String getPid(ServerType server, String accumuloHome, String hostname)
      throws IOException {
    String[] getPidCommand = getPidCommand(server, accumuloHome);
    Entry<Integer,String> ret = exec(hostname, getPidCommand);
    if (ret.getKey() != 0) {
      throw new IOException(
          "Could not locate PID for " + getProcessString(server) + " on " + hostname);
    }

    return ret.getValue();
  }

  protected String[] getPidCommand(ServerType server, String accumuloHome) {
    // Lifted from stop-server.sh to get the PID
    return new String[] {"ps", "aux", "|", "fgrep", accumuloHome, "|", "fgrep",
        getProcessString(server), "|", "fgrep", "-v", "grep", "|", "fgrep", "-v", "ssh", "|", "awk",
        "'{print \\$2}'", "|", "head", "-1", "|", "tr", "-d", "'\\n'"};
  }

  @SuppressWarnings("removal")
  protected String getProcessString(ServerType server) {
    switch (server) {
      case TABLET_SERVER:
        return "tserver";
      case GARBAGE_COLLECTOR:
        return "gc";
      case MASTER:
      case MANAGER:
        return "manager";
      case MONITOR:
        return "monitor";
      default:
        throw new UnsupportedOperationException("Unhandled ServerType " + server);
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input file name")
  protected File getClientConfDir() {
    File confDir = new File(clientAccumuloConfDir);
    if (!confDir.exists() || !confDir.isDirectory()) {
      throw new IllegalStateException(
          "Accumulo client conf dir does not exist or is not a directory: " + confDir);
    }
    return confDir;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input file name")
  protected File getServerConfDir() {
    File confDir = new File(serverAccumuloConfDir);
    if (!confDir.exists() || !confDir.isDirectory()) {
      throw new IllegalStateException(
          "Accumulo server conf dir does not exist or is not a directory: " + confDir);
    }
    return confDir;
  }

  /**
   * Read hosts in file named by 'fn' in Accumulo conf dir
   */
  protected List<String> getHosts(String fn) throws IOException {
    return getHosts(new File(getServerConfDir(), fn));
  }

  /**
   * Read the provided file and return all lines which don't start with a '#' character
   */
  protected List<String> getHosts(File f) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(f, UTF_8))) {
      List<String> hosts = new ArrayList<>();
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (!line.isEmpty() && !line.startsWith("#")) {
          hosts.add(line);
        }
      }

      return hosts;
    }
  }

  @Override
  public void startCompactors(Class<? extends Compactor> compactor, int limit, String queueName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void startCoordinator(Class<? extends CompactionCoordinator> coordinator)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }
}

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.cluster.RemoteShell;
import org.apache.accumulo.cluster.RemoteShellOptions;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.util.Admin;
import org.apache.hadoop.util.Shell.ExitCodeException;

import com.google.common.collect.Maps;

/**
 * Use the {@link RemoteShell} to control a standalone (possibly distibuted) Accumulo instance
 */
public class StandaloneClusterControl implements ClusterControl {

  protected String accumuloHome, accumuloConfDir;
  protected RemoteShellOptions options;

  protected String startServerPath, accumuloPath;

  public StandaloneClusterControl() {
    this(System.getenv("ACCUMULO_HOME"), System.getenv("ACCUMULO_CONF_DIR"));
  }

  public StandaloneClusterControl(String accumuloHome, String accumuloConfDir) {
    this.options = new RemoteShellOptions();
    this.accumuloHome = accumuloHome;
    this.accumuloConfDir = accumuloConfDir;

    File bin = new File(accumuloHome, "bin");
    File startServer = new File(bin, "start-server.sh");
    this.startServerPath = startServer.getAbsolutePath();
    File accumulo = new File(bin, "accumulo");
    this.accumuloPath = accumulo.getAbsolutePath();
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
  public int exec(Class<?> clz, String[] args) throws IOException {
    File confDir = getConfDir();
    String master = getHosts(new File(confDir, "masters")).get(0);
    String[] cmd = new String[2 + args.length];
    cmd[0] = accumuloPath;
    cmd[1] = clz.getName();
    System.arraycopy(args, 0, cmd, 2, args.length);
    return exec(master, cmd).getKey();
  }

  @Override
  public void adminStopAll() throws IOException {
    File confDir = getConfDir();
    String master = getHosts(new File(confDir, "masters")).get(0);
    String[] cmd = new String[] { accumuloPath, Admin.class.getName(), "stopAll" };
    exec(master, cmd);
  }

  @Override
  public void startAllServers(ServerType server) throws IOException {
    File confDir = getConfDir();

    switch (server) {
      case TABLET_SERVER:
        for (String tserver : getHosts(new File(confDir, "slaves"))) {
          start(server, tserver);
        }
        break;
      case MASTER:
        for (String master : getHosts(new File(confDir, "masters"))) {
          start(server, master);
        }
        break;
      case GARBAGE_COLLECTOR:
        for (String gc : getHosts(new File(confDir, "gc"))) {
          start(server, gc);
        }
        break;
      case TRACER:
        for (String tracer : getHosts(new File(confDir, "tracers"))) {
          start(server, tracer);
        }
        break;
      case MONITOR:
        for (String monitor : getHosts(new File(confDir, "monitor"))) {
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
    String[] cmd = new String[] {startServerPath, hostname, getProcessString(server)};
    exec(hostname, cmd);
  }

  @Override
  public void stopAllServers(ServerType server) throws IOException {
    File confDir = getConfDir();

    switch (server) {
      case TABLET_SERVER:
        for (String tserver : getHosts(new File(confDir, "slaves"))) {
          stop(server, tserver);
        }
        break;
      case MASTER:
        for (String master : getHosts(new File(confDir, "masters"))) {
          stop(server, master);
        }
        break;
      case GARBAGE_COLLECTOR:
        for (String gc : getHosts(new File(confDir, "gc"))) {
          stop(server, gc);
        }
        break;
      case TRACER:
        for (String tracer : getHosts(new File(confDir, "tracers"))) {
          stop(server, tracer);
        }
        break;
      case MONITOR:
        for (String monitor : getHosts(new File(confDir, "monitor"))) {
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

  protected File getConfDir() {
    String confPath = null == accumuloConfDir ? System.getenv("ACCUMULO_CONF_DIR") : accumuloConfDir;
    File confDir;
    if (null == confPath) {
      String homePath = null == accumuloHome ? System.getenv("ACCUMULO_HOME") : accumuloHome;
      if (null == homePath) {
        throw new IllegalStateException("Cannot extrapolate an ACCUMULO_CONF_DIR");
      }
      confDir = new File(homePath, "conf");
    } else {
      confDir = new File(confPath);
    }

    if (!confDir.exists() || !confDir.isDirectory()) {
      throw new IllegalStateException("ACCUMULO_CONF_DIR does not exist or is not a directory: " + confDir);
    }

    return confDir;
  }

  /**
   * Read the provided file and return all lines which don't start with a '#' character
   */
  protected List<String> getHosts(File f) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(f));
    try {
      List<String> hosts = new ArrayList<String>();
      String line = null;
      while ((line = reader.readLine()) != null) {
        if (!line.trim().startsWith("#")) {
          hosts.add(line);
        }
      }

      return hosts;
    } finally {
      reader.close();
    }
  }
}

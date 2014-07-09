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
package org.apache.accumulo.minicluster.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.state.SetGoalState;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.start.Main;
import org.apache.accumulo.start.classloader.vfs.MiniDFSUtil;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ZooKeeperServerMain;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

/**
 * A utility class that will create Zookeeper and Accumulo processes that write all of their data to a single local directory. This class makes it easy to test
 * code against a real Accumulo instance. Its much more accurate for testing than {@link org.apache.accumulo.core.client.mock.MockAccumulo}, but much slower.
 * 
 * @since 1.6.0
 */
public class MiniAccumuloClusterImpl implements AccumuloCluster {
  private static final Logger log = Logger.getLogger(MiniAccumuloClusterImpl.class);

  public static class LogWriter extends Daemon {
    private BufferedReader in;
    private BufferedWriter out;

    public LogWriter(InputStream stream, File logFile) throws IOException {
      this.in = new BufferedReader(new InputStreamReader(stream));
      out = new BufferedWriter(new FileWriter(logFile));

      SimpleTimer.getInstance().schedule(new Runnable() {
        @Override
        public void run() {
          try {
            flush();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }, 1000, 1000);
    }

    public synchronized void flush() throws IOException {
      if (out != null)
        out.flush();
    }

    @Override
    public void run() {
      String line;

      try {
        while ((line = in.readLine()) != null) {
          out.append(line);
          out.append("\n");
        }

        synchronized (this) {
          out.close();
          out = null;
          in.close();
        }

      } catch (IOException e) {}
    }
  }

  private boolean initialized = false;
  private Process zooKeeperProcess = null;
  private Process masterProcess = null;
  private Process gcProcess = null;
  private List<Process> tabletServerProcesses = Collections.synchronizedList(new ArrayList<Process>());

  private Set<Pair<ServerType,Integer>> debugPorts = new HashSet<Pair<ServerType,Integer>>();

  private File zooCfgFile;
  private String dfsUri;

  public List<LogWriter> getLogWriters() {
    return logWriters;
  }

  private List<LogWriter> logWriters = new ArrayList<MiniAccumuloClusterImpl.LogWriter>();

  private MiniAccumuloConfigImpl config;
  private MiniDFSCluster miniDFS = null;
  private List<Process> cleanup = new ArrayList<Process>();

  public Process exec(Class<?> clazz, String... args) throws IOException {
    return exec(clazz, null, args);
  }

  public Process exec(Class<?> clazz, List<String> jvmArgs, String... args) throws IOException {
    ArrayList<String> jvmArgs2 = new ArrayList<String>(1 + (jvmArgs == null ? 0 : jvmArgs.size()));
    jvmArgs2.add("-Xmx" + config.getDefaultMemory());
    if (jvmArgs != null)
      jvmArgs2.addAll(jvmArgs);
    Process proc = _exec(clazz, jvmArgs2, args);
    cleanup.add(proc);
    return proc;
  }

  private boolean containsSiteFile(File f) {
    return f.isDirectory() && f.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith("site.xml");
      }
    }).length > 0;
  }

  private void append(StringBuilder classpathBuilder, URL url) throws URISyntaxException {
    File file = new File(url.toURI());
    // do not include dirs containing hadoop or accumulo site files
    if (!containsSiteFile(file))
      classpathBuilder.append(File.pathSeparator).append(file.getAbsolutePath());
  }

  private String getClasspath() throws IOException {

    try {
      ArrayList<ClassLoader> classloaders = new ArrayList<ClassLoader>();

      ClassLoader cl = this.getClass().getClassLoader();

      while (cl != null) {
        classloaders.add(cl);
        cl = cl.getParent();
      }

      Collections.reverse(classloaders);

      StringBuilder classpathBuilder = new StringBuilder();
      classpathBuilder.append(config.getConfDir().getAbsolutePath());

      if (config.getClasspathItems() == null) {

        // assume 0 is the system classloader and skip it
        for (int i = 1; i < classloaders.size(); i++) {
          ClassLoader classLoader = classloaders.get(i);

          if (classLoader instanceof URLClassLoader) {

            URLClassLoader ucl = (URLClassLoader) classLoader;

            for (URL u : ucl.getURLs()) {
              append(classpathBuilder, u);
            }

          } else if (classLoader instanceof VFSClassLoader) {

            VFSClassLoader vcl = (VFSClassLoader) classLoader;
            for (FileObject f : vcl.getFileObjects()) {
              append(classpathBuilder, f.getURL());
            }
          } else {
            throw new IllegalArgumentException("Unknown classloader type : " + classLoader.getClass().getName());
          }
        }
      } else {
        for (String s : config.getClasspathItems())
          classpathBuilder.append(File.pathSeparator).append(s);
      }

      return classpathBuilder.toString();

    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private Process _exec(Class<?> clazz, List<String> extraJvmOpts, String... args) throws IOException {
    String javaHome = System.getProperty("java.home");
    String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
    String classpath = getClasspath();

    String className = clazz.getName();

    ArrayList<String> argList = new ArrayList<String>();
    argList.addAll(Arrays.asList(javaBin, "-Dproc=" + clazz.getSimpleName(), "-cp", classpath));
    argList.addAll(extraJvmOpts);
    for (Entry<String,String> sysProp : config.getSystemProperties().entrySet()) {
      argList.add(String.format("-D%s=%s", sysProp.getKey(), sysProp.getValue()));
    }
    
    String gcPolicyArgs = System.getenv("GC_POLICY_ARGS");
    if (gcPolicyArgs == null) {
      gcPolicyArgs = "-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75";
    }
    String[] gcPolicyArgsArr = gcPolicyArgs.split("\\s+");
    argList.addAll(Arrays.asList(gcPolicyArgsArr));
    argList.addAll(Arrays.asList("-Dapple.awt.UIElement=true", Main.class.getName(), className));
    argList.addAll(Arrays.asList(args));

    ProcessBuilder builder = new ProcessBuilder(argList);

    builder.environment().put("ACCUMULO_HOME", config.getDir().getAbsolutePath());
    builder.environment().put("ACCUMULO_LOG_DIR", config.getLogDir().getAbsolutePath());
    builder.environment().put("ACCUMULO_CLIENT_CONF_PATH", config.getClientConfFile().getAbsolutePath());
    String ldLibraryPath = StringUtil.join(Arrays.asList(config.getNativeLibPaths()), File.pathSeparator);
    builder.environment().put("LD_LIBRARY_PATH", ldLibraryPath);
    builder.environment().put("DYLD_LIBRARY_PATH", ldLibraryPath);

    // if we're running under accumulo.start, we forward these env vars
    String env = System.getenv("HADOOP_PREFIX");
    if (env != null)
      builder.environment().put("HADOOP_PREFIX", env);
    env = System.getenv("ZOOKEEPER_HOME");
    if (env != null)
      builder.environment().put("ZOOKEEPER_HOME", env);
    builder.environment().put("ACCUMULO_CONF_DIR", config.getConfDir().getAbsolutePath());
    // hadoop-2.2 puts error messages in the logs if this is not set
    builder.environment().put("HADOOP_HOME", config.getDir().getAbsolutePath());

    Process process = builder.start();

    LogWriter lw;
    lw = new LogWriter(process.getErrorStream(), new File(config.getLogDir(), clazz.getSimpleName() + "_" + process.hashCode() + ".err"));
    logWriters.add(lw);
    lw.start();
    lw = new LogWriter(process.getInputStream(), new File(config.getLogDir(), clazz.getSimpleName() + "_" + process.hashCode() + ".out"));
    logWriters.add(lw);
    lw.start();

    return process;
  }

  private Process _exec(Class<?> clazz, ServerType serverType, String... args) throws IOException {

    List<String> jvmOpts = new ArrayList<String>();
    jvmOpts.add("-Xmx" + config.getMemory(serverType));

    if (config.isJDWPEnabled()) {
      Integer port = PortUtils.getRandomFreePort();
      jvmOpts.addAll(buildRemoteDebugParams(port));
      debugPorts.add(new Pair<ServerType,Integer>(serverType, port));
    }
    return _exec(clazz, jvmOpts, args);
  }

  /**
   * 
   * @param dir
   *          An empty or nonexistant temp directoy that Accumulo and Zookeeper can store data in. Creating the directory is left to the user. Java 7, Guava,
   *          and Junit provide methods for creating temporary directories.
   * @param rootPassword
   *          Initial root password for instance.
   */
  public MiniAccumuloClusterImpl(File dir, String rootPassword) throws IOException {
    this(new MiniAccumuloConfigImpl(dir, rootPassword));
  }

  /**
   * @param config
   *          initial configuration
   */
  @SuppressWarnings("deprecation")
  public MiniAccumuloClusterImpl(MiniAccumuloConfigImpl config) throws IOException {

    this.config = config.initialize();

    config.getConfDir().mkdirs();
    config.getAccumuloDir().mkdirs();
    config.getZooKeeperDir().mkdirs();
    config.getLogDir().mkdirs();
    config.getWalogDir().mkdirs();
    config.getLibDir().mkdirs();
    config.getLibExtDir().mkdirs();

    if (config.useMiniDFS()) {
      File nn = new File(config.getAccumuloDir(), "nn");
      nn.mkdirs();
      File dn = new File(config.getAccumuloDir(), "dn");
      dn.mkdirs();
      File dfs = new File(config.getAccumuloDir(), "dfs");
      dfs.mkdirs();
      Configuration conf = new Configuration();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nn.getAbsolutePath());
      conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dn.getAbsolutePath());
      conf.set(DFSConfigKeys.DFS_REPLICATION_KEY, "1");
      conf.set("dfs.support.append", "true");
      conf.set("dfs.datanode.synconclose", "true");
      conf.set("dfs.datanode.data.dir.perm", MiniDFSUtil.computeDatanodeDirectoryPermission());
      String oldTestBuildData = System.setProperty("test.build.data", dfs.getAbsolutePath());
      miniDFS = new MiniDFSCluster(conf, 1, true, null);
      if (oldTestBuildData == null)
        System.clearProperty("test.build.data");
      else
        System.setProperty("test.build.data", oldTestBuildData);
      miniDFS.waitClusterUp();
      InetSocketAddress dfsAddress = miniDFS.getNameNode().getNameNodeAddress();
      dfsUri = "hdfs://" + dfsAddress.getHostName() + ":" + dfsAddress.getPort();
      File coreFile = new File(config.getConfDir(), "core-site.xml");
      writeConfig(coreFile, Collections.singletonMap("fs.default.name", dfsUri).entrySet());
      File hdfsFile = new File(config.getConfDir(), "hdfs-site.xml");
      writeConfig(hdfsFile, conf);

      Map<String,String> siteConfig = config.getSiteConfig();
      siteConfig.put(Property.INSTANCE_DFS_URI.getKey(), dfsUri);
      siteConfig.put(Property.INSTANCE_DFS_DIR.getKey(), "/accumulo");
      config.setSiteConfig(siteConfig);
    } else {
      dfsUri = "file://";
    }

    File clientConfFile = config.getClientConfFile();
    // Write only the properties that correspond to ClientConfiguration properties
    writeConfigProperties(clientConfFile, Maps.filterEntries(config.getSiteConfig(), new Predicate<Entry<String,String>>() {
      @Override
      public boolean apply(Entry<String,String> v) {
        return ClientConfiguration.ClientProperty.getPropertyByKey(v.getKey()) != null;
      }
    }));

    File siteFile = new File(config.getConfDir(), "accumulo-site.xml");
    writeConfig(siteFile, config.getSiteConfig().entrySet());

    zooCfgFile = new File(config.getConfDir(), "zoo.cfg");
    FileWriter fileWriter = new FileWriter(zooCfgFile);

    // zookeeper uses Properties to read its config, so use that to write in order to properly escape things like Windows paths
    Properties zooCfg = new Properties();
    zooCfg.setProperty("tickTime", "2000");
    zooCfg.setProperty("initLimit", "10");
    zooCfg.setProperty("syncLimit", "5");
    zooCfg.setProperty("clientPort", config.getZooKeeperPort() + "");
    zooCfg.setProperty("maxClientCnxns", "1000");
    zooCfg.setProperty("dataDir", config.getZooKeeperDir().getAbsolutePath());
    zooCfg.store(fileWriter, null);

    fileWriter.close();

    // disable audit logging for mini....
    InputStream auditStream = this.getClass().getResourceAsStream("/auditLog.xml");

    if (auditStream != null) {
      FileUtils.copyInputStreamToFile(auditStream, new File(config.getConfDir(), "auditLog.xml"));
    }
  }

  private void writeConfig(File file, Iterable<Map.Entry<String,String>> settings) throws IOException {
    FileWriter fileWriter = new FileWriter(file);
    fileWriter.append("<configuration>\n");

    for (Entry<String,String> entry : settings) {
      String value = entry.getValue().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
      fileWriter.append("<property><name>" + entry.getKey() + "</name><value>" + value + "</value></property>\n");
    }
    fileWriter.append("</configuration>\n");
    fileWriter.close();
  }

  private void writeConfigProperties(File file, Map<String,String> settings) throws IOException {
    FileWriter fileWriter = new FileWriter(file);

    for (Entry<String,String> entry : settings.entrySet())
      fileWriter.append(entry.getKey() + "=" + entry.getValue() + "\n");
    fileWriter.close();
  }

  /**
   * Starts Accumulo and Zookeeper processes. Can only be called once.
   * 
   * @throws IllegalStateException
   *           if already started
   */
  @Override
  public void start() throws IOException, InterruptedException {

    if (!initialized) {

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          try {
            MiniAccumuloClusterImpl.this.stop();
          } catch (IOException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      });
    }

    if (zooKeeperProcess == null) {
      zooKeeperProcess = _exec(ZooKeeperServerMain.class, ServerType.ZOOKEEPER, zooCfgFile.getAbsolutePath());
    }

    if (!initialized) {
      // sleep a little bit to let zookeeper come up before calling init, seems to work better
      long startTime = System.currentTimeMillis();
      while (true) {
        Socket s = null;
        try {
          s = new Socket("localhost", config.getZooKeeperPort());
          s.getOutputStream().write("ruok\n".getBytes());
          s.getOutputStream().flush();
          byte buffer[] = new byte[100];
          int n = s.getInputStream().read(buffer);
          if (n >= 4 && new String(buffer, 0, 4).equals("imok"))
            break;
        } catch (Exception e) {
          if (System.currentTimeMillis() - startTime >= config.getZooKeeperStartupTime()) {
            throw new RuntimeException("Zookeeper did not start within " + (config.getZooKeeperStartupTime()/1000) + " seconds. Check the logs in " + config.getLogDir() + " for errors.  Last exception: " + e);
          }
          UtilWaitThread.sleep(250);
        } finally {
          if (s != null)
            s.close();
        }
      }
      Process initProcess = exec(Initialize.class, "--instance-name", config.getInstanceName(), "--password", config.getRootPassword());
      int ret = initProcess.waitFor();
      if (ret != 0) {
        throw new RuntimeException("Initialize process returned " + ret + ". Check the logs in " + config.getLogDir() + " for errors.");
      }
      initialized = true;
    }
    synchronized (tabletServerProcesses) {
      for (int i = tabletServerProcesses.size(); i < config.getNumTservers(); i++) {
        tabletServerProcesses.add(_exec(TabletServer.class, ServerType.TABLET_SERVER));
      }
    }
    int ret = 0;
    for (int i = 0; i < 5; i++) {
      ret = exec(Main.class, SetGoalState.class.getName(), MasterGoalState.NORMAL.toString()).waitFor();
      if (ret == 0)
        break;
      UtilWaitThread.sleep(1000);
    }
    if (ret != 0) {
      throw new RuntimeException("Could not set master goal state, process returned " + ret + ". Check the logs in " + config.getLogDir() + " for errors.");
    }
    if (masterProcess == null) {
      masterProcess = _exec(Master.class, ServerType.MASTER);
    }

    if (gcProcess == null) {
      gcProcess = _exec(SimpleGarbageCollector.class, ServerType.GARBAGE_COLLECTOR);
    }
  }

  private List<String> buildRemoteDebugParams(int port) {
    return Arrays.asList(new String[] {"-Xdebug", String.format("-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=%d", port)});
  }

  /**
   * @return generated remote debug ports if in debug mode.
   * @since 1.6.0
   */
  public Set<Pair<ServerType,Integer>> getDebugPorts() {
    return debugPorts;
  }

  List<ProcessReference> references(Process... procs) {
    List<ProcessReference> result = new ArrayList<ProcessReference>();
    for (Process proc : procs) {
      result.add(new ProcessReference(proc));
    }
    return result;
  }

  public Map<ServerType,Collection<ProcessReference>> getProcesses() {
    Map<ServerType,Collection<ProcessReference>> result = new HashMap<ServerType,Collection<ProcessReference>>();
    result.put(ServerType.MASTER, references(masterProcess));
    result.put(ServerType.TABLET_SERVER, references(tabletServerProcesses.toArray(new Process[0])));
    result.put(ServerType.ZOOKEEPER, references(zooKeeperProcess));
    if (null != gcProcess) {
      result.put(ServerType.GARBAGE_COLLECTOR, references(gcProcess));
    }
    return result;
  }

  public void killProcess(ServerType type, ProcessReference proc) throws ProcessNotFoundException, InterruptedException {
    boolean found = false;
    switch (type) {
      case MASTER:
        if (proc.equals(masterProcess)) {
          masterProcess.destroy();
          masterProcess.waitFor();
          masterProcess = null;
          found = true;
        }
        break;
      case TABLET_SERVER:
        synchronized (tabletServerProcesses) {
          for (Process tserver : tabletServerProcesses) {
            if (proc.equals(tserver)) {
              tabletServerProcesses.remove(tserver);
              tserver.destroy();
              tserver.waitFor();
              found = true;
              break;
            }
          }
        }
        break;
      case ZOOKEEPER:
        if (proc.equals(zooKeeperProcess)) {
          zooKeeperProcess.destroy();
          zooKeeperProcess.waitFor();
          zooKeeperProcess = null;
          found = true;
        }
        break;
      case GARBAGE_COLLECTOR:
        if (proc.equals(gcProcess)) {
          gcProcess.destroy();
          gcProcess.waitFor();
          gcProcess = null;
          found = true;
        }
        break;
    }
    if (!found)
      throw new ProcessNotFoundException();
  }

  /**
   * @return Accumulo instance name
   */
  @Override
  public String getInstanceName() {
    return config.getInstanceName();
  }

  /**
   * @return zookeeper connection string
   */
  @Override
  public String getZooKeepers() {
    return config.getZooKeepers();
  }

  /**
   * Stops Accumulo and Zookeeper processes. If stop is not called, there is a shutdown hook that is setup to kill the processes. However its probably best to
   * call stop in a finally block as soon as possible.
   */
  @Override
  public void stop() throws IOException, InterruptedException {
    for (LogWriter lw : logWriters) {
      lw.flush();
    }

    if (gcProcess != null) {
      try {
        stopProcessWithTimeout(gcProcess, 30, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        log.warn("GarbageCollector did not fully stop after 30 seconds", e);
      } catch (TimeoutException e) {
        log.warn("GarbageCollector did not fully stop after 30 seconds", e);
      }
    }
    if (masterProcess != null) {
      try {
        stopProcessWithTimeout(masterProcess, 30, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        log.warn("Master did not fully stop after 30 seconds", e);
      } catch (TimeoutException e) {
        log.warn("Master did not fully stop after 30 seconds", e);
      }
    }
    if (tabletServerProcesses != null) {
      synchronized (tabletServerProcesses) {
        for (Process tserver : tabletServerProcesses) {
          try {
            stopProcessWithTimeout(tserver, 30, TimeUnit.SECONDS);
          } catch (ExecutionException e) {
            log.warn("TabletServer did not fully stop after 30 seconds", e);
          } catch (TimeoutException e) {
            log.warn("TabletServer did not fully stop after 30 seconds", e);
          }
        }
      }
    }
    if (zooKeeperProcess != null) {
      try {
        stopProcessWithTimeout(zooKeeperProcess, 30, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        log.warn("ZooKeeper did not fully stop after 30 seconds", e);
      } catch (TimeoutException e) {
        log.warn("ZooKeeper did not fully stop after 30 seconds", e);
      }
    }

    zooKeeperProcess = null;
    masterProcess = null;
    gcProcess = null;
    tabletServerProcesses.clear();
    if (config.useMiniDFS() && miniDFS != null)
      miniDFS.shutdown();
    for (Process p : cleanup) {
      p.destroy();
      p.waitFor();
    }
    miniDFS = null;
  }

  /**
   * @since 1.6.0
   */
  @Override
  public MiniAccumuloConfigImpl getConfig() {
    return config;
  }

  /**
   * Utility method to get a connector to the MAC.
   * 
   * @since 1.6.0
   */
  @Override
  public Connector getConnector(String user, String passwd) throws AccumuloException, AccumuloSecurityException {
    Instance instance = new ZooKeeperInstance(getClientConfig());
    return instance.getConnector(user, new PasswordToken(passwd));
  }

  @Override
  public ClientConfiguration getClientConfig() {
    return new ClientConfiguration(Arrays.asList(new MapConfiguration(config.getSiteConfig()))).withInstance(this.getInstanceName()).withZkHosts(
        this.getZooKeepers());
  }

  public FileSystem getFileSystem() {
    try {
      return FileSystem.get(new URI(dfsUri), new Configuration());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  private int stopProcessWithTimeout(final Process proc, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    FutureTask<Integer> future = new FutureTask<Integer>(new Callable<Integer>() {
        @Override
        public Integer call() throws InterruptedException {
          proc.destroy();
          return proc.waitFor();
        }
    });

    executor.execute(future);

    return future.get(timeout, unit);
  }
}

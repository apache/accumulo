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

import static com.google.common.base.Charsets.UTF_8;

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
import org.apache.accumulo.cluster.ClusterServerType;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.master.state.SetGoalState;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.util.AccumuloStatus;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.zookeeper.ZooReaderWriterFactory;
import org.apache.accumulo.start.Main;
import org.apache.accumulo.start.classloader.vfs.MiniDFSUtil;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

/**
 * A utility class that will create Zookeeper and Accumulo processes that write all of their data to a single local directory. This class makes it easy to test
 * code against a real Accumulo instance. Its much more accurate for testing than {@link org.apache.accumulo.core.client.mock.MockAccumulo}, but much slower.
 *
 * @since 1.6.0
 */
public class MiniAccumuloClusterImpl implements AccumuloCluster {
  private static final Logger log = LoggerFactory.getLogger(MiniAccumuloClusterImpl.class);

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

  private Set<Pair<ClusterServerType,Integer>> debugPorts = new HashSet<Pair<ClusterServerType,Integer>>();

  private File zooCfgFile;
  private String dfsUri;

  public List<LogWriter> getLogWriters() {
    return logWriters;
  }

  private List<LogWriter> logWriters = new ArrayList<MiniAccumuloClusterImpl.LogWriter>();

  private MiniAccumuloConfigImpl config;
  private MiniDFSCluster miniDFS = null;
  private List<Process> cleanup = new ArrayList<Process>();

  private ExecutorService executor;

  private MiniAccumuloClusterControl clusterControl;

  File getZooCfgFile() {
    return zooCfgFile;
  }

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

      if (config.getHadoopConfDir() != null)
        classpathBuilder.append(File.pathSeparator).append(config.getHadoopConfDir().getAbsolutePath());

      if (config.getClasspathItems() == null) {

        // assume 0 is the system classloader and skip it
        for (int i = 1; i < classloaders.size(); i++) {
          ClassLoader classLoader = classloaders.get(i);

          if (classLoader instanceof URLClassLoader) {

            for (URL u : ((URLClassLoader) classLoader).getURLs()) {
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
    argList.addAll(Arrays.asList("-XX:+UseConcMarkSweepGC", "-XX:CMSInitiatingOccupancyFraction=75", "-Dapple.awt.UIElement=true",
        "-Djava.net.preferIPv4Stack=true", Main.class.getName(), className));
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
    if (config.getHadoopConfDir() != null)
      builder.environment().put("HADOOP_CONF_DIR", config.getHadoopConfDir().getAbsolutePath());

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

  Process _exec(Class<?> clazz, ClusterServerType serverType, String... args) throws IOException {

    List<String> jvmOpts = new ArrayList<String>();
    jvmOpts.add("-Xmx" + config.getMemory(serverType));

    if (config.isJDWPEnabled()) {
      Integer port = PortUtils.getRandomFreePort();
      jvmOpts.addAll(buildRemoteDebugParams(port));
      debugPorts.add(new Pair<ClusterServerType,Integer>(serverType, port));
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
    config.getLogDir().mkdirs();
    config.getLibDir().mkdirs();
    config.getLibExtDir().mkdirs();

    if (!config.useExistingInstance()) {
      config.getZooKeeperDir().mkdirs();
      config.getWalogDir().mkdirs();
      config.getAccumuloDir().mkdirs();
    }

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
    } else if (config.useExistingInstance()) {
      dfsUri = CachedConfiguration.getInstance().get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY);
    } else {
      dfsUri = "file:///";
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

    if (!config.useExistingInstance()) {
      zooCfgFile = new File(config.getConfDir(), "zoo.cfg");
      FileWriter fileWriter = new FileWriter(zooCfgFile);

      // zookeeper uses Properties to read its config, so use that to write in order to properly escape things like Windows paths
      Properties zooCfg = new Properties();
      zooCfg.setProperty("tickTime", "2000");
      zooCfg.setProperty("initLimit", "10");
      zooCfg.setProperty("syncLimit", "5");
      zooCfg.setProperty("clientPortAddress", "127.0.0.1");
      zooCfg.setProperty("clientPort", config.getZooKeeperPort() + "");
      zooCfg.setProperty("maxClientCnxns", "1000");
      zooCfg.setProperty("dataDir", config.getZooKeeperDir().getAbsolutePath());
      zooCfg.store(fileWriter, null);

      fileWriter.close();
    }

    // disable audit logging for mini....
    InputStream auditStream = this.getClass().getResourceAsStream("/auditLog.xml");

    if (auditStream != null) {
      FileUtils.copyInputStreamToFile(auditStream, new File(config.getConfDir(), "auditLog.xml"));
    }

    clusterControl = new MiniAccumuloClusterControl(this);
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
   */
  @Override
  public synchronized void start() throws IOException, InterruptedException {
    MiniAccumuloClusterControl control = getClusterControl();

    if (config.useExistingInstance()) {
      Configuration acuConf = config.getAccumuloConfiguration();
      Configuration hadoopConf = config.getHadoopConfiguration();

      ConfigurationCopy cc = new ConfigurationCopy(acuConf);
      VolumeManager fs;
      try {
        fs = VolumeManagerImpl.get(cc, hadoopConf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      Path instanceIdPath = Accumulo.getAccumuloInstanceIdPath(fs);

      String instanceIdFromFile = ZooUtil.getInstanceIDFromHdfs(instanceIdPath, cc, hadoopConf);
      IZooReaderWriter zrw = new ZooReaderWriterFactory().getZooReaderWriter(cc.get(Property.INSTANCE_ZK_HOST),
          (int) cc.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT), cc.get(Property.INSTANCE_SECRET));

      String rootPath = ZooUtil.getRoot(instanceIdFromFile);

      String instanceName = null;
      try {
        for (String name : zrw.getChildren(Constants.ZROOT + Constants.ZINSTANCES)) {
          String instanceNamePath = Constants.ZROOT + Constants.ZINSTANCES + "/" + name;
          byte[] bytes = zrw.getData(instanceNamePath, new Stat());
          String iid = new String(bytes, UTF_8);
          if (iid.equals(instanceIdFromFile)) {
            instanceName = name;
          }
        }
      } catch (KeeperException e) {
        throw new RuntimeException("Unable to read instance name from zookeeper.", e);
      }
      if (instanceName == null)
        throw new RuntimeException("Unable to read instance name from zookeeper.");

      config.setInstanceName(instanceName);
      if (!AccumuloStatus.isAccumuloOffline(zrw, rootPath))
        throw new RuntimeException("The Accumulo instance being used is already running. Aborting.");
    } else {
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

      control.start(ClusterServerType.ZOOKEEPER);

      if (!initialized) {
        // sleep a little bit to let zookeeper come up before calling init, seems to work better
        long startTime = System.currentTimeMillis();
        while (true) {
          Socket s = null;
          try {
            s = new Socket("localhost", config.getZooKeeperPort());
            s.setReuseAddress(true);
            s.getOutputStream().write("ruok\n".getBytes());
            s.getOutputStream().flush();
            byte buffer[] = new byte[100];
            int n = s.getInputStream().read(buffer);
            if (n >= 4 && new String(buffer, 0, 4).equals("imok"))
              break;
          } catch (Exception e) {
            if (System.currentTimeMillis() - startTime >= config.getZooKeeperStartupTime()) {
              throw new ZooKeeperBindException("Zookeeper did not start within " + (config.getZooKeeperStartupTime() / 1000) + " seconds. Check the logs in "
                  + config.getLogDir() + " for errors.  Last exception: " + e);
            }
            // Don't spin absurdly fast
            Thread.sleep(250);
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
    }

    log.info("Starting MAC against instance {} and zookeeper(s) {}.", config.getInstanceName(), config.getZooKeepers());

    control.start(ClusterServerType.TABLET_SERVER);

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

    control.start(ClusterServerType.MASTER);
    control.start(ClusterServerType.GARBAGE_COLLECTOR);

    if (null == executor) {
      executor = Executors.newSingleThreadExecutor();
    }
  }

  private List<String> buildRemoteDebugParams(int port) {
    return Arrays.asList(new String[] {"-Xdebug", String.format("-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=%d", port)});
  }

  /**
   * @return generated remote debug ports if in debug mode.
   * @since 1.6.0
   */
  public Set<Pair<ClusterServerType,Integer>> getDebugPorts() {
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
    MiniAccumuloClusterControl control = getClusterControl();
    result.put(ServerType.MASTER, references(control.masterProcess));
    result.put(ServerType.TABLET_SERVER, references(control.tabletServerProcesses.toArray(new Process[0])));
    result.put(ServerType.ZOOKEEPER, references(control.zooKeeperProcess));
    if (null != control.gcProcess) {
      result.put(ServerType.GARBAGE_COLLECTOR, references(control.gcProcess));
    }
    return result;
  }

  public void killProcess(ServerType type, ProcessReference proc) throws ProcessNotFoundException, InterruptedException {
    getClusterControl().killProcess(type, proc);
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
  public synchronized void stop() throws IOException, InterruptedException {
    if (null == executor) {
      // keep repeated calls to stop() from failing
      return;
    }

    for (LogWriter lw : logWriters) {
      lw.flush();
    }

    MiniAccumuloClusterControl control = getClusterControl();

    control.stop(ClusterServerType.GARBAGE_COLLECTOR, null);
    control.stop(ClusterServerType.MASTER, null);
    control.stop(ClusterServerType.TABLET_SERVER, null);
    control.stop(ClusterServerType.ZOOKEEPER, null);

    // ACCUMULO-2985 stop the ExecutorService after we finished using it to stop accumulo procs
    if (null != executor) {
      List<Runnable> tasksRemaining = executor.shutdownNow();

      // the single thread executor shouldn't have any pending tasks, but check anyways
      if (!tasksRemaining.isEmpty()) {
        log.warn("Unexpectedly had " + tasksRemaining.size() + " task(s) remaining in threadpool for execution when being stopped");
      }

      executor = null;
    }

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
  public MiniAccumuloConfigImpl getConfig() {
    return config;
  }

  /**
   * Utility method to get a connector to the MAC.
   *
   * @since 1.6.0
   */
  @Deprecated
  public Connector getConnector(String user, String passwd) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, new PasswordToken(passwd));
  }

  @Override
  public Connector getConnector(String user, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    Instance instance = new ZooKeeperInstance(getClientConfig());
    return instance.getConnector(user, token);
  }

  @Override
  public ClientConfiguration getClientConfig() {
    return new ClientConfiguration(Arrays.asList(new MapConfiguration(config.getSiteConfig()))).withInstance(this.getInstanceName()).withZkHosts(
        this.getZooKeepers());
  }

  @Override
  public FileSystem getFileSystem() {
    try {
      return FileSystem.get(new URI(dfsUri), new Configuration());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Visible for testing
  protected void setShutdownExecutor(ExecutorService svc) {
    this.executor = svc;
  }

  // Visible for testing
  protected ExecutorService getShutdownExecutor() {
    return executor;
  }

  int stopProcessWithTimeout(final Process proc, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
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

  /**
   * Get programmatic interface to information available in a normal monitor. XXX the returned structure won't contain information about the metadata table
   * until there is data in it. e.g. if you want to see the metadata table you should create a table.
   *
   * @since 1.6.1
   */
  public MasterMonitorInfo getMasterMonitorInfo() throws AccumuloException, AccumuloSecurityException {
    MasterClientService.Iface client = null;
    MasterMonitorInfo stats = null;
    try {
      Instance instance = new ZooKeeperInstance(getClientConfig());
      client = MasterClient.getConnectionWithRetry(instance);
      stats = client.getMasterStats(Tracer.traceInfo(), SystemCredentials.get(instance).toThrift(instance));
    } catch (ThriftSecurityException exception) {
      throw new AccumuloSecurityException(exception);
    } catch (TException exception) {
      throw new AccumuloException(exception);
    } finally {
      if (client != null) {
        MasterClient.close(client);
      }
    }
    return stats;
  }

  @Override
  public MiniAccumuloClusterControl getClusterControl() {
    return clusterControl;
  }
}

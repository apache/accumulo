/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.miniclusterImpl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.MasterClient;
import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.master.state.SetGoalState;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerUtil;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.server.util.AccumuloStatus;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.accumulo.start.Main;
import org.apache.accumulo.start.classloader.vfs.MiniDFSUtil;
import org.apache.commons.io.IOUtils;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class provides the backing implementation for {@link MiniAccumuloCluster}, and may contain
 * features for internal testing which have not yet been promoted to the public API. It's best to
 * use {@link MiniAccumuloCluster} whenever possible. Use of this class risks API breakage between
 * versions.
 *
 * @since 1.6.0
 */
public class MiniAccumuloClusterImpl implements AccumuloCluster {
  private static final Logger log = LoggerFactory.getLogger(MiniAccumuloClusterImpl.class);

  private boolean initialized = false;

  private Set<Pair<ServerType,Integer>> debugPorts = new HashSet<>();

  private File zooCfgFile;
  private String dfsUri;
  private SiteConfiguration siteConfig;
  private ServerContext context;
  private Properties clientProperties;

  private MiniAccumuloConfigImpl config;
  private MiniDFSCluster miniDFS = null;
  private List<Process> cleanup = new ArrayList<>();

  private ExecutorService executor;

  private MiniAccumuloClusterControl clusterControl;

  File getZooCfgFile() {
    return zooCfgFile;
  }

  public ProcessInfo exec(Class<?> clazz, String... args) throws IOException {
    return exec(clazz, null, args);
  }

  public ProcessInfo exec(Class<?> clazz, List<String> jvmArgs, String... args) throws IOException {
    ArrayList<String> jvmArgs2 = new ArrayList<>(1 + (jvmArgs == null ? 0 : jvmArgs.size()));
    jvmArgs2.add("-Xmx" + config.getDefaultMemory());
    if (jvmArgs != null) {
      jvmArgs2.addAll(jvmArgs);
    }
    return _exec(clazz, jvmArgs2, args);
  }

  private String getClasspath() {
    StringBuilder classpathBuilder = new StringBuilder();
    classpathBuilder.append(config.getConfDir().getAbsolutePath());

    if (config.getHadoopConfDir() != null) {
      classpathBuilder.append(File.pathSeparator)
          .append(config.getHadoopConfDir().getAbsolutePath());
    }

    if (config.getClasspathItems() == null) {
      String javaClassPath = System.getProperty("java.class.path");
      if (javaClassPath == null) {
        throw new IllegalStateException("java.class.path is not set");
      }
      classpathBuilder.append(File.pathSeparator).append(javaClassPath);
    } else {
      for (String s : config.getClasspathItems()) {
        classpathBuilder.append(File.pathSeparator).append(s);
      }
    }

    return classpathBuilder.toString();
  }

  public static class ProcessInfo {

    private final Process process;
    private final File stdOut;

    public ProcessInfo(Process process, File stdOut) {
      this.process = process;
      this.stdOut = stdOut;
    }

    public Process getProcess() {
      return process;
    }

    public String readStdOut() {
      try (InputStream in = new FileInputStream(stdOut)) {
        return IOUtils.toString(in, UTF_8);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @SuppressFBWarnings(value = {"COMMAND_INJECTION", "PATH_TRAVERSAL_IN"},
      justification = "mini runs in the same security context as user providing the args")
  private ProcessInfo _exec(Class<?> clazz, List<String> extraJvmOpts, String... args)
      throws IOException {
    String javaHome = System.getProperty("java.home");
    String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
    String classpath = getClasspath();

    String className = clazz.getName();

    ArrayList<String> argList = new ArrayList<>();
    argList.addAll(Arrays.asList(javaBin, "-Dproc=" + clazz.getSimpleName(), "-cp", classpath));
    argList.addAll(extraJvmOpts);
    for (Entry<String,String> sysProp : config.getSystemProperties().entrySet()) {
      argList.add(String.format("-D%s=%s", sysProp.getKey(), sysProp.getValue()));
    }
    // @formatter:off
    argList.addAll(Arrays.asList(
        "-Dapple.awt.UIElement=true",
        "-Djava.net.preferIPv4Stack=true",
        "-XX:+PerfDisableSharedMem",
        "-XX:+AlwaysPreTouch",
        Main.class.getName(), className));
    // @formatter:on
    argList.addAll(Arrays.asList(args));

    ProcessBuilder builder = new ProcessBuilder(argList);

    builder.environment().put("ACCUMULO_HOME", config.getDir().getAbsolutePath());
    builder.environment().put("ACCUMULO_LOG_DIR", config.getLogDir().getAbsolutePath());
    builder.environment().put("ACCUMULO_CLIENT_CONF_PATH",
        config.getClientConfFile().getAbsolutePath());
    String ldLibraryPath = Joiner.on(File.pathSeparator).join(config.getNativeLibPaths());
    builder.environment().put("LD_LIBRARY_PATH", ldLibraryPath);
    builder.environment().put("DYLD_LIBRARY_PATH", ldLibraryPath);

    // if we're running under accumulo.start, we forward these env vars
    String env = System.getenv("HADOOP_HOME");
    if (env != null) {
      builder.environment().put("HADOOP_HOME", env);
    }
    env = System.getenv("ZOOKEEPER_HOME");
    if (env != null) {
      builder.environment().put("ZOOKEEPER_HOME", env);
    }
    builder.environment().put("ACCUMULO_CONF_DIR", config.getConfDir().getAbsolutePath());
    if (config.getHadoopConfDir() != null) {
      builder.environment().put("HADOOP_CONF_DIR", config.getHadoopConfDir().getAbsolutePath());
    }

    log.debug("Starting MiniAccumuloCluster process with class: " + clazz.getSimpleName()
        + "\n, jvmOpts: " + extraJvmOpts + "\n, classpath: " + classpath + "\n, args: " + argList
        + "\n, environment: " + builder.environment());

    int hashcode = builder.hashCode();

    File stdOut = new File(config.getLogDir(), clazz.getSimpleName() + "_" + hashcode + ".out");
    File stdErr = new File(config.getLogDir(), clazz.getSimpleName() + "_" + hashcode + ".err");

    Process process = builder.redirectError(stdErr).redirectOutput(stdOut).start();

    cleanup.add(process);

    return new ProcessInfo(process, stdOut);
  }

  public ProcessInfo _exec(Class<?> clazz, ServerType serverType,
      Map<String,String> configOverrides, String... args) throws IOException {
    List<String> jvmOpts = new ArrayList<>();
    if (serverType == ServerType.ZOOKEEPER) {
      // disable zookeeper's log4j 1.2 jmx support, which depends on log4j 1.2 on the class path,
      // which we don't need or expect to be there
      jvmOpts.add("-Dzookeeper.jmx.log4j.disable=true");
    }
    jvmOpts.add("-Xmx" + config.getMemory(serverType));
    if (configOverrides != null && !configOverrides.isEmpty()) {
      File siteFile =
          Files.createTempFile(config.getConfDir().toPath(), "accumulo", ".properties").toFile();
      Map<String,String> confMap = new HashMap<>();
      confMap.putAll(config.getSiteConfig());
      confMap.putAll(configOverrides);
      writeConfigProperties(siteFile, confMap);
      jvmOpts.add("-Daccumulo.properties=" + siteFile.getName());
    }

    if (config.isJDWPEnabled()) {
      int port = PortUtils.getRandomFreePort();
      jvmOpts.addAll(buildRemoteDebugParams(port));
      debugPorts.add(new Pair<>(serverType, port));
    }
    return _exec(clazz, jvmOpts, args);
  }

  /**
   *
   * @param dir
   *          An empty or nonexistent temp directory that Accumulo and Zookeeper can store data in.
   *          Creating the directory is left to the user. Java 7, Guava, and Junit provide methods
   *          for creating temporary directories.
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

    mkdirs(config.getConfDir());
    mkdirs(config.getLogDir());
    mkdirs(config.getLibDir());
    mkdirs(config.getLibExtDir());

    if (!config.useExistingInstance()) {
      if (!config.useExistingZooKeepers()) {
        mkdirs(config.getZooKeeperDir());
      }
      mkdirs(config.getAccumuloDir());
    }

    if (config.useMiniDFS()) {
      File nn = new File(config.getAccumuloDir(), "nn");
      mkdirs(nn);
      File dn = new File(config.getAccumuloDir(), "dn");
      mkdirs(dn);
      File dfs = new File(config.getAccumuloDir(), "dfs");
      mkdirs(dfs);
      Configuration conf = new Configuration();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nn.getAbsolutePath());
      conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dn.getAbsolutePath());
      conf.set(DFSConfigKeys.DFS_REPLICATION_KEY, "1");
      conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, "1");
      conf.set("dfs.support.append", "true");
      conf.set("dfs.datanode.synconclose", "true");
      conf.set("dfs.datanode.data.dir.perm", MiniDFSUtil.computeDatanodeDirectoryPermission());
      String oldTestBuildData = System.setProperty("test.build.data", dfs.getAbsolutePath());
      miniDFS = new MiniDFSCluster.Builder(conf).build();
      if (oldTestBuildData == null) {
        System.clearProperty("test.build.data");
      } else {
        System.setProperty("test.build.data", oldTestBuildData);
      }
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
      dfsUri = config.getHadoopConfiguration().get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY);
    } else {
      dfsUri = "file:///";
    }

    File clientConfFile = config.getClientConfFile();
    // Write only the properties that correspond to ClientConfiguration properties
    writeConfigProperties(clientConfFile,
        Maps.filterEntries(config.getSiteConfig(),
            v -> org.apache.accumulo.core.client.ClientConfiguration.ClientProperty
                .getPropertyByKey(v.getKey()) != null));

    Map<String,String> clientProps = config.getClientProps();
    clientProps.put(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), config.getZooKeepers());
    clientProps.put(ClientProperty.INSTANCE_NAME.getKey(), config.getInstanceName());
    if (!clientProps.containsKey(ClientProperty.AUTH_TYPE.getKey())) {
      clientProps.put(ClientProperty.AUTH_TYPE.getKey(), "password");
      clientProps.put(ClientProperty.AUTH_PRINCIPAL.getKey(), config.getRootUserName());
      clientProps.put(ClientProperty.AUTH_TOKEN.getKey(), config.getRootPassword());
    }

    File clientPropsFile = config.getClientPropsFile();
    writeConfigProperties(clientPropsFile, clientProps);

    File siteFile = new File(config.getConfDir(), "accumulo.properties");
    writeConfigProperties(siteFile, config.getSiteConfig());
    siteConfig = SiteConfiguration.fromFile(siteFile).build();

    if (!config.useExistingInstance() && !config.useExistingZooKeepers()) {
      zooCfgFile = new File(config.getConfDir(), "zoo.cfg");
      FileWriter fileWriter = new FileWriter(zooCfgFile);

      // zookeeper uses Properties to read its config, so use that to write in order to properly
      // escape things like Windows paths
      Properties zooCfg = new Properties();
      zooCfg.setProperty("tickTime", "2000");
      zooCfg.setProperty("initLimit", "10");
      zooCfg.setProperty("syncLimit", "5");
      zooCfg.setProperty("clientPortAddress", "127.0.0.1");
      zooCfg.setProperty("clientPort", config.getZooKeeperPort() + "");
      zooCfg.setProperty("maxClientCnxns", "1000");
      zooCfg.setProperty("dataDir", config.getZooKeeperDir().getAbsolutePath());
      zooCfg.setProperty("4lw.commands.whitelist", "ruok,wchs");
      zooCfg.setProperty("admin.enableServer", "false");
      zooCfg.store(fileWriter, null);

      fileWriter.close();
    }
    clusterControl = new MiniAccumuloClusterControl(this);
  }

  private static void mkdirs(File dir) {
    if (!dir.mkdirs()) {
      log.warn("Unable to create {}", dir);
    }
  }

  private void writeConfig(File file, Iterable<Map.Entry<String,String>> settings)
      throws IOException {
    FileWriter fileWriter = new FileWriter(file);
    fileWriter.append("<configuration>\n");

    for (Entry<String,String> entry : settings) {
      String value =
          entry.getValue().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
      fileWriter.append(
          "<property><name>" + entry.getKey() + "</name><value>" + value + "</value></property>\n");
    }
    fileWriter.append("</configuration>\n");
    fileWriter.close();
  }

  private void writeConfigProperties(File file, Map<String,String> settings) throws IOException {
    FileWriter fileWriter = new FileWriter(file);

    for (Entry<String,String> entry : settings.entrySet()) {
      fileWriter.append(entry.getKey() + "=" + entry.getValue() + "\n");
    }
    fileWriter.close();
  }

  /**
   * Starts Accumulo and Zookeeper processes. Can only be called once.
   */
  @SuppressFBWarnings(value = "UNENCRYPTED_SOCKET",
      justification = "insecure socket used for reservation")
  @Override
  public synchronized void start() throws IOException, InterruptedException {
    if (config.useMiniDFS() && miniDFS == null) {
      throw new IllegalStateException("Cannot restart mini when using miniDFS");
    }

    MiniAccumuloClusterControl control = getClusterControl();

    if (config.useExistingInstance()) {
      AccumuloConfiguration acuConf = config.getAccumuloConfiguration();
      Configuration hadoopConf = config.getHadoopConfiguration();

      ConfigurationCopy cc = new ConfigurationCopy(acuConf);
      Path instanceIdPath;
      try (var fs = VolumeManagerImpl.get(cc, hadoopConf)) {
        instanceIdPath = ServerUtil.getAccumuloInstanceIdPath(fs);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      String instanceIdFromFile =
          VolumeManager.getInstanceIDFromHdfs(instanceIdPath, cc, hadoopConf);
      ZooReaderWriter zrw = new ZooReaderWriter(cc.get(Property.INSTANCE_ZK_HOST),
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
      if (instanceName == null) {
        throw new RuntimeException("Unable to read instance name from zookeeper.");
      }

      config.setInstanceName(instanceName);
      if (!AccumuloStatus.isAccumuloOffline(zrw, rootPath)) {
        throw new RuntimeException(
            "The Accumulo instance being used is already running. Aborting.");
      }
    } else {
      if (!initialized) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          try {
            MiniAccumuloClusterImpl.this.stop();
          } catch (IOException e) {
            log.error("IOException while attempting to stop the MiniAccumuloCluster.", e);
          } catch (InterruptedException e) {
            log.error("The stopping of MiniAccumuloCluster was interrupted.", e);
          }
        }));
      }

      if (!config.useExistingZooKeepers()) {
        control.start(ServerType.ZOOKEEPER);
      }

      if (!initialized) {
        if (!config.useExistingZooKeepers()) {
          // sleep a little bit to let zookeeper come up before calling init, seems to work better
          long startTime = System.currentTimeMillis();
          while (true) {
            try (Socket s = new Socket("localhost", config.getZooKeeperPort())) {
              s.setReuseAddress(true);
              s.getOutputStream().write("ruok\n".getBytes());
              s.getOutputStream().flush();
              byte[] buffer = new byte[100];
              int n = s.getInputStream().read(buffer);
              if (n >= 4 && new String(buffer, 0, 4).equals("imok")) {
                break;
              }
            } catch (Exception e) {
              if (System.currentTimeMillis() - startTime >= config.getZooKeeperStartupTime()) {
                throw new ZooKeeperBindException("Zookeeper did not start within "
                    + (config.getZooKeeperStartupTime() / 1000) + " seconds. Check the logs in "
                    + config.getLogDir() + " for errors.  Last exception: " + e);
              }
              // Don't spin absurdly fast
              sleepUninterruptibly(250, TimeUnit.MILLISECONDS);
            }
          }
        }

        LinkedList<String> args = new LinkedList<>();
        args.add("--instance-name");
        args.add(config.getInstanceName());
        args.add("--user");
        args.add(config.getRootUserName());
        args.add("--clear-instance-name");

        // If we aren't using SASL, add in the root password
        final String saslEnabled =
            config.getSiteConfig().get(Property.INSTANCE_RPC_SASL_ENABLED.getKey());
        if (saslEnabled == null || !Boolean.parseBoolean(saslEnabled)) {
          args.add("--password");
          args.add(config.getRootPassword());
        }

        Process initProcess = exec(Initialize.class, args.toArray(new String[0])).getProcess();
        int ret = initProcess.waitFor();
        if (ret != 0) {
          throw new RuntimeException("Initialize process returned " + ret + ". Check the logs in "
              + config.getLogDir() + " for errors.");
        }
        initialized = true;
      }
    }

    log.info("Starting MAC against instance {} and zookeeper(s) {}.", config.getInstanceName(),
        config.getZooKeepers());

    control.start(ServerType.TABLET_SERVER);

    int ret = 0;
    for (int i = 0; i < 5; i++) {
      ret = exec(Main.class, SetGoalState.class.getName(), MasterGoalState.NORMAL.toString())
          .getProcess().waitFor();
      if (ret == 0) {
        break;
      }
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
    if (ret != 0) {
      throw new RuntimeException("Could not set master goal state, process returned " + ret
          + ". Check the logs in " + config.getLogDir() + " for errors.");
    }

    control.start(ServerType.MASTER);
    control.start(ServerType.GARBAGE_COLLECTOR);

    if (executor == null) {
      executor = Executors.newSingleThreadExecutor();
    }
  }

  private List<String> buildRemoteDebugParams(int port) {
    return Arrays.asList("-Xdebug",
        String.format("-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=%d", port));
  }

  /**
   * @return generated remote debug ports if in debug mode.
   * @since 1.6.0
   */
  public Set<Pair<ServerType,Integer>> getDebugPorts() {
    return debugPorts;
  }

  List<ProcessReference> references(Process... procs) {
    List<ProcessReference> result = new ArrayList<>();
    for (Process proc : procs) {
      result.add(new ProcessReference(proc));
    }
    return result;
  }

  public Map<ServerType,Collection<ProcessReference>> getProcesses() {
    Map<ServerType,Collection<ProcessReference>> result = new HashMap<>();
    MiniAccumuloClusterControl control = getClusterControl();
    result.put(ServerType.MASTER, references(control.masterProcess));
    result.put(ServerType.TABLET_SERVER,
        references(control.tabletServerProcesses.toArray(new Process[0])));
    if (control.zooKeeperProcess != null) {
      result.put(ServerType.ZOOKEEPER, references(control.zooKeeperProcess));
    }
    if (control.gcProcess != null) {
      result.put(ServerType.GARBAGE_COLLECTOR, references(control.gcProcess));
    }
    return result;
  }

  public void killProcess(ServerType type, ProcessReference proc)
      throws ProcessNotFoundException, InterruptedException {
    getClusterControl().killProcess(type, proc);
  }

  @Override
  public String getInstanceName() {
    return config.getInstanceName();
  }

  @Override
  public String getZooKeepers() {
    return config.getZooKeepers();
  }

  @Override
  public synchronized ServerContext getServerContext() {
    if (context == null) {
      context = new ServerContext(siteConfig);
    }
    return context;
  }

  /**
   * Stops Accumulo and Zookeeper processes. If stop is not called, there is a shutdown hook that is
   * setup to kill the processes. However it's probably best to call stop in a finally block as soon
   * as possible.
   */
  @Override
  public synchronized void stop() throws IOException, InterruptedException {
    if (executor == null) {
      // keep repeated calls to stop() from failing
      return;
    }

    MiniAccumuloClusterControl control = getClusterControl();

    control.stop(ServerType.GARBAGE_COLLECTOR, null);
    control.stop(ServerType.MASTER, null);
    control.stop(ServerType.TABLET_SERVER, null);
    control.stop(ServerType.ZOOKEEPER, null);

    // ACCUMULO-2985 stop the ExecutorService after we finished using it to stop accumulo procs
    if (executor != null) {
      List<Runnable> tasksRemaining = executor.shutdownNow();

      // the single thread executor shouldn't have any pending tasks, but check anyways
      if (!tasksRemaining.isEmpty()) {
        log.warn(
            "Unexpectedly had {} task(s) remaining in threadpool for execution when being stopped",
            tasksRemaining.size());
      }

      executor = null;
    }

    if (config.useMiniDFS() && miniDFS != null) {
      miniDFS.shutdown();
    }
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

  @Override
  public AccumuloClient createAccumuloClient(String user, AuthenticationToken token) {
    return Accumulo.newClient().from(getClientProperties()).as(user, token).build();
  }

  @SuppressWarnings("deprecation")
  @Override
  public org.apache.accumulo.core.client.ClientConfiguration getClientConfig() {
    return org.apache.accumulo.core.client.ClientConfiguration.fromMap(config.getSiteConfig())
        .withInstance(this.getInstanceName()).withZkHosts(this.getZooKeepers());
  }

  @Override
  public synchronized Properties getClientProperties() {
    if (clientProperties == null) {
      clientProperties =
          Accumulo.newClientProperties().from(config.getClientPropsFile().toPath()).build();
    }
    return clientProperties;
  }

  @Override
  public FileSystem getFileSystem() {
    try {
      return FileSystem.get(new URI(dfsUri), new Configuration());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  protected void setShutdownExecutor(ExecutorService svc) {
    this.executor = svc;
  }

  @VisibleForTesting
  protected ExecutorService getShutdownExecutor() {
    return executor;
  }

  int stopProcessWithTimeout(final Process proc, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    FutureTask<Integer> future = new FutureTask<>(() -> {
      proc.destroy();
      return proc.waitFor();
    });

    executor.execute(future);

    return future.get(timeout, unit);
  }

  /**
   * Get programmatic interface to information available in a normal monitor. XXX the returned
   * structure won't contain information about the metadata table until there is data in it. e.g. if
   * you want to see the metadata table you should create a table.
   *
   * @since 1.6.1
   */
  public MasterMonitorInfo getMasterMonitorInfo()
      throws AccumuloException, AccumuloSecurityException {
    MasterClientService.Iface client = null;
    while (true) {
      try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
        client = MasterClient.getConnectionWithRetry((ClientContext) c);
        return client.getMasterStats(TraceUtil.traceInfo(), ((ClientContext) c).rpcCreds());
      } catch (ThriftSecurityException exception) {
        throw new AccumuloSecurityException(exception);
      } catch (ThriftNotActiveServiceException e) {
        // Let it loop, fetching a new location
        log.debug("Contacted a Master which is no longer active, retrying");
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } catch (TException exception) {
        throw new AccumuloException(exception);
      } finally {
        if (client != null) {
          MasterClient.close(client);
        }
      }
    }
  }

  public synchronized MiniDFSCluster getMiniDfs() {
    return this.miniDFS;
  }

  @Override
  public MiniAccumuloClusterControl getClusterControl() {
    return clusterControl;
  }

  @Override
  public Path getTemporaryPath() {
    if (config.useMiniDFS()) {
      return new Path("/tmp/");
    } else {
      File tmp = new File(config.getDir(), "tmp");
      mkdirs(tmp);
      return new Path(tmp.toString());
    }
  }

  @Override
  public AccumuloConfiguration getSiteConfiguration() {
    return new ConfigurationCopy(
        Iterables.concat(DefaultConfiguration.getInstance(), config.getSiteConfig().entrySet()));
  }

  @Override
  public String getAccumuloPropertiesPath() {
    return new File(config.getConfDir(), "accumulo.properties").toString();
  }

  @Override
  public String getClientPropsPath() {
    return config.getClientPropsFile().getAbsolutePath();
  }
}

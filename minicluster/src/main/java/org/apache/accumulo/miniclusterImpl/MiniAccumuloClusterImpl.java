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
package org.apache.accumulo.miniclusterImpl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.manager.thrift.ManagerGoalState;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.manager.state.SetGoalState;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerDirs;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.server.util.AccumuloStatus;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.accumulo.start.Main;
import org.apache.accumulo.start.classloader.vfs.MiniDFSUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Suppliers;

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

  private final Set<Pair<ServerType,Integer>> debugPorts = new HashSet<>();
  private final File zooCfgFile;
  private final String dfsUri;
  private final MiniAccumuloConfigImpl config;
  private final Supplier<Properties> clientProperties;
  private final SiteConfiguration siteConfig;
  private final Supplier<ServerContext> context;
  private final AtomicReference<MiniDFSCluster> miniDFS = new AtomicReference<>();
  private final List<Process> cleanup = new ArrayList<>();
  private final MiniAccumuloClusterControl clusterControl;

  private boolean initialized = false;
  private ExecutorService executor;

  /**
   *
   * @param dir An empty or nonexistent temp directory that Accumulo and Zookeeper can store data
   *        in. Creating the directory is left to the user. Java 7, Guava, and Junit provide methods
   *        for creating temporary directories.
   * @param rootPassword Initial root password for instance.
   */
  public MiniAccumuloClusterImpl(File dir, String rootPassword) throws IOException {
    this(new MiniAccumuloConfigImpl(dir, rootPassword));
  }

  /**
   * @param config initial configuration
   */
  public MiniAccumuloClusterImpl(MiniAccumuloConfigImpl config) throws IOException {

    this.config = config.initialize();
    this.clientProperties = Suppliers.memoize(
        () -> Accumulo.newClientProperties().from(config.getClientPropsFile().toPath()).build());

    if (Boolean.valueOf(config.getSiteConfig().get(Property.TSERV_NATIVEMAP_ENABLED.getKey()))
        && config.getNativeLibPaths().length == 0
        && !config.getSystemProperties().containsKey("accumulo.native.lib.path")) {
      throw new IllegalStateException(
          "MAC configured to use native maps, but native library path was not provided.");
    }

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
      miniDFS.set(new MiniDFSCluster.Builder(conf).build());
      if (oldTestBuildData == null) {
        System.clearProperty("test.build.data");
      } else {
        System.setProperty("test.build.data", oldTestBuildData);
      }
      miniDFS.get().waitClusterUp();
      InetSocketAddress dfsAddress = miniDFS.get().getNameNode().getNameNodeAddress();
      dfsUri = "hdfs://" + dfsAddress.getHostName() + ":" + dfsAddress.getPort();
      File coreFile = new File(config.getConfDir(), "core-site.xml");
      writeConfig(coreFile, Collections.singletonMap("fs.default.name", dfsUri).entrySet());
      File hdfsFile = new File(config.getConfDir(), "hdfs-site.xml");
      writeConfig(hdfsFile, conf);

      Map<String,String> siteConfig = config.getSiteConfig();
      siteConfig.put(Property.INSTANCE_VOLUMES.getKey(), dfsUri + "/accumulo");
      config.setSiteConfig(siteConfig);
    } else if (config.useExistingInstance()) {
      dfsUri = config.getHadoopConfiguration().get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY);
    } else {
      dfsUri = "file:///";
    }

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
    this.siteConfig = SiteConfiguration.fromFile(siteFile).build();
    this.context = Suppliers.memoize(() -> new ServerContext(siteConfig));

    if (!config.useExistingInstance() && !config.useExistingZooKeepers()) {
      zooCfgFile = new File(config.getConfDir(), "zoo.cfg");
      FileWriter fileWriter = new FileWriter(zooCfgFile, UTF_8);

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
    } else {
      zooCfgFile = null;
    }
    clusterControl = new MiniAccumuloClusterControl(this);
  }

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

    var basicArgs = Stream.of(javaBin, "-Dproc=" + clazz.getSimpleName());
    var jvmArgs = extraJvmOpts.stream();
    var propsArgs = config.getSystemProperties().entrySet().stream()
        .map(e -> String.format("-D%s=%s", e.getKey(), e.getValue()));

    // @formatter:off
    var hardcodedArgs = Stream.of(
        "-Dapple.awt.UIElement=true",
        "-Djava.net.preferIPv4Stack=true",
        "-XX:+PerfDisableSharedMem",
        "-XX:+AlwaysPreTouch",
        Main.class.getName(), clazz.getName());
    // @formatter:on

    // concatenate all the args sources into a single list of args
    var argList = Stream.of(basicArgs, jvmArgs, propsArgs, hardcodedArgs, Stream.of(args))
        .flatMap(Function.identity()).collect(toList());
    ProcessBuilder builder = new ProcessBuilder(argList);

    final String classpath = getClasspath();
    builder.environment().put("CLASSPATH", classpath);
    builder.environment().put("ACCUMULO_HOME", config.getDir().getAbsolutePath());
    builder.environment().put("ACCUMULO_LOG_DIR", config.getLogDir().getAbsolutePath());
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
        + "\n, args: " + argList + "\n, environment: " + builder.environment());

    int hashcode = builder.hashCode();

    File stdOut = new File(config.getLogDir(), clazz.getSimpleName() + "_" + hashcode + ".out");
    File stdErr = new File(config.getLogDir(), clazz.getSimpleName() + "_" + hashcode + ".err");

    Process process = builder.redirectError(stdErr).redirectOutput(stdOut).start();

    cleanup.add(process);

    return new ProcessInfo(process, stdOut);
  }

  public ProcessInfo _exec(KeywordExecutable server, ServerType serverType,
      Map<String,String> configOverrides, String... args) throws IOException {
    String[] modifiedArgs;
    if (args == null || args.length == 0) {
      modifiedArgs = new String[] {server.keyword()};
    } else {
      modifiedArgs =
          Stream.concat(Stream.of(server.keyword()), Stream.of(args)).toArray(String[]::new);
    }
    return _exec(Main.class, serverType, configOverrides, modifiedArgs);
  }

  public ProcessInfo _exec(Class<?> clazz, ServerType serverType,
      Map<String,String> configOverrides, String... args) throws IOException {
    List<String> jvmOpts = new ArrayList<>();
    if (serverType == ServerType.ZOOKEEPER) {
      // disable zookeeper's log4j 1.2 jmx support, which requires old versions of log4j 1.2
      // and won't work with reload4j or log4j2
      jvmOpts.add("-Dzookeeper.jmx.log4j.disable=true");
    }
    jvmOpts.add("-Xmx" + config.getMemory(serverType));
    if (configOverrides != null && !configOverrides.isEmpty()) {
      File siteFile =
          Files.createTempFile(config.getConfDir().toPath(), "accumulo", ".properties").toFile();
      Map<String,String> confMap = new HashMap<>(config.getSiteConfig());
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

  private static void mkdirs(File dir) {
    if (!dir.mkdirs()) {
      log.warn("Unable to create {}", dir);
    }
  }

  private void writeConfig(File file, Iterable<Map.Entry<String,String>> settings)
      throws IOException {
    FileWriter fileWriter = new FileWriter(file, UTF_8);
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
    FileWriter fileWriter = new FileWriter(file, UTF_8);

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
    if (config.useMiniDFS() && miniDFS.get() == null) {
      throw new IllegalStateException("Cannot restart mini when using miniDFS");
    }

    MiniAccumuloClusterControl control = getClusterControl();

    if (config.useExistingInstance()) {
      AccumuloConfiguration acuConf = config.getAccumuloConfiguration();
      Configuration hadoopConf = config.getHadoopConfiguration();
      ServerDirs serverDirs = new ServerDirs(acuConf, hadoopConf);

      Path instanceIdPath;
      try (var fs = getServerContext().getVolumeManager()) {
        instanceIdPath = serverDirs.getInstanceIdLocation(fs.getFirst());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      InstanceId instanceIdFromFile =
          VolumeManager.getInstanceIDFromHdfs(instanceIdPath, hadoopConf);
      ZooReaderWriter zrw = getServerContext().getZooReaderWriter();

      String rootPath = ZooUtil.getRoot(instanceIdFromFile);

      String instanceName = null;
      try {
        for (String name : zrw.getChildren(Constants.ZROOT + Constants.ZINSTANCES)) {
          String instanceNamePath = Constants.ZROOT + Constants.ZINSTANCES + "/" + name;
          byte[] bytes = zrw.getData(instanceNamePath);
          InstanceId iid = InstanceId.of(new String(bytes, UTF_8));
          if (iid.equals(instanceIdFromFile)) {
            instanceName = name;
          }
        }
      } catch (KeeperException e) {
        throw new IllegalStateException("Unable to read instance name from zookeeper.", e);
      }
      if (instanceName == null) {
        throw new IllegalStateException("Unable to read instance name from zookeeper.");
      }

      config.setInstanceName(instanceName);
      if (!AccumuloStatus.isAccumuloOffline(zrw, rootPath)) {
        throw new IllegalStateException(
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
        log.warn("Starting ZooKeeper");
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
            } catch (IOException | RuntimeException e) {
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

        log.warn("Initializing ZooKeeper");
        Process initProcess = exec(Initialize.class, args.toArray(new String[0])).getProcess();
        int ret = initProcess.waitFor();
        if (ret != 0) {
          throw new IllegalStateException("Initialize process returned " + ret
              + ". Check the logs in " + config.getLogDir() + " for errors.");
        }
        initialized = true;
      } else {
        log.warn("Not initializing ZooKeeper, already initialized");
      }
    }

    log.info("Starting MAC against instance {} and zookeeper(s) {}.", config.getInstanceName(),
        config.getZooKeepers());

    control.start(ServerType.TABLET_SERVER);

    int ret = 0;
    for (int i = 0; i < 5; i++) {
      ret = exec(Main.class, SetGoalState.class.getName(), ManagerGoalState.NORMAL.toString())
          .getProcess().waitFor();
      if (ret == 0) {
        break;
      }
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
    if (ret != 0) {
      throw new IllegalStateException("Could not set manager goal state, process returned " + ret
          + ". Check the logs in " + config.getLogDir() + " for errors.");
    }

    control.start(ServerType.MANAGER);
    control.start(ServerType.GARBAGE_COLLECTOR);

    if (executor == null) {
      executor = Executors.newSingleThreadExecutor();
    }

    verifyUp();

  }

  // wait up to 10 seconds for the process to start
  private static void waitForProcessStart(Process p, String name) throws InterruptedException {
    long start = System.nanoTime();
    while (p.info().startInstant().isEmpty()) {
      if (NANOSECONDS.toSeconds(System.nanoTime() - start) > 10) {
        throw new IllegalStateException(
            "Error starting " + name + " - instance not started within 10 seconds");
      }
      Thread.sleep(50);
    }
  }

  private void verifyUp() throws InterruptedException, IOException {

    int numTries = 10;

    requireNonNull(getClusterControl().managerProcess, "Error starting Manager - no process");
    waitForProcessStart(getClusterControl().managerProcess, "Manager");

    requireNonNull(getClusterControl().gcProcess, "Error starting GC - no process");
    waitForProcessStart(getClusterControl().gcProcess, "GC");

    int tsExpectedCount = 0;
    for (Process tsp : getClusterControl().tabletServerProcesses) {
      tsExpectedCount++;
      requireNonNull(tsp, "Error starting TabletServer " + tsExpectedCount + " - no process");
      waitForProcessStart(tsp, "TabletServer" + tsExpectedCount);
    }

    try (ZooKeeper zk = new ZooKeeper(getZooKeepers(), 60000, event -> log.warn("{}", event))) {

      String secret = getSiteConfiguration().get(Property.INSTANCE_SECRET);

      while (!(zk.getState() == States.CONNECTED)) {
        log.info("Waiting for ZK client to connect, state: {} - will retry", zk.getState());
        Thread.sleep(1000);
      }

      String instanceId = null;
      for (int i = 0; i < numTries; i++) {
        if (zk.getState() == States.CONNECTED) {
          ZooUtil.digestAuth(zk, secret);
          try {
            final AtomicInteger rc = new AtomicInteger();
            final CountDownLatch waiter = new CountDownLatch(1);
            zk.sync("/", (code, arg1, arg2) -> {
              rc.set(code);
              waiter.countDown();
            }, null);
            waiter.await();
            Code code = Code.get(rc.get());
            if (code != Code.OK) {
              throw KeeperException.create(code);
            }
            String instanceNamePath =
                Constants.ZROOT + Constants.ZINSTANCES + "/" + config.getInstanceName();
            byte[] bytes = zk.getData(instanceNamePath, null, null);
            instanceId = new String(bytes, UTF_8);
            break;
          } catch (KeeperException e) {
            log.warn("Error trying to read instance id from zookeeper: " + e.getMessage());
            log.debug("Unable to read instance id from zookeeper.", e);
          }
        } else {
          log.warn("ZK client not connected, state: {}", zk.getState());
        }
        Thread.sleep(1000);
      }

      if (instanceId == null) {
        for (int i = 0; i < numTries; i++) {
          if (zk.getState() == States.CONNECTED) {
            ZooUtil.digestAuth(zk, secret);
            try {
              log.warn("******* COULD NOT FIND INSTANCE ID - DUMPING ZK ************");
              log.warn("Connected to ZooKeeper: {}", getZooKeepers());
              log.warn("Looking for instanceId at {}",
                  Constants.ZROOT + Constants.ZINSTANCES + "/" + config.getInstanceName());
              ZKUtil.visitSubTreeDFS(zk, Constants.ZROOT, false,
                  (rc, path, ctx, name) -> log.warn("{}", path));
              log.warn("******* END ZK DUMP ************");
            } catch (KeeperException | InterruptedException e) {
              log.error("Error dumping zk", e);
            }
          }
          Thread.sleep(1000);
        }
        throw new IllegalStateException("Unable to find instance id from zookeeper.");
      }

      String rootPath = Constants.ZROOT + "/" + instanceId;
      int tsActualCount = 0;
      try {
        while (tsActualCount < tsExpectedCount) {
          tsActualCount = 0;
          for (String child : zk.getChildren(rootPath + Constants.ZTSERVERS, null)) {
            if (zk.getChildren(rootPath + Constants.ZTSERVERS + "/" + child, null).isEmpty()) {
              log.info("TServer " + tsActualCount + " not yet present in ZooKeeper");
            } else {
              tsActualCount++;
              log.info("TServer " + tsActualCount + " present in ZooKeeper");
            }
          }
          Thread.sleep(500);
        }
      } catch (KeeperException e) {
        throw new IllegalStateException("Unable to read TServer information from zookeeper.", e);
      }

      try {
        while (zk.getChildren(rootPath + Constants.ZMANAGER_LOCK, null).isEmpty()) {
          log.info("Manager not yet present in ZooKeeper");
          Thread.sleep(500);
        }
      } catch (KeeperException e) {
        throw new IllegalStateException("Unable to read Manager information from zookeeper.", e);
      }

      try {
        while (zk.getChildren(rootPath + Constants.ZGC_LOCK, null).isEmpty()) {
          log.info("GC not yet present in ZooKeeper");
          Thread.sleep(500);
        }
      } catch (KeeperException e) {
        throw new IllegalStateException("Unable to read GC information from zookeeper.", e);
      }

    }
  }

  private List<String> buildRemoteDebugParams(int port) {
    return Collections.singletonList(
        String.format("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=%d", port));
  }

  /**
   * @return generated remote debug ports if in debug mode.
   * @since 1.6.0
   */
  public Set<Pair<ServerType,Integer>> getDebugPorts() {
    return debugPorts;
  }

  List<ProcessReference> references(Process... procs) {
    return Stream.of(procs).map(ProcessReference::new).collect(toList());
  }

  public Map<ServerType,Collection<ProcessReference>> getProcesses() {
    Map<ServerType,Collection<ProcessReference>> result = new HashMap<>();
    MiniAccumuloClusterControl control = getClusterControl();
    result.put(ServerType.MANAGER, references(control.managerProcess));
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
  public ServerContext getServerContext() {
    return context.get();
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
    control.stop(ServerType.MANAGER, null);
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

    var miniDFSActual = miniDFS.get();
    if (config.useMiniDFS() && miniDFSActual != null) {
      miniDFSActual.shutdown();
    }
    for (Process p : cleanup) {
      p.destroy();
      p.waitFor();
    }
    miniDFS.set(null);
  }

  /**
   * @since 1.6.0
   */
  public MiniAccumuloConfigImpl getConfig() {
    return config;
  }

  @Override
  public AccumuloClient createAccumuloClient(String user, AuthenticationToken token) {
    return Accumulo.newClient().from(clientProperties.get()).as(user, token).build();
  }

  @Override
  public Properties getClientProperties() {
    // return a copy, without re-reading the file
    var copy = new Properties();
    copy.putAll(clientProperties.get());
    return copy;
  }

  @Override
  public FileSystem getFileSystem() {
    try {
      return FileSystem.get(new URI(dfsUri), new Configuration());
    } catch (IOException | URISyntaxException e) {
      throw new IllegalStateException(e);
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

  public int stopProcessWithTimeout(final Process proc, long timeout, TimeUnit unit)
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
  public ManagerMonitorInfo getManagerMonitorInfo()
      throws AccumuloException, AccumuloSecurityException {
    try (AccumuloClient c = Accumulo.newClient().from(clientProperties.get()).build()) {
      ClientContext context = (ClientContext) c;
      return ThriftClientTypes.MANAGER.execute(context,
          client -> client.getManagerStats(TraceUtil.traceInfo(), context.rpcCreds()));
    }
  }

  public MiniDFSCluster getMiniDfs() {
    return this.miniDFS.get();
  }

  @Override
  public MiniAccumuloClusterControl getClusterControl() {
    return clusterControl;
  }

  @Override
  public Path getTemporaryPath() {
    String p;
    if (config.useMiniDFS()) {
      p = "/tmp/";
    } else {
      File tmp = new File(config.getDir(), "tmp");
      mkdirs(tmp);
      p = tmp.toString();
    }
    return getFileSystem().makeQualified(new Path(p));
  }

  @Override
  public AccumuloConfiguration getSiteConfiguration() {
    return new ConfigurationCopy(Stream.concat(DefaultConfiguration.getInstance().stream(),
        config.getSiteConfig().entrySet().stream()));
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

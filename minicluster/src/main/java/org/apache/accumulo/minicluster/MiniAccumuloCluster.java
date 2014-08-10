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
package org.apache.accumulo.minicluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.gc.SimpleGarbageCollector;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.tabletserver.TabletServer;
import org.apache.accumulo.server.util.Initialize;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.start.Main;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ZooKeeperServerMain;

/**
 * A utility class that will create Zookeeper and Accumulo processes that write all of their data to a single local directory. This class makes it easy to test
 * code against a real Accumulo instance. Its much more accurate for testing than MockAccumulo, but much slower than MockAccumulo.
 * 
 * @since 1.5.0
 */
public class MiniAccumuloCluster {
  private static final Logger log = Logger.getLogger(MiniAccumuloCluster.class);
  
  private static final String INSTANCE_SECRET = "DONTTELL";
  private static final String INSTANCE_NAME = "miniInstance";
  
  private static class LogWriter extends Thread {
    private BufferedReader in;
    private BufferedWriter out;
    
    public LogWriter(InputStream stream, File logFile) throws IOException {
      this.setDaemon(true);
      this.in = new BufferedReader(new InputStreamReader(stream, Constants.UTF8));
      out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile), Constants.UTF8));
      
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
  
  private File libDir;
  private File libExtDir;
  private File confDir;
  private File zooKeeperDir;
  private File accumuloDir;
  private File zooCfgFile;
  private File logDir;
  private File walogDir;
  
  private Process zooKeeperProcess;
  private Process masterProcess;
  private Process gcProcess;
  
  private int zooKeeperPort;
  
  private List<LogWriter> logWriters = new ArrayList<MiniAccumuloCluster.LogWriter>();
  
  private MiniAccumuloConfig config;
  private Process[] tabletServerProcesses;
  
  // Non-final for testing purposes
  private ExecutorService executor;
  
  private Process exec(Class<? extends Object> clazz, String... args) throws IOException {
    String javaHome = System.getProperty("java.home");
    String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    
    classpath = confDir.getAbsolutePath() + File.pathSeparator + classpath;
    
    String className = clazz.getCanonicalName();
    
    ArrayList<String> argList = new ArrayList<String>();
    
    argList.addAll(Arrays.asList(javaBin, "-cp", classpath, "-Xmx128m", "-XX:+UseConcMarkSweepGC", "-XX:CMSInitiatingOccupancyFraction=75",
        "-Dapple.awt.UIElement=true", Main.class.getName(), className));
    
    argList.addAll(Arrays.asList(args));
    
    ProcessBuilder builder = new ProcessBuilder(argList);
    
    builder.environment().put("ACCUMULO_HOME", config.getDir().getAbsolutePath());
    builder.environment().put("ACCUMULO_LOG_DIR", logDir.getAbsolutePath());
    
    // if we're running under accumulo.start, we forward these env vars
    String env = System.getenv("HADOOP_PREFIX");
    if (env != null)
      builder.environment().put("HADOOP_PREFIX", env);
    env = System.getenv("ZOOKEEPER_HOME");
    if (env != null)
      builder.environment().put("ZOOKEEPER_HOME", env);
    
    Process process = builder.start();
    
    LogWriter lw;
    lw = new LogWriter(process.getErrorStream(), new File(logDir, clazz.getSimpleName() + "_" + process.hashCode() + ".err"));
    logWriters.add(lw);
    lw.start();
    lw = new LogWriter(process.getInputStream(), new File(logDir, clazz.getSimpleName() + "_" + process.hashCode() + ".out"));
    logWriters.add(lw);
    lw.start();
    
    return process;
  }
  
  private void appendProp(Writer fileWriter, Property key, String value, Map<String,String> siteConfig) throws IOException {
    appendProp(fileWriter, key.getKey(), value, siteConfig);
  }
  
  private void appendProp(Writer fileWriter, String key, String value, Map<String,String> siteConfig) throws IOException {
    if (!siteConfig.containsKey(key))
      fileWriter.append("<property><name>" + key + "</name><value>" + value + "</value></property>\n");
  }
  
  /**
   * Sets a given key with a random port for the value on the site config if it doesn't already exist.
   */
  private void mergePropWithRandomPort(Map<String,String> siteConfig, String key) {
    if (!siteConfig.containsKey(key)) {
      siteConfig.put(key, "0");
    }
  }
  
  /**
   * 
   * @param dir
   *          An empty or nonexistant temp directoy that Accumulo and Zookeeper can store data in. Creating the directory is left to the user. Java 7, Guava,
   *          and Junit provide methods for creating temporary directories.
   * @param rootPassword
   *          Initial root password for instance.
   */
  public MiniAccumuloCluster(File dir, String rootPassword) throws IOException {
    this(new MiniAccumuloConfig(dir, rootPassword));
  }
  
  /**
   * @param config
   *          initial configuration
   */
  
  public MiniAccumuloCluster(MiniAccumuloConfig config) throws IOException {
    
    if (config.getDir().exists() && !config.getDir().isDirectory())
      throw new IllegalArgumentException("Must pass in directory, " + config.getDir() + " is a file");
    
    if (config.getDir().exists() && config.getDir().list().length != 0)
      throw new IllegalArgumentException("Directory " + config.getDir() + " is not empty");
    
    this.config = config;
    
    libDir = new File(config.getDir(), "lib");
    libExtDir = new File(libDir, "ext");
    confDir = new File(config.getDir(), "conf");
    accumuloDir = new File(config.getDir(), "accumulo");
    zooKeeperDir = new File(config.getDir(), "zookeeper");
    logDir = new File(config.getDir(), "logs");
    walogDir = new File(config.getDir(), "walogs");
    
    confDir.mkdirs();
    accumuloDir.mkdirs();
    zooKeeperDir.mkdirs();
    logDir.mkdirs();
    walogDir.mkdirs();
    libDir.mkdirs();
    
    // Avoid the classloader yelling that the general.dynamic.classpaths value is invalid because
    // $ACCUMULO_HOME/lib/ext isn't defined.
    libExtDir.mkdirs();
    
    zooKeeperPort = PortUtils.getRandomFreePort();
    
    File siteFile = new File(confDir, "accumulo-site.xml");
    
    OutputStreamWriter fileWriter = new OutputStreamWriter(new FileOutputStream(siteFile), Constants.UTF8);
    fileWriter.append("<configuration>\n");
    
    HashMap<String,String> siteConfig = new HashMap<String,String>(config.getSiteConfig());
    
    appendProp(fileWriter, Property.INSTANCE_DFS_URI, "file:///", siteConfig);
    appendProp(fileWriter, Property.INSTANCE_DFS_DIR, accumuloDir.getAbsolutePath(), siteConfig);
    appendProp(fileWriter, Property.INSTANCE_ZK_HOST, "localhost:" + zooKeeperPort, siteConfig);
    appendProp(fileWriter, Property.INSTANCE_SECRET, INSTANCE_SECRET, siteConfig);
    appendProp(fileWriter, Property.TSERV_PORTSEARCH, "true", siteConfig);
    appendProp(fileWriter, Property.LOGGER_DIR, walogDir.getAbsolutePath(), siteConfig);
    appendProp(fileWriter, Property.TSERV_DATACACHE_SIZE, "10M", siteConfig);
    appendProp(fileWriter, Property.TSERV_INDEXCACHE_SIZE, "10M", siteConfig);
    appendProp(fileWriter, Property.TSERV_MAXMEM, "50M", siteConfig);
    appendProp(fileWriter, Property.TSERV_WALOG_MAX_SIZE, "100M", siteConfig);
    appendProp(fileWriter, Property.TSERV_NATIVEMAP_ENABLED, "false", siteConfig);
    appendProp(fileWriter, Property.TRACE_TOKEN_PROPERTY_PREFIX + ".password", config.getRootPassword(), siteConfig);
    appendProp(fileWriter, Property.GC_CYCLE_DELAY, "4s", siteConfig);
    appendProp(fileWriter, Property.GC_CYCLE_START, "0s", siteConfig);
    mergePropWithRandomPort(siteConfig, Property.MASTER_CLIENTPORT.getKey());
    mergePropWithRandomPort(siteConfig, Property.TRACE_PORT.getKey());
    mergePropWithRandomPort(siteConfig, Property.TSERV_CLIENTPORT.getKey());
    mergePropWithRandomPort(siteConfig, Property.MONITOR_PORT.getKey());
    mergePropWithRandomPort(siteConfig, Property.GC_PORT.getKey());
    
    // since there is a small amount of memory, check more frequently for majc... setting may not be needed in 1.5
    appendProp(fileWriter, Property.TSERV_MAJC_DELAY, "3", siteConfig);
    
    // ACCUMULO-1472 -- Use the classpath, not what might be installed on the system.
    // We have to set *something* here, otherwise the AccumuloClassLoader will default to pulling from
    // environment variables (e.g. ACCUMULO_HOME, HADOOP_HOME/PREFIX) which will result in multiple copies
    // of artifacts on the classpath as they'll be provided by the invoking application
    appendProp(fileWriter, Property.GENERAL_CLASSPATHS, libDir.getAbsolutePath() + "/[^.].*.jar", siteConfig);
    appendProp(fileWriter, Property.GENERAL_DYNAMIC_CLASSPATHS, libExtDir.getAbsolutePath() + "/[^.].*.jar", siteConfig);
    
    for (Entry<String,String> entry : siteConfig.entrySet())
      fileWriter.append("<property><name>" + entry.getKey() + "</name><value>" + entry.getValue() + "</value></property>\n");
    fileWriter.append("</configuration>\n");
    fileWriter.close();
    
    zooCfgFile = new File(confDir, "zoo.cfg");
    fileWriter = new OutputStreamWriter(new FileOutputStream(zooCfgFile), Constants.UTF8);
    
    // zookeeper uses Properties to read its config, so use that to write in order to properly escape things like Windows paths
    Properties zooCfg = new Properties();
    zooCfg.setProperty("tickTime", "1000");
    zooCfg.setProperty("initLimit", "10");
    zooCfg.setProperty("syncLimit", "5");
    zooCfg.setProperty("clientPort", zooKeeperPort + "");
    zooCfg.setProperty("maxClientCnxns", "100");
    zooCfg.setProperty("dataDir", zooKeeperDir.getAbsolutePath());
    zooCfg.store(fileWriter, null);
    
    fileWriter.close();
  }
  
  /**
   * Starts Accumulo and Zookeeper processes. Can only be called once.
   * 
   * @throws IllegalStateException
   *           if already started
   */
  public void start() throws IOException, InterruptedException {
    if (zooKeeperProcess != null)
      throw new IllegalStateException("Already started");
    
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          MiniAccumuloCluster.this.stop();
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    
    zooKeeperProcess = exec(Main.class, ZooKeeperServerMain.class.getName(), zooCfgFile.getAbsolutePath());
    
    // sleep a little bit to let zookeeper come up before calling init, seems to work better
    UtilWaitThread.sleep(250);
    
    Process initProcess = exec(Initialize.class, "--instance-name", INSTANCE_NAME, "--password", config.getRootPassword());
    int ret = initProcess.waitFor();
    if (ret != 0) {
      throw new RuntimeException("Initialize process returned " + ret + ". Check the logs in " + logDir + " for errors.");
    }
    
    tabletServerProcesses = new Process[config.getNumTservers()];
    for (int i = 0; i < config.getNumTservers(); i++) {
      tabletServerProcesses[i] = exec(TabletServer.class);
    }
    
    masterProcess = exec(Master.class);
    
    gcProcess = exec(SimpleGarbageCollector.class);
    
    if (null == executor) {
      executor = Executors.newSingleThreadExecutor();
    }
  }
  
  /**
   * @return Accumulo instance name
   */
  
  public String getInstanceName() {
    return INSTANCE_NAME;
  }
  
  /**
   * @return zookeeper connection string
   */
  
  public String getZooKeepers() {
    return "localhost:" + zooKeeperPort;
  }
  
  /**
   * Stops Accumulo and Zookeeper processes. If stop is not called, there is a shutdown hook that is setup to kill the processes. Howerver its probably best to
   * call stop in a finally block as soon as possible.
   */
  public void stop() throws IOException, InterruptedException {
    if (zooKeeperProcess != null) {
      try {
        stopProcessWithTimeout(zooKeeperProcess, 30, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        log.warn("ZooKeeper did not fully stop after 30 seconds", e);
      } catch (TimeoutException e) {
        log.warn("ZooKeeper did not fully stop after 30 seconds", e);
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
    
    for (LogWriter lw : logWriters)
      lw.flush();
    
    if (gcProcess != null) {
      try {
        stopProcessWithTimeout(gcProcess, 30, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        log.warn("GarbageCollector did not fully stop after 30 seconds", e);
      } catch (TimeoutException e) {
        log.warn("GarbageCollector did not fully stop after 30 seconds", e);
      }
    }
    
    // ACCUMULO-2985 stop the ExecutorService after we finished using it to stop accumulo procs
    if (null != executor) {
      List<Runnable> tasksRemaining = executor.shutdownNow();
      
      // the single thread executor shouldn't have any pending tasks, but check anyways
      if (!tasksRemaining.isEmpty()) {
        log.warn("Unexpectedly had " + tasksRemaining.size() + " task(s) remaining in threadpool for execution when being stopped");
      }
      
      executor = null;
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

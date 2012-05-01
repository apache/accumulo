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
package org.apache.accumulo.server.logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.cloudtrace.instrument.thrift.TraceWrap;
import org.apache.accumulo.cloudtrace.thrift.TInfo;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.LogCopyInfo;
import org.apache.accumulo.core.tabletserver.thrift.LogFile;
import org.apache.accumulo.core.tabletserver.thrift.LoggerClosedException;
import org.apache.accumulo.core.tabletserver.thrift.MutationLogger;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchLogIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletMutations;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.logger.LogWriter.LogWriteException;
import org.apache.accumulo.server.security.Authenticator;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.ZKAuthenticator;
import org.apache.accumulo.server.trace.TraceFileSystem;
import org.apache.accumulo.server.util.FileSystemMonitor;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.TServerUtils;
import org.apache.accumulo.server.util.TServerUtils.ServerPort;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * A Mutation logging service.
 * 
 * This class will register the logging service in ZooKeeper, log updates from tservers, and provided logs to HDFS for recovery. Wraps the LogWriter, but
 * provides configuration, authentication and ZooKeeper registration. This makes the LogWriter easier to test. The service will stop if it loses its ephemeral
 * registration in ZooKeeper.
 */
public class LogService implements MutationLogger.Iface, Watcher {
  static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(LogService.class);
  
  private final Instance instance;
  private final Authenticator authenticator;
  private final TServer service;
  private final LogWriter writer_;
  private final MutationLogger.Iface writer;
  private ShutdownState shutdownState = ShutdownState.STARTED;
  private final List<FileLock> fileLocks = new ArrayList<FileLock>();
  private final String addressString;
  private String ephemeralNode;
  
  enum ShutdownState {
    STARTED, REGISTERED, WAITING_FOR_HALT, HALT
  };
  
  synchronized void switchState(ShutdownState state) {
    LOG.info("Switching from " + shutdownState + " to " + state);
    shutdownState = state;
  }
  
  synchronized private void closedCheck() throws LoggerClosedException {
    if (!shutdownState.equals(ShutdownState.REGISTERED))
      throw new LoggerClosedException();
  }
  
  public static void main(String[] args) throws Exception {
    SecurityUtil.serverLogin();

    LogService logService;
    Instance instance = HdfsZooInstance.getInstance();
    ServerConfiguration conf = new ServerConfiguration(instance);
    FileSystem fs = FileUtil.getFileSystem(CachedConfiguration.getInstance(), conf.getConfiguration());
    Accumulo.init(fs, conf, "logger");
    String hostname = Accumulo.getLocalAddress(args);
    try {
      logService = new LogService(conf, fs, hostname);
    } catch (Exception e) {
      LOG.fatal("Failed to initialize log service args=" + Arrays.asList(args), e);
      throw e;
    }
    Accumulo.enableTracing(hostname, "logger");
    try {
      logService.run();
    } catch (Exception ex) {
      LOG.error("Unexpected exception, exiting.", ex);
    }
  }
  
  public LogService(ServerConfiguration config, FileSystem fs, String hostname) throws UnknownHostException, KeeperException, InterruptedException, IOException {
    this.instance = config.getInstance();
    AccumuloConfiguration acuConf = config.getConfiguration();
    FileSystemMonitor.start(acuConf, Property.LOGGER_MONITOR_FS);
    
    fs = TraceFileSystem.wrap(fs);
    final Set<String> rootDirs = new HashSet<String>();
    for (String root : acuConf.get(Property.LOGGER_DIR).split(",")) {
      if (!root.startsWith("/"))
        root = System.getenv("ACCUMULO_HOME") + "/" + root;
      else if (root.equals(""))
        root = System.getProperty("org.apache.accumulo.core.dir.log");
      else if (root == null || root.isEmpty()) {
        String msg = "Write-ahead log directory not set!";
        LOG.fatal(msg);
        throw new RuntimeException(msg);
      }
      rootDirs.add(root);
    }
    
    for (String root : rootDirs) {
      File rootFile = new File(root);
      rootFile.mkdirs();
      FileOutputStream lockOutputStream = new FileOutputStream(root + "/.lock");
      FileLock fileLock = lockOutputStream.getChannel().tryLock();
      if (fileLock == null)
        throw new IOException("Failed to acquire lock file");
      fileLocks.add(fileLock);
      
      try {
        File test = new File(root, "test_writable");
        if (!test.mkdir())
          throw new RuntimeException("Unable to write to write-ahead log directory " + root);
        test.delete();
      } catch (Throwable t) {
        LOG.fatal("Unable to write to write-ahead log directory", t);
        throw new RuntimeException(t);
      }
      LOG.info("Storing recovery logs at " + root);
    }
    
    authenticator = ZKAuthenticator.getInstance();
    int poolSize = acuConf.getCount(Property.LOGGER_COPY_THREADPOOL_SIZE);
    boolean archive = acuConf.getBoolean(Property.LOGGER_ARCHIVE);
    writer_ = new LogWriter(acuConf, fs, rootDirs, instance.getInstanceID(), poolSize, archive);
    
    // call before putting this service online
    removeIncompleteCopies(acuConf, fs, rootDirs);

    InvocationHandler h = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Throwable error;
        try {
          return method.invoke(writer_, args);
        } catch (InvocationTargetException ex) {
          if (ex.getCause() instanceof TException) {
            throw ex.getCause();
          }
          if (ex.getCause() instanceof TBase) {
            throw ex.getCause();
          }
          if (ex.getCause() instanceof LogWriteException) {
            for (String root : rootDirs) {
              final File rootFile = new File(root);
              if ((rootFile.getUsableSpace() / (float) rootFile.getTotalSpace()) < 0.95) {
                LOG.fatal("Logger appears to be running out of space, quitting.");
                service.stop();
              }
            }
          }
          error = ex;
        } catch (Throwable t) {
          error = t;
        }
        LOG.error("Error invoking log writer: ", error);
        throw error;
      }
    };
    writer = (MutationLogger.Iface) Proxy.newProxyInstance(MutationLogger.Iface.class.getClassLoader(), new Class[] {MutationLogger.Iface.class}, h);
    // Create the thrift-based logging service
    MutationLogger.Processor processor = new MutationLogger.Processor(TraceWrap.service(this));
    ServerPort sp = TServerUtils.startServer(acuConf, Property.LOGGER_PORT, processor, this.getClass().getSimpleName(), "Logger Client Service Handler",
        Property.LOGGER_PORTSEARCH, Property.LOGGER_MINTHREADS, Property.LOGGER_THREADCHECK);
    service = sp.server;
    InetSocketAddress address = new InetSocketAddress(hostname, sp.port);
    addressString = AddressUtil.toString(address);
    registerInZooKeeper(Constants.ZLOGGERS);
    this.switchState(ShutdownState.REGISTERED);
  }
  
  /**
   * @param acuConf
   * @param fs
   * @param rootDirs
   * @throws IOException
   */
  private void removeIncompleteCopies(AccumuloConfiguration acuConf, FileSystem fs, Set<String> rootDirs) throws IOException {

    Set<String> walogs = new HashSet<String>();
    for (String root : rootDirs) {
      File rootFile = new File(root);
      for (File walog : rootFile.listFiles()) {
        try {
          UUID.fromString(walog.getName());
          walogs.add(walog.getName());
        } catch (IllegalArgumentException iea) {
          LOG.debug("Ignoring " + walog.getName());
        }
      }
    }
    
    // look for .recovered that are not finished
    for (String walog : walogs) {
      Path path = new Path(ServerConstants.getRecoveryDir() + "/" + walog + ".recovered");
      if (fs.exists(path) && !fs.exists(new Path(path, "finished"))) {
        LOG.debug("Incomplete copy/sort in dfs, deleting " + path);
        fs.delete(path, true);
      }
    }
  }

  public void run() {
    try {
      while (!service.isServing()) {
        UtilWaitThread.sleep(500);
      }
      while (service.isServing()) {
        UtilWaitThread.sleep(500);
      }
      LOG.info("Logger shutting down");
      writer_.shutdown();
    } catch (Throwable t) {
      LOG.fatal("Error in LogService", t);
      throw new RuntimeException(t);
    }
    Runtime.getRuntime().halt(0);
  }
  
  void registerInZooKeeper(String zooDir) {
    try {
      IZooReaderWriter zoo = ZooReaderWriter.getInstance();
      String path = ZooUtil.getRoot(instance) + zooDir;
      path += "/logger-";
      path = zoo.putEphemeralSequential(path, addressString.getBytes());
      ephemeralNode = path;
      zoo.exists(path, this);
    } catch (Exception ex) {
      throw new RuntimeException("Unexpected error creating zookeeper entry " + zooDir);
    }
  }
  
  private void checkForSystemPrivs(String request, AuthInfo credentials) throws ThriftSecurityException {
    try {
      if (!authenticator.hasSystemPermission(credentials, credentials.user, SystemPermission.SYSTEM)) {
        LOG.warn("Got " + request + " from user: " + credentials.user);
        throw new ThriftSecurityException(credentials.user, SecurityErrorCode.PERMISSION_DENIED);
      }
    } catch (AccumuloSecurityException e) {
      LOG.warn("Got " + request + " from unauthenticatable user: " + e.getUser());
      throw e.asThriftException();
    }
  }
  
  @Override
  public void close(TInfo info, long id) throws NoSuchLogIDException, LoggerClosedException, TException {
    closedCheck();
    writer.close(info, id);
  }
  
  @Override
  public LogCopyInfo startCopy(TInfo info, AuthInfo credentials, String localLog, String fullyQualifiedFileName, boolean sort) throws ThriftSecurityException,
      TException {
    checkForSystemPrivs("copy", credentials);
    LogCopyInfo lci = writer.startCopy(null, credentials, localLog, fullyQualifiedFileName, sort);
    lci.loggerZNode = ephemeralNode;
    return lci;
  }
  
  @Override
  public LogFile create(TInfo info, AuthInfo credentials, String tserverSession) throws ThriftSecurityException, LoggerClosedException, TException {
    checkForSystemPrivs("create", credentials);
    closedCheck();
    return writer.create(info, credentials, tserverSession);
  }
  
  @Override
  public void log(TInfo info, long id, long seq, int tid, TMutation mutation) throws NoSuchLogIDException, LoggerClosedException, TException {
    closedCheck();
    writer.log(info, id, seq, tid, mutation);
  }
  
  @Override
  public void logManyTablets(TInfo info, long id, List<TabletMutations> mutations) throws NoSuchLogIDException, LoggerClosedException, TException {
    closedCheck();
    writer.logManyTablets(info, id, mutations);
  }
  
  @Override
  public void minorCompactionFinished(TInfo info, long id, long seq, int tid, String fqfn) throws NoSuchLogIDException, LoggerClosedException, TException {
    closedCheck();
    writer.minorCompactionFinished(info, id, seq, tid, fqfn);
  }
  
  @Override
  public void minorCompactionStarted(TInfo info, long id, long seq, int tid, String fqfn) throws NoSuchLogIDException, LoggerClosedException, TException {
    closedCheck();
    writer.minorCompactionStarted(info, id, seq, tid, fqfn);
  }
  
  @Override
  public void defineTablet(TInfo info, long id, long seq, int tid, TKeyExtent tablet) throws NoSuchLogIDException, LoggerClosedException, TException {
    closedCheck();
    writer.defineTablet(info, id, seq, tid, tablet);
  }
  
  @Override
  public void process(WatchedEvent event) {
    LOG.debug("event " + event.getPath() + " " + event.getType() + " " + event.getState());
    
    if (event.getState() == KeeperState.Expired) {
      LOG.warn("Logger lost zookeeper registration at " + event.getPath());
      service.stop();
    } else if (event.getType() == EventType.NodeDeleted) {
      LOG.info("Logger zookeeper entry lost " + event.getPath());
      String[] path = event.getPath().split("/");
      if (path[path.length - 1].equals(Constants.ZLOGGERS) && this.shutdownState == ShutdownState.REGISTERED) {
        LOG.fatal("Stopping server, zookeeper entry lost " + event.getPath());
        service.stop();
      }
    }
  }
  
  @Override
  public List<String> getClosedLogs(TInfo info, AuthInfo credentials) throws ThriftSecurityException, TException {
    checkForSystemPrivs("getClosedLogs", credentials);
    return writer.getClosedLogs(info, credentials);
  }
  
  @Override
  public void remove(TInfo info, AuthInfo credentials, List<String> files) throws TException {
    try {
      checkForSystemPrivs("remove", credentials);
      writer.remove(info, credentials, files);
    } catch (ThriftSecurityException ex) {
      LOG.error(ex, ex);
    }
  }
  
  @Override
  public void beginShutdown(TInfo tinfo, AuthInfo credentials) throws TException {
    try {
      checkForSystemPrivs("beginShutdown", credentials);
      writer.beginShutdown(tinfo, credentials);
      switchState(ShutdownState.WAITING_FOR_HALT);
    } catch (ThriftSecurityException ex) {
      LOG.error(ex, ex);
    }
  }
  
  @Override
  public void halt(TInfo tinfo, AuthInfo credentials) throws TException {
    try {
      checkForSystemPrivs("halt", credentials);
      Halt.halt(0, new Runnable() {
        @Override
        public void run() {
          LOG.info("Halting by request");
        }
      });
    } catch (ThriftSecurityException ex) {
      LOG.error(ex, ex);
    }
  }
}

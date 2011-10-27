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
import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.LogFile;
import org.apache.accumulo.core.tabletserver.thrift.MutationLogger;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchLogIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletMutations;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.logger.LogWriter.LogWriteException;
import org.apache.accumulo.server.security.Authenticator;
import org.apache.accumulo.server.security.ZKAuthenticator;
import org.apache.accumulo.server.util.FileSystemMonitor;
import org.apache.accumulo.server.util.TServerUtils;
import org.apache.accumulo.server.util.TServerUtils.ServerPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import cloudtrace.instrument.thrift.TraceWrap;
import cloudtrace.thrift.TInfo;

/**
 * A Mutation logging service.
 * 
 * This class will register the logging service in ZooKeeper, log updates from tservers, and provided logs to HDFS for recovery. Wraps the LogWriter, but
 * provides configuration, authentication and ZooKeeper registration. This makes the LogWriter easier to test. The service will stop if it loses its ephemeral
 * registration in ZooKeeper.
 */
public class LogService implements MutationLogger.Iface, Watcher {
  static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(LogService.class);
  
  private Configuration conf;
  private Authenticator authenticator;
  private ZooKeeper zooKeeper;
  private TServer service;
  private LogWriter writer_;
  private MutationLogger.Iface writer;
  
  private FileLock fileLock;
  
  public static void main(String[] args) throws Exception {
    LogService logService;
    
    try {
      logService = new LogService(args);
    } catch (Exception e) {
      LOG.fatal("Failed to initialize log service args=" + Arrays.asList(args), e);
      throw e;
    }
    
    logService.run();
  }
  
  public LogService(String[] args) throws UnknownHostException, KeeperException, InterruptedException, IOException {
    FileSystem fs = null;
    try {
      Accumulo.init("logger");
    } catch (UnknownHostException e1) {
      LOG.error("Error reading logging configuration");
    }
    
    FileSystemMonitor.start(Property.LOGGER_MONITOR_FS);
    
    conf = CachedConfiguration.getInstance();
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      String msg = "Exception connecting to FileSystem";
      LOG.error(msg, e);
      throw new RuntimeException(msg);
    }
    String root = AccumuloConfiguration.getSystemConfiguration().get(Property.LOGGER_DIR);
    if (root.equals(""))
      root = System.getProperty("org.apache.accumulo.core.dir.log");
    if (root == null || root.isEmpty()) {
      String msg = "Write-ahead log directory not set!";
      LOG.fatal(msg);
      throw new RuntimeException(msg);
    }
    
    FileOutputStream lockOutputStream = new FileOutputStream(root + "/.lock");
    fileLock = lockOutputStream.getChannel().tryLock();
    if (fileLock == null)
      throw new IOException("Failed to acquire lock file");
    
    try {
      File test = new File(root, "test_writable");
      if (!test.mkdir())
        throw new RuntimeException("Unable to write to write-ahead log directory " + root);
      test.delete();
    } catch (Throwable t) {
      LOG.fatal("Unable to write to write-ahead log directory", t);
      throw new RuntimeException(t);
    }
    authenticator = ZKAuthenticator.getInstance();
    LOG.info("Storing recovery logs at " + root);
    
    final File rootFile = new File(root);
    writer_ = new LogWriter(conf, fs, root, HdfsZooInstance.getInstance().getInstanceID());
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
          if (ex.getCause() instanceof LogWriteException) {
            if ((rootFile.getUsableSpace() / (float) rootFile.getTotalSpace()) < 0.95) {
              LOG.fatal("Logger appears to be running out of space, quitting.");
              service.stop();
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
    boolean search = AccumuloConfiguration.getSystemConfiguration().getBoolean(Property.LOGGER_PORTSEARCH);
    ServerPort sp = TServerUtils.startServer(Property.LOGGER_PORT, processor, this.getClass().getSimpleName(), "Logger Client Service Handler", search);
    service = sp.server;
    InetSocketAddress address = new InetSocketAddress(Accumulo.getLocalAddress(args), sp.port);
    String addressString = AddressUtil.toString(address);
    registerInZooKeeper(addressString);
    Accumulo.enableTracing(address.getHostName(), "logger");
  }
  
  public void run() {
    try {
      service.serve();
      LOG.info("Logger shutting down");
      writer_.shutdown();
    } catch (Throwable t) {
      LOG.fatal("Error in LogService", t);
      throw new RuntimeException(t);
    }
    Runtime.getRuntime().halt(0);
  }
  
  void registerInZooKeeper(String name) throws KeeperException, InterruptedException {
    zooKeeper = ZooSession.getSession(this);
    String path = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZLOGGERS;
    path += "/logger-";
    path = zooKeeper.create(path, name.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    zooKeeper.exists(path, true);
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
  public void close(TInfo info, long id) throws NoSuchLogIDException, TException {
    writer.close(info, id);
  }
  
  @Override
  public long startCopy(TInfo info, AuthInfo credentials, String localLog, String fullyQualifiedFileName, boolean sort) throws ThriftSecurityException,
      TException {
    checkForSystemPrivs("copy", credentials);
    return writer.startCopy(null, credentials, localLog, fullyQualifiedFileName, sort);
  }
  
  @Override
  public LogFile create(TInfo info, AuthInfo credentials, String tserverSession) throws ThriftSecurityException, TException {
    checkForSystemPrivs("create", credentials);
    return writer.create(info, credentials, tserverSession);
  }
  
  @Override
  public void log(TInfo info, long id, long seq, int tid, TMutation mutation) throws NoSuchLogIDException, TException {
    writer.log(info, id, seq, tid, mutation);
  }
  
  @Override
  public void logManyTablets(TInfo info, long id, List<TabletMutations> mutations) throws NoSuchLogIDException, TException {
    writer.logManyTablets(info, id, mutations);
  }
  
  @Override
  public void minorCompactionFinished(TInfo info, long id, long seq, int tid, String fqfn) throws NoSuchLogIDException, TException {
    writer.minorCompactionFinished(info, id, seq, tid, fqfn);
  }
  
  @Override
  public void minorCompactionStarted(TInfo info, long id, long seq, int tid, String fqfn) throws NoSuchLogIDException, TException {
    writer.minorCompactionStarted(info, id, seq, tid, fqfn);
  }
  
  @Override
  public void defineTablet(TInfo info, long id, long seq, int tid, TKeyExtent tablet) throws NoSuchLogIDException, TException {
    writer.defineTablet(info, id, seq, tid, tablet);
  }
  
  @Override
  public void process(WatchedEvent event) {
    LOG.debug("event " + event.getPath() + " " + event.getType() + " " + event.getState());
    
    if (event.getState() == KeeperState.Expired) {
      LOG.warn("Logger lost zookeeper registration at " + event.getPath());
      service.stop();
    } else if (event.getType() == EventType.NodeDeleted) {
      LOG.warn("Logger zookeeper entry lost " + event.getPath());
      service.stop();
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
}

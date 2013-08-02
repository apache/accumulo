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
package org.apache.accumulo.server.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.core.util.TBufferedSocket;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.thrift.metrics.ThriftMetrics;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class TServerUtils {
  private static final Logger log = Logger.getLogger(TServerUtils.class);
  
  public static final ThreadLocal<String> clientAddress = new ThreadLocal<String>();
  
  public static class ServerAddress {
    public final TServer server;
    public final InetSocketAddress address;
    
    public ServerAddress(TServer server, InetSocketAddress address) {
      this.server = server;
      this.address = address;
    }
  }
  
  /**
   * Start a server, at the given port, or higher, if that port is not available.
   * 
   * @param portHintProperty
   *          the port to attempt to open, can be zero, meaning "any available port"
   * @param processor
   *          the service to be started
   * @param serverName
   *          the name of the class that is providing the service
   * @param threadName
   *          name this service's thread for better debugging
   * @param portSearchProperty
   * @param minThreadProperty
   * @param timeBetweenThreadChecksProperty
   * @return the server object created, and the port actually used
   * @throws UnknownHostException
   *           when we don't know our own address
   */
  public static ServerAddress startServer(AccumuloConfiguration conf, String address, Property portHintProperty, TProcessor processor, String serverName, String threadName,
      Property portSearchProperty,
      Property minThreadProperty, 
      Property timeBetweenThreadChecksProperty, 
      Property maxMessageSizeProperty) throws UnknownHostException {
    int portHint = conf.getPort(portHintProperty);
    int minThreads = 2;
    if (minThreadProperty != null)
      minThreads = conf.getCount(minThreadProperty);
    long timeBetweenThreadChecks = 1000;
    if (timeBetweenThreadChecksProperty != null)
      timeBetweenThreadChecks = conf.getTimeInMillis(timeBetweenThreadChecksProperty);
    long maxMessageSize = 10 * 1000 * 1000;
    if (maxMessageSizeProperty != null)
      maxMessageSize = conf.getMemoryInBytes(maxMessageSizeProperty);
    boolean portSearch = false;
    if (portSearchProperty != null)
      portSearch = conf.getBoolean(portSearchProperty);
    // create the TimedProcessor outside the port search loop so we don't try to register the same metrics mbean more than once
    TServerUtils.TimedProcessor timedProcessor = new TServerUtils.TimedProcessor(processor, serverName, threadName);
    Random random = new Random();
    for (int j = 0; j < 100; j++) {
      
      // Are we going to slide around, looking for an open port?
      int portsToSearch = 1;
      if (portSearch)
        portsToSearch = 1000;
      
      for (int i = 0; i < portsToSearch; i++) {
        int port = portHint + i;
        if (portHint == 0)
          port = 1024 + random.nextInt(65535 - 1024);
        if (port > 65535)
          port = 1024 + port % (65535 - 1024);
        try {
          InetSocketAddress addr = new InetSocketAddress(address, port);
          return TServerUtils.startTServer(addr, timedProcessor, serverName, threadName, minThreads, timeBetweenThreadChecks, maxMessageSize);
        } catch (Exception ex) {
          log.info("Unable to use port " + port + ", retrying. (Thread Name = " + threadName + ")");
          UtilWaitThread.sleep(250);
        }
      }
    }
    throw new UnknownHostException("Unable to find a listen port");
  }
  
  public static class TimedProcessor implements TProcessor {
    
    final TProcessor other;
    ThriftMetrics metrics = null;
    long idleStart = 0;
    
    TimedProcessor(TProcessor next, String serverName, String threadName) {
      this.other = next;
      // Register the metrics MBean
      try {
        metrics = new ThriftMetrics(serverName, threadName);
        metrics.register();
      } catch (Exception e) {
        log.error("Exception registering MBean with MBean Server", e);
      }
      idleStart = System.currentTimeMillis();
    }
    
    @Override
    public boolean process(TProtocol in, TProtocol out) throws TException {
      long now = 0;
      if (metrics.isEnabled()) {
        now = System.currentTimeMillis();
        metrics.add(ThriftMetrics.idle, (now - idleStart));
      }
      try {
        try {
          return other.process(in, out);
        } catch (NullPointerException ex) {
          // THRIFT-1447 - remove with thrift 0.9
          return true;
        }
      } finally {
        if (metrics.isEnabled()) {
          idleStart = System.currentTimeMillis();
          metrics.add(ThriftMetrics.execute, idleStart - now);
        }
      }
    }
  }
  
  public static class ClientInfoProcessorFactory extends TProcessorFactory {
    
    public ClientInfoProcessorFactory(TProcessor processor) {
      super(processor);
    }
    
    public TProcessor getProcessor(TTransport trans) {
      if (trans instanceof TBufferedSocket) {
        TBufferedSocket tsock = (TBufferedSocket) trans;
        clientAddress.set(tsock.getClientString());
      }
      return super.getProcessor(trans);
    }
  }
  
  public static class THsHaServer extends org.apache.thrift.server.THsHaServer {
    public THsHaServer(Args args) {
      super(args);
    }
    
    protected Runnable getRunnable(FrameBuffer frameBuffer) {
      return new Invocation(frameBuffer);
    }
    
    private class Invocation implements Runnable {
      
      private final FrameBuffer frameBuffer;
      
      public Invocation(final FrameBuffer frameBuffer) {
        this.frameBuffer = frameBuffer;
      }
      
      public void run() {
        if (frameBuffer.trans_ instanceof TNonblockingSocket) {
          TNonblockingSocket tsock = (TNonblockingSocket) frameBuffer.trans_;
          Socket sock = tsock.getSocketChannel().socket();
          clientAddress.set(sock.getInetAddress().getHostAddress() + ":" + sock.getPort());
        }
        frameBuffer.invoke();
      }
    }
  }
  
  public static ServerAddress startHsHaServer(InetSocketAddress address, TProcessor processor, final String serverName, String threadName, final int numThreads,
      long timeBetweenThreadChecks, long maxMessageSize) throws TTransportException {
    TNonblockingServerSocket transport = new TNonblockingServerSocket(address);
    // check for the special "bind to everything address"
    if (address.getAddress().getHostAddress().equals("0.0.0.0")) {
      // can't get the address from the bind, so we'll do our best to invent our hostname
      try {
        address = new InetSocketAddress(InetAddress.getLocalHost().getHostName(), address.getPort());
      } catch (UnknownHostException e) {
        throw new TTransportException(e);
      }
    }
    THsHaServer.Args options = new THsHaServer.Args(transport);
    options.protocolFactory(ThriftUtil.protocolFactory());
    options.transportFactory(ThriftUtil.transportFactory(maxMessageSize));
    options.stopTimeoutVal(5);
    /*
     * Create our own very special thread pool.
     */
    final ThreadPoolExecutor pool = new SimpleThreadPool(numThreads, "ClientPool");
    // periodically adjust the number of threads we need by checking how busy our threads are
    SimpleTimer.getInstance().schedule(new Runnable() {
      @Override
      public void run() {
        if (pool.getCorePoolSize() <= pool.getActiveCount()) {
          int larger = pool.getCorePoolSize() + Math.min(pool.getQueue().size(), 2);
          log.info("Increasing server thread pool size on " + serverName + " to " + larger);
          pool.setMaximumPoolSize(larger);
          pool.setCorePoolSize(larger);
        } else {
          if (pool.getCorePoolSize() > pool.getActiveCount() + 3) {
            int smaller = Math.max(numThreads, pool.getCorePoolSize() - 1);
            if (smaller != pool.getCorePoolSize()) {
              // there is a race condition here... the active count could be higher by the time
              // we decrease the core pool size... so the active count could end up higher than
              // the core pool size, in which case everything will be queued... the increase case
              // should handle this and prevent deadlock
              log.info("Decreasing server thread pool size on " + serverName + " to " + smaller);
              pool.setCorePoolSize(smaller);
            }
          }
        }
      }
    }, timeBetweenThreadChecks, timeBetweenThreadChecks);
    options.executorService(pool);
    options.processorFactory(new TProcessorFactory(processor));
    return new ServerAddress(new THsHaServer(options), address);
  }
  
  public static ServerAddress startThreadPoolServer(InetSocketAddress address, TProcessor processor, String serverName, String threadName, int numThreads)
      throws TTransportException {
    
    // if port is zero, then we must bind to get the port number
    ServerSocket sock;
    try {
      sock = ServerSocketChannel.open().socket();
      sock.setReuseAddress(true);
      sock.bind(address);
      address = new InetSocketAddress(address.getHostName(), sock.getLocalPort());
    } catch (IOException ex) {
      throw new TTransportException(ex);
    }
    TServerTransport transport = new TBufferedServerSocket(sock, 32 * 1024);
    TThreadPoolServer.Args options = new TThreadPoolServer.Args(transport);
    options.protocolFactory(ThriftUtil.protocolFactory());
    options.transportFactory(ThriftUtil.transportFactory());
    options.processorFactory(new ClientInfoProcessorFactory(processor));
    return new ServerAddress(new TThreadPoolServer(options), address);
  }
  
  public static ServerAddress startTServer(InetSocketAddress address, TProcessor processor, String serverName, String threadName, int numThreads, long timeBetweenThreadChecks, long maxMessageSize)
      throws TTransportException {
    return startTServer(address, new TimedProcessor(processor, serverName, threadName), serverName, threadName, numThreads, timeBetweenThreadChecks, maxMessageSize);
  }
  
  public static ServerAddress startTServer(InetSocketAddress address, TimedProcessor processor, String serverName, String threadName, int numThreads, long timeBetweenThreadChecks, long maxMessageSize)
      throws TTransportException {
    ServerAddress result = startHsHaServer(address, processor, serverName, threadName, numThreads, timeBetweenThreadChecks, maxMessageSize);
    // ServerPort result = startThreadPoolServer(port, processor, serverName, threadName, -1);
    final TServer finalServer = result.server;
    Runnable serveTask = new Runnable() {
      public void run() {
        try {
          finalServer.serve();
        } catch (Error e) {
          Halt.halt("Unexpected error in TThreadPoolServer " + e + ", halting.");
        }
      }
    };
    serveTask = new LoggingRunnable(TServerUtils.log, serveTask);
    Thread thread = new Daemon(serveTask, threadName);
    thread.start();
    return result;
  }
  
  // Existing connections will keep our thread running: reach in with reflection and insist that they shutdown.
  public static void stopTServer(TServer s) {
    if (s == null)
      return;
    s.stop();
    try {
      Field f = s.getClass().getDeclaredField("executorService_");
      f.setAccessible(true);
      ExecutorService es = (ExecutorService) f.get(s);
      es.shutdownNow();
    } catch (Exception e) {
      TServerUtils.log.error("Unable to call shutdownNow", e);
    }
  }
}

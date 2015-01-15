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
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
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
import org.apache.accumulo.core.util.SslConnectionParams;
import org.apache.accumulo.core.util.TBufferedSocket;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.metrics.ThriftMetrics;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.google.common.net.HostAndPort;

public class TServerUtils {
  private static final Logger log = Logger.getLogger(TServerUtils.class);

  public static final ThreadLocal<String> clientAddress = new ThreadLocal<String>();

  public static class ServerAddress {
    public final TServer server;
    public final HostAndPort address;

    public ServerAddress(TServer server, HostAndPort address) {
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
   * @return the server object created, and the port actually used
   * @throws UnknownHostException
   *           when we don't know our own address
   */
  public static ServerAddress startServer(AccumuloConfiguration conf, String address, Property portHintProperty, TProcessor processor, String serverName,
      String threadName, Property portSearchProperty, Property minThreadProperty, Property timeBetweenThreadChecksProperty, Property maxMessageSizeProperty)
      throws UnknownHostException {
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
        if (portHint != 0 && i > 0)
          port = 1024 + random.nextInt(65535 - 1024);
        if (port > 65535)
          port = 1024 + port % (65535 - 1024);
        try {
          HostAndPort addr = HostAndPort.fromParts(address, port);
          return TServerUtils.startTServer(addr, timedProcessor, serverName, threadName, minThreads, timeBetweenThreadChecks, maxMessageSize,
              SslConnectionParams.forServer(conf), conf.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT));
        } catch (TTransportException ex) {
          log.error("Unable to start TServer", ex);
          if (ex.getCause() == null || ex.getCause().getClass() == BindException.class) {
            // Note: with a TNonblockingServerSocket a "port taken" exception is a cause-less
            // TTransportException, and with a TSocket created by TSSLTransportFactory, it
            // comes through as caused by a BindException.
            log.info("Unable to use port " + port + ", retrying. (Thread Name = " + threadName + ")");
            UtilWaitThread.sleep(250);
          } else {
            // thrift is passing up a nested exception that isn't a BindException,
            // so no reason to believe retrying on a different port would help.
            log.error("Unable to start TServer", ex);
            break;
          }
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
        return other.process(in, out);
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

    @Override
    public TProcessor getProcessor(TTransport trans) {
      if (trans instanceof TBufferedSocket) {
        TBufferedSocket tsock = (TBufferedSocket) trans;
        clientAddress.set(tsock.getClientString());
      } else if (trans instanceof TSocket) {
        TSocket tsock = (TSocket) trans;
        clientAddress.set(tsock.getSocket().getInetAddress().getHostAddress() + ":" + tsock.getSocket().getPort());
      } else {
        log.warn("Unable to extract clientAddress from transport of type " + trans.getClass());
      }
      return super.getProcessor(trans);
    }
  }

  public static ServerAddress createNonBlockingServer(HostAndPort address, TProcessor processor, final String serverName, String threadName,
      final int numThreads, long timeBetweenThreadChecks, long maxMessageSize) throws TTransportException {
    TNonblockingServerSocket transport = new TNonblockingServerSocket(new InetSocketAddress(address.getHostText(), address.getPort()));
    CustomNonBlockingServer.Args options = new CustomNonBlockingServer.Args(transport);
    options.protocolFactory(ThriftUtil.protocolFactory());
    options.transportFactory(ThriftUtil.transportFactory(maxMessageSize));
    options.maxReadBufferBytes = maxMessageSize;
    options.stopTimeoutVal(5);
    /*
     * Create our own very special thread pool.
     */
    final ThreadPoolExecutor pool = new SimpleThreadPool(numThreads, "ClientPool");
    // periodically adjust the number of threads we need by checking how busy our threads are
    SimpleTimer.getInstance().schedule(new Runnable() {
      @Override
      public void run() {
        // there is a minor race condition between sampling the current state of the thread pool and adjusting it
        // however, this isn't really an issue, since it adjusts periodically anyway
        if (pool.getCorePoolSize() <= pool.getActiveCount()) {
          int larger = pool.getCorePoolSize() + Math.min(pool.getQueue().size(), 2);
          log.info("Increasing server thread pool size on " + serverName + " to " + larger);
          pool.setMaximumPoolSize(larger);
          pool.setCorePoolSize(larger);
        } else {
          if (pool.getCorePoolSize() > pool.getActiveCount() + 3) {
            int smaller = Math.max(numThreads, pool.getCorePoolSize() - 1);
            if (smaller != pool.getCorePoolSize()) {
              log.info("Decreasing server thread pool size on " + serverName + " to " + smaller);
              pool.setCorePoolSize(smaller);
            }
          }
        }
      }
    }, timeBetweenThreadChecks, timeBetweenThreadChecks);
    options.executorService(pool);
    options.processorFactory(new TProcessorFactory(processor));
    if (address.getPort() == 0) {
      address = HostAndPort.fromParts(address.getHostText(), transport.getPort());
    }
    return new ServerAddress(new CustomNonBlockingServer(options), address);
  }

  public static ServerAddress createThreadPoolServer(HostAndPort address, TProcessor processor, String serverName, String threadName, int numThreads)
      throws TTransportException {

    // if port is zero, then we must bind to get the port number
    ServerSocket sock;
    try {
      sock = ServerSocketChannel.open().socket();
      sock.setReuseAddress(true);
      sock.bind(new InetSocketAddress(address.getHostText(), address.getPort()));
      address = HostAndPort.fromParts(address.getHostText(), sock.getLocalPort());
    } catch (IOException ex) {
      throw new TTransportException(ex);
    }
    TServerTransport transport = new TBufferedServerSocket(sock, 32 * 1024);
    return new ServerAddress(createThreadPoolServer(transport, processor), address);
  }

  public static TServer createThreadPoolServer(TServerTransport transport, TProcessor processor) {
    TThreadPoolServer.Args options = new TThreadPoolServer.Args(transport);
    options.protocolFactory(ThriftUtil.protocolFactory());
    options.transportFactory(ThriftUtil.transportFactory());
    options.processorFactory(new ClientInfoProcessorFactory(processor));
    return new TThreadPoolServer(options);
  }

  public static ServerAddress createSslThreadPoolServer(HostAndPort address, TProcessor processor, long socketTimeout, SslConnectionParams sslParams)
      throws TTransportException {
    org.apache.thrift.transport.TServerSocket transport;
    try {
      transport = ThriftUtil.getServerSocket(address.getPort(), (int) socketTimeout, InetAddress.getByName(address.getHostText()), sslParams);
    } catch (UnknownHostException e) {
      throw new TTransportException(e);
    }
    if (address.getPort() == 0) {
      address = HostAndPort.fromParts(address.getHostText(), transport.getServerSocket().getLocalPort());
    }
    return new ServerAddress(createThreadPoolServer(transport, processor), address);
  }

  public static ServerAddress startTServer(HostAndPort address, TProcessor processor, String serverName, String threadName, int numThreads,
      long timeBetweenThreadChecks, long maxMessageSize, SslConnectionParams sslParams, long sslSocketTimeout) throws TTransportException {
    return startTServer(address, new TimedProcessor(processor, serverName, threadName), serverName, threadName, numThreads, timeBetweenThreadChecks,
        maxMessageSize, sslParams, sslSocketTimeout);
  }

  public static ServerAddress startTServer(HostAndPort address, TimedProcessor processor, String serverName, String threadName, int numThreads,
      long timeBetweenThreadChecks, long maxMessageSize, SslConnectionParams sslParams, long sslSocketTimeout) throws TTransportException {

    ServerAddress serverAddress;
    if (sslParams != null) {
      serverAddress = createSslThreadPoolServer(address, processor, sslSocketTimeout, sslParams);
    } else {
      serverAddress = createNonBlockingServer(address, processor, serverName, threadName, numThreads, timeBetweenThreadChecks, maxMessageSize);
    }
    final TServer finalServer = serverAddress.server;
    Runnable serveTask = new Runnable() {
      @Override
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
    // check for the special "bind to everything address"
    if (serverAddress.address.getHostText().equals("0.0.0.0")) {
      // can't get the address from the bind, so we'll do our best to invent our hostname
      try {
        serverAddress = new ServerAddress(finalServer, HostAndPort.fromParts(InetAddress.getLocalHost().getHostName(), serverAddress.address.getPort()));
      } catch (UnknownHostException e) {
        throw new TTransportException(e);
      }
    }
    return serverAddress;
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

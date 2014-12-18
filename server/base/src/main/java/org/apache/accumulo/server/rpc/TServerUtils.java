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
package org.apache.accumulo.server.rpc;

import java.lang.reflect.Field;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import javax.net.ssl.SSLServerSocket;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

/**
 * Factory methods for creating Thrift server objects
 */
public class TServerUtils {
  private static final Logger log = LoggerFactory.getLogger(TServerUtils.class);

  /**
   * Static instance, passed to {@link ClientInfoProcessorFactory}, which will contain the client address of any incoming RPC.
   */
  public static final ThreadLocal<String> clientAddress = new ThreadLocal<String>();

  /**
   * Start a server, at the given port, or higher, if that port is not available.
   *
   * @param service
   *          RPC configuration
   * @param portHintProperty
   *          the port to attempt to open, can be zero, meaning "any available port"
   * @param processor
   *          the service to be started
   * @param serverName
   *          the name of the class that is providing the service
   * @param threadName
   *          name this service's thread for better debugging
   * @param portSearchProperty
   *          A boolean Property to control if port-search should be used, or null to disable
   * @param minThreadProperty
   *          A Property to control the minimum number of threads in the pool
   * @param timeBetweenThreadChecksProperty
   *          A Property to control the amount of time between checks to resize the thread pool
   * @param maxMessageSizeProperty
   *          A Property to control the maximum Thrift message size accepted
   * @return the server object created, and the port actually used
   * @throws UnknownHostException
   *           when we don't know our own address
   */
  public static ServerAddress startServer(AccumuloServerContext service, String hostname, Property portHintProperty, TProcessor processor, String serverName,
      String threadName, Property portSearchProperty, Property minThreadProperty, Property timeBetweenThreadChecksProperty, Property maxMessageSizeProperty)
      throws UnknownHostException {
    final AccumuloConfiguration config = service.getConfiguration();

    final int portHint = config.getPort(portHintProperty);

    int minThreads = 2;
    if (minThreadProperty != null)
      minThreads = config.getCount(minThreadProperty);

    long timeBetweenThreadChecks = 1000;
    if (timeBetweenThreadChecksProperty != null)
      timeBetweenThreadChecks = config.getTimeInMillis(timeBetweenThreadChecksProperty);

    long maxMessageSize = 10 * 1000 * 1000;
    if (maxMessageSizeProperty != null)
      maxMessageSize = config.getMemoryInBytes(maxMessageSizeProperty);

    boolean portSearch = false;
    if (portSearchProperty != null)
      portSearch = config.getBoolean(portSearchProperty);

    final int simpleTimerThreadpoolSize = config.getCount(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);

    // create the TimedProcessor outside the port search loop so we don't try to register the same metrics mbean more than once
    TimedProcessor timedProcessor = new TimedProcessor(config, processor, serverName, threadName);

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
          HostAndPort addr = HostAndPort.fromParts(hostname, port);
          return TServerUtils.startTServer(addr, timedProcessor, serverName, threadName, minThreads, simpleTimerThreadpoolSize, timeBetweenThreadChecks,
              maxMessageSize, service.getServerSslParams(), service.getClientTimeoutInMillis());
        } catch (TTransportException ex) {
          log.error("Unable to start TServer", ex);
          if (ex.getCause() == null || ex.getCause().getClass() == BindException.class) {
            // Note: with a TNonblockingServerSocket a "port taken" exception is a cause-less
            // TTransportException, and with a TSocket created by TSSLTransportFactory, it
            // comes through as caused by a BindException.
            log.info("Unable to use port {}, retrying. (Thread Name = {})", port, threadName);
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

  /**
   * Create a NonBlockingServer with a custom thread pool that can dynamically resize itself.
   */
  public static ServerAddress createNonBlockingServer(HostAndPort address, TProcessor processor, final String serverName, String threadName,
      final int numThreads, final int numSTThreads, long timeBetweenThreadChecks, long maxMessageSize) throws TTransportException {

    final TNonblockingServerSocket transport = new TNonblockingServerSocket(new InetSocketAddress(address.getHostText(), address.getPort()));
    final CustomNonBlockingServer.Args options = new CustomNonBlockingServer.Args(transport);

    options.protocolFactory(ThriftUtil.protocolFactory());
    options.transportFactory(ThriftUtil.transportFactory(maxMessageSize));
    options.maxReadBufferBytes = maxMessageSize;
    options.stopTimeoutVal(5);

    // Create our own very special thread pool.
    final ThreadPoolExecutor pool = new SimpleThreadPool(numThreads, "ClientPool");
    // periodically adjust the number of threads we need by checking how busy our threads are
    SimpleTimer.getInstance(numSTThreads).schedule(new Runnable() {
      @Override
      public void run() {
        if (pool.getCorePoolSize() <= pool.getActiveCount()) {
          int larger = pool.getCorePoolSize() + Math.min(pool.getQueue().size(), 2);
          log.info("Increasing server thread pool size on {} to {}", serverName, larger);
          pool.setMaximumPoolSize(larger);
          pool.setCorePoolSize(larger);
        } else {
          if (pool.getCorePoolSize() > pool.getActiveCount() + 3) {
            int smaller = Math.max(numThreads, pool.getCorePoolSize() - 1);
            if (smaller != pool.getCorePoolSize()) {
              // ACCUMULO-2997 there is a race condition here... the active count could be higher by the time
              // we decrease the core pool size... so the active count could end up higher than
              // the core pool size, in which case everything will be queued... the increase case
              // should handle this and prevent deadlock
              log.info("Decreasing server thread pool size on {} to {}", serverName, smaller);
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

  /**
   * Create a TThreadPoolServer with the given transport and processor
   *
   * @param transport
   *          TServerTransport for the server
   * @param processor
   *          TProcessor for the server
   * @return A configured TThreadPoolServer
   */
  public static TThreadPoolServer createThreadPoolServer(TServerTransport transport, TProcessor processor) {
    final TThreadPoolServer.Args options = new TThreadPoolServer.Args(transport);
    options.protocolFactory(ThriftUtil.protocolFactory());
    options.transportFactory(ThriftUtil.transportFactory());
    options.processorFactory(new ClientInfoProcessorFactory(clientAddress, processor));
    return new TThreadPoolServer(options);
  }

  /**
   * Create the Thrift server socket for RPC running over SSL.
   *
   * @param port
   *          Port of the server socket to bind to
   * @param timeout
   *          Socket timeout
   * @param address
   *          Address to bind the socket to
   * @param params
   *          SSL parameters
   * @return A configured TServerSocket configured to use SSL
   */
  public static TServerSocket getSslServerSocket(int port, int timeout, InetAddress address, SslConnectionParams params) throws TTransportException {
    TServerSocket tServerSock;
    if (params.useJsse()) {
      tServerSock = TSSLTransportFactory.getServerSocket(port, timeout, params.isClientAuth(), address);
    } else {
      tServerSock = TSSLTransportFactory.getServerSocket(port, timeout, address, params.getTTransportParams());
    }

    final ServerSocket serverSock = tServerSock.getServerSocket();
    if (serverSock instanceof SSLServerSocket) {
      SSLServerSocket sslServerSock = (SSLServerSocket) serverSock;
      String[] protocols = params.getServerProtocols();

      // Be nice for the user and automatically remove protocols that might not exist in their JVM. Keeps us from forcing config alterations too
      // e.g. TLSv1.1 and TLSv1.2 don't exist in JDK6
      Set<String> socketEnabledProtocols = new HashSet<String>(Arrays.asList(sslServerSock.getEnabledProtocols()));
      // Keep only the enabled protocols that were specified by the configuration
      socketEnabledProtocols.retainAll(Arrays.asList(protocols));
      if (socketEnabledProtocols.isEmpty()) {
        // Bad configuration...
        throw new RuntimeException("No available protocols available for secure socket. Availaable protocols: "
            + Arrays.toString(sslServerSock.getEnabledProtocols()) + ", allowed protocols: " + Arrays.toString(protocols));
      }

      // Set the protocol(s) on the server socket
      sslServerSock.setEnabledProtocols(socketEnabledProtocols.toArray(new String[0]));
    }

    return tServerSock;
  }

  /**
   * Create a Thrift SSL server.
   *
   * @param address
   *          host and port to bind to
   * @param processor
   *          TProcessor for the server
   * @param socketTimeout
   *          Socket timeout
   * @param sslParams
   *          SSL parameters
   * @return A ServerAddress with the bound-socket information and the Thrift server
   */
  public static ServerAddress createSslThreadPoolServer(HostAndPort address, TProcessor processor, long socketTimeout, SslConnectionParams sslParams)
      throws TTransportException {
    org.apache.thrift.transport.TServerSocket transport;
    try {
      transport = getSslServerSocket(address.getPort(), (int) socketTimeout, InetAddress.getByName(address.getHostText()), sslParams);
    } catch (UnknownHostException e) {
      throw new TTransportException(e);
    }
    if (address.getPort() == 0) {
      address = HostAndPort.fromParts(address.getHostText(), transport.getServerSocket().getLocalPort());
    }
    return new ServerAddress(createThreadPoolServer(transport, processor), address);
  }

  /**
   * Create a Thrift server given the provided and Accumulo configuration.
   */
  public static ServerAddress startTServer(AccumuloConfiguration conf, HostAndPort address, TProcessor processor, String serverName, String threadName,
      int numThreads, int numSTThreads, long timeBetweenThreadChecks, long maxMessageSize, SslConnectionParams sslParams, long sslSocketTimeout)
      throws TTransportException {
    return startTServer(address, new TimedProcessor(conf, processor, serverName, threadName), serverName, threadName, numThreads, numSTThreads,
        timeBetweenThreadChecks, maxMessageSize, sslParams, sslSocketTimeout);
  }

  /**
   * Start the appropriate Thrift server (SSL or non-blocking server) for the given parameters. Non-null SSL parameters will cause an SSL server to be started.
   *
   * @return A ServerAddress encapsulating the Thrift server created and the host/port which it is bound to.
   */
  public static ServerAddress startTServer(HostAndPort address, TimedProcessor processor, String serverName, String threadName, int numThreads,
      int numSTThreads, long timeBetweenThreadChecks, long maxMessageSize, SslConnectionParams sslParams, long sslSocketTimeout) throws TTransportException {

    ServerAddress serverAddress;
    if (sslParams != null) {
      serverAddress = createSslThreadPoolServer(address, processor, sslSocketTimeout, sslParams);
    } else {
      serverAddress = createNonBlockingServer(address, processor, serverName, threadName, numThreads, numSTThreads, timeBetweenThreadChecks, maxMessageSize);
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

  /**
   * Stop a Thrift TServer. Existing connections will keep our thread running; use reflection to forcibly shut down the threadpool.
   *
   * @param s
   *          The TServer to stop
   */
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
      log.error("Unable to call shutdownNow", e);
    }
  }
}

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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import javax.net.ssl.SSLServerSocket;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.UGIAssumingTransportFactory;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.fate.util.LoggingRunnable;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory methods for creating Thrift server objects
 */
public class TServerUtils {
  private static final Logger log = LoggerFactory.getLogger(TServerUtils.class);

  /**
   * Static instance, passed to {@link ClientInfoProcessorFactory}, which will contain the client address of any incoming RPC.
   */
  public static final ThreadLocal<String> clientAddress = new ThreadLocal<>();

  /**
   *
   * @param hostname
   *          name of the host
   * @param ports
   *          array of ports
   * @return array of HostAndPort objects
   */
  public static HostAndPort[] getHostAndPorts(String hostname, int[] ports) {
    HostAndPort[] addresses = new HostAndPort[ports.length];
    for (int i = 0; i < ports.length; i++) {
      addresses[i] = HostAndPort.fromParts(hostname, ports[i]);
    }
    return addresses;
  }

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

    final int[] portHint = config.getPort(portHintProperty);

    int minThreads = 2;
    if (minThreadProperty != null)
      minThreads = config.getCount(minThreadProperty);

    long timeBetweenThreadChecks = 1000;
    if (timeBetweenThreadChecksProperty != null)
      timeBetweenThreadChecks = config.getTimeInMillis(timeBetweenThreadChecksProperty);

    long maxMessageSize = 10 * 1000 * 1000;
    if (maxMessageSizeProperty != null)
      maxMessageSize = config.getAsBytes(maxMessageSizeProperty);

    boolean portSearch = false;
    if (portSearchProperty != null)
      portSearch = config.getBoolean(portSearchProperty);

    final int simpleTimerThreadpoolSize = config.getCount(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);
    final ThriftServerType serverType = service.getThriftServerType();

    if (ThriftServerType.SASL == serverType) {
      processor = updateSaslProcessor(serverType, processor);
    }

    // create the TimedProcessor outside the port search loop so we don't try to register the same metrics mbean more than once
    TimedProcessor timedProcessor = new TimedProcessor(config, processor, serverName, threadName);

    HostAndPort[] addresses = getHostAndPorts(hostname, portHint);
    try {
      return TServerUtils.startTServer(serverType, timedProcessor, serverName, threadName, minThreads, simpleTimerThreadpoolSize, timeBetweenThreadChecks,
          maxMessageSize, service.getServerSslParams(), service.getSaslParams(), service.getClientTimeoutInMillis(), addresses);
    } catch (TTransportException e) {
      if (portSearch) {
        HostAndPort last = addresses[addresses.length - 1];
        // Attempt to allocate a port outside of the specified port property
        // Search sequentially over the next 1000 ports
        for (int i = last.getPort() + 1; i < last.getPort() + 1001; i++) {
          int port = i;
          if (port > 65535) {
            break;
          }
          try {
            HostAndPort addr = HostAndPort.fromParts(hostname, port);
            return TServerUtils.startTServer(serverType, timedProcessor, serverName, threadName, minThreads, simpleTimerThreadpoolSize,
                timeBetweenThreadChecks, maxMessageSize, service.getServerSslParams(), service.getSaslParams(), service.getClientTimeoutInMillis(), addr);
          } catch (TTransportException tte) {
            log.info("Unable to use port {}, retrying. (Thread Name = {})", port, threadName);
          }
        }
        log.error("Unable to start TServer", e);
        throw new UnknownHostException("Unable to find a listen port");
      } else {
        log.error("Unable to start TServer", e);
        throw new UnknownHostException("Unable to find a listen port");
      }
    }
  }

  /**
   * Create a NonBlockingServer with a custom thread pool that can dynamically resize itself.
   */
  public static ServerAddress createNonBlockingServer(HostAndPort address, TProcessor processor, TProtocolFactory protocolFactory, final String serverName,
      String threadName, final int numThreads, final int numSTThreads, long timeBetweenThreadChecks, long maxMessageSize) throws TTransportException {

    final TNonblockingServerSocket transport = new TNonblockingServerSocket(new InetSocketAddress(address.getHost(), address.getPort()));
    final CustomNonBlockingServer.Args options = new CustomNonBlockingServer.Args(transport);

    options.protocolFactory(protocolFactory);
    options.transportFactory(ThriftUtil.transportFactory(maxMessageSize));
    options.maxReadBufferBytes = maxMessageSize;
    options.stopTimeoutVal(5);

    // Create our own very special thread pool.
    ThreadPoolExecutor pool = createSelfResizingThreadPool(serverName, numThreads, numSTThreads, timeBetweenThreadChecks);

    options.executorService(pool);
    options.processorFactory(new TProcessorFactory(processor));

    if (address.getPort() == 0) {
      address = HostAndPort.fromParts(address.getHost(), transport.getPort());
    }

    return new ServerAddress(new CustomNonBlockingServer(options), address);
  }

  /**
   * Creates a {@link SimpleThreadPool} which uses {@link SimpleTimer} to inspect the core pool size and number of active threads of the
   * {@link ThreadPoolExecutor} and increase or decrease the core pool size based on activity (excessive or lack thereof).
   *
   * @param serverName
   *          A name to describe the thrift server this executor will service
   * @param executorThreads
   *          The maximum number of threads for the executor
   * @param simpleTimerThreads
   *          The numbers of threads used to get the {@link SimpleTimer} instance
   * @param timeBetweenThreadChecks
   *          The amount of time, in millis, between attempts to resize the executor thread pool
   * @return A {@link ThreadPoolExecutor} which will resize itself automatically
   */
  public static ThreadPoolExecutor createSelfResizingThreadPool(final String serverName, final int executorThreads, int simpleTimerThreads,
      long timeBetweenThreadChecks) {
    final ThreadPoolExecutor pool = new SimpleThreadPool(executorThreads, "ClientPool");
    // periodically adjust the number of threads we need by checking how busy our threads are
    SimpleTimer.getInstance(simpleTimerThreads).schedule(new Runnable() {
      @Override
      public void run() {
        // there is a minor race condition between sampling the current state of the thread pool and adjusting it
        // however, this isn't really an issue, since it adjusts periodically anyway
        if (pool.getCorePoolSize() <= pool.getActiveCount()) {
          int larger = pool.getCorePoolSize() + Math.min(pool.getQueue().size(), 2);
          log.info("Increasing server thread pool size on {} to {}", serverName, larger);
          pool.setMaximumPoolSize(larger);
          pool.setCorePoolSize(larger);
        } else {
          if (pool.getCorePoolSize() > pool.getActiveCount() + 3) {
            int smaller = Math.max(executorThreads, pool.getCorePoolSize() - 1);
            if (smaller != pool.getCorePoolSize()) {
              log.info("Decreasing server thread pool size on {} to {}", serverName, smaller);
              pool.setCorePoolSize(smaller);
            }
          }
        }
      }
    }, timeBetweenThreadChecks, timeBetweenThreadChecks);
    return pool;
  }

  /**
   * Creates a TTheadPoolServer for normal unsecure operation. Useful for comparing performance against SSL or SASL transports.
   *
   * @param address
   *          Address to bind to
   * @param processor
   *          TProcessor for the server
   * @param maxMessageSize
   *          Maximum size of a Thrift message allowed
   * @return A configured TThreadPoolServer and its bound address information
   */
  public static ServerAddress createBlockingServer(HostAndPort address, TProcessor processor, TProtocolFactory protocolFactory, long maxMessageSize,
      String serverName, int numThreads, int numSimpleTimerThreads, long timeBetweenThreadChecks) throws TTransportException {

    TServerSocket transport = new TServerSocket(address.getPort());
    ThreadPoolExecutor pool = createSelfResizingThreadPool(serverName, numThreads, numSimpleTimerThreads, timeBetweenThreadChecks);
    TThreadPoolServer server = createTThreadPoolServer(transport, processor, ThriftUtil.transportFactory(maxMessageSize), protocolFactory, pool);

    if (address.getPort() == 0) {
      address = HostAndPort.fromParts(address.getHost(), transport.getServerSocket().getLocalPort());
      log.info("Blocking Server bound on {}", address);
    }

    return new ServerAddress(server, address);

  }

  /**
   * Create a {@link TThreadPoolServer} with the provided transport, processor and transport factory.
   *
   * @param transport
   *          Server transport
   * @param processor
   *          Processor implementation
   * @param transportFactory
   *          Transport factory
   * @return A configured {@link TThreadPoolServer}
   */
  public static TThreadPoolServer createTThreadPoolServer(TServerTransport transport, TProcessor processor, TTransportFactory transportFactory,
      TProtocolFactory protocolFactory) {
    return createTThreadPoolServer(transport, processor, transportFactory, protocolFactory, null);
  }

  /**
   * Create a {@link TThreadPoolServer} with the provided server transport, processor and transport factory.
   *
   * @param transport
   *          TServerTransport for the server
   * @param processor
   *          TProcessor for the server
   * @param transportFactory
   *          TTransportFactory for the server
   */
  public static TThreadPoolServer createTThreadPoolServer(TServerTransport transport, TProcessor processor, TTransportFactory transportFactory,
      TProtocolFactory protocolFactory, ExecutorService service) {
    TThreadPoolServer.Args options = new TThreadPoolServer.Args(transport);
    options.protocolFactory(protocolFactory);
    options.transportFactory(transportFactory);
    options.processorFactory(new ClientInfoProcessorFactory(clientAddress, processor));
    if (null != service) {
      options.executorService(service);
    }
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
      Set<String> socketEnabledProtocols = new HashSet<>(Arrays.asList(sslServerSock.getEnabledProtocols()));
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
  public static ServerAddress createSslThreadPoolServer(HostAndPort address, TProcessor processor, TProtocolFactory protocolFactory, long socketTimeout,
      SslConnectionParams sslParams, String serverName, int numThreads, int numSimpleTimerThreads, long timeBetweenThreadChecks) throws TTransportException {
    TServerSocket transport;
    try {
      transport = getSslServerSocket(address.getPort(), (int) socketTimeout, InetAddress.getByName(address.getHost()), sslParams);
    } catch (UnknownHostException e) {
      throw new TTransportException(e);
    }

    if (address.getPort() == 0) {
      address = HostAndPort.fromParts(address.getHost(), transport.getServerSocket().getLocalPort());
      log.info("SSL Thread Pool Server bound on {}", address);
    }

    ThreadPoolExecutor pool = createSelfResizingThreadPool(serverName, numThreads, numSimpleTimerThreads, timeBetweenThreadChecks);

    return new ServerAddress(createTThreadPoolServer(transport, processor, ThriftUtil.transportFactory(), protocolFactory, pool), address);
  }

  public static ServerAddress createSaslThreadPoolServer(HostAndPort address, TProcessor processor, TProtocolFactory protocolFactory, long socketTimeout,
      SaslServerConnectionParams params, final String serverName, String threadName, final int numThreads, final int numSTThreads, long timeBetweenThreadChecks)
      throws TTransportException {
    // We'd really prefer to use THsHaServer (or similar) to avoid 1 RPC == 1 Thread that the TThreadPoolServer does,
    // but sadly this isn't the case. Because TSaslTransport needs to issue a handshake when it open()'s which will fail
    // when the server does an accept() to (presumably) wake up the eventing system.
    log.info("Creating SASL thread pool thrift server on listening on {}:{}", address.getHost(), address.getPort());
    TServerSocket transport = new TServerSocket(address.getPort(), (int) socketTimeout);

    String hostname, fqdn;
    try {
      hostname = InetAddress.getByName(address.getHost()).getCanonicalHostName();
      fqdn = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      transport.close();
      throw new TTransportException(e);
    }

    // If we can't get a real hostname from the provided host test, use the hostname from DNS for localhost
    if ("0.0.0.0".equals(hostname)) {
      hostname = fqdn;
    }

    // ACCUMULO-3497 an easy sanity check we can perform for the user when SASL is enabled. Clients and servers have to agree upon the FQDN
    // so that the SASL handshake can occur. If the provided hostname doesn't match the FQDN for this host, fail quickly and inform them to update
    // their configuration.
    if (!hostname.equals(fqdn)) {
      log.error(
          "Expected hostname of '{}' but got '{}'. Ensure the entries in the Accumulo hosts files (e.g. masters, tservers) are the FQDN for each host when using SASL.",
          fqdn, hostname);
      transport.close();
      throw new RuntimeException("SASL requires that the address the thrift server listens on is the same as the FQDN for this host");
    }

    final UserGroupInformation serverUser;
    try {
      serverUser = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      transport.close();
      throw new TTransportException(e);
    }

    log.debug("Logged in as {}, creating TSaslServerTransport factory with {}/{}", serverUser, params.getKerberosServerPrimary(), hostname);

    // Make the SASL transport factory with the instance and primary from the kerberos server principal, SASL properties
    // and the SASL callback handler from Hadoop to ensure authorization ID is the authentication ID. Despite the 'protocol' argument seeming to be useless, it
    // *must* be the primary of the server.
    TSaslServerTransport.Factory saslTransportFactory = new TSaslServerTransport.Factory();
    saslTransportFactory.addServerDefinition(ThriftUtil.GSSAPI, params.getKerberosServerPrimary(), hostname, params.getSaslProperties(),
        new SaslRpcServer.SaslGssCallbackHandler());

    if (null != params.getSecretManager()) {
      log.info("Adding DIGEST-MD5 server definition for delegation tokens");
      saslTransportFactory.addServerDefinition(ThriftUtil.DIGEST_MD5, params.getKerberosServerPrimary(), hostname, params.getSaslProperties(),
          new SaslServerDigestCallbackHandler(params.getSecretManager()));
    } else {
      log.info("SecretManager is null, not adding support for delegation token authentication");
    }

    // Make sure the TTransportFactory is performing a UGI.doAs
    TTransportFactory ugiTransportFactory = new UGIAssumingTransportFactory(saslTransportFactory, serverUser);

    if (address.getPort() == 0) {
      // If we chose a port dynamically, make a new use it (along with the proper hostname)
      address = HostAndPort.fromParts(address.getHost(), transport.getServerSocket().getLocalPort());
      log.info("SASL thrift server bound on {}", address);
    }

    ThreadPoolExecutor pool = createSelfResizingThreadPool(serverName, numThreads, numSTThreads, timeBetweenThreadChecks);

    final TThreadPoolServer server = createTThreadPoolServer(transport, processor, ugiTransportFactory, protocolFactory, pool);

    return new ServerAddress(server, address);
  }

  public static ServerAddress startTServer(AccumuloConfiguration conf, ThriftServerType serverType, TProcessor processor, String serverName, String threadName,
      int numThreads, int numSTThreads, long timeBetweenThreadChecks, long maxMessageSize, SslConnectionParams sslParams,
      SaslServerConnectionParams saslParams, long serverSocketTimeout, HostAndPort... addresses) throws TTransportException {

    if (ThriftServerType.SASL == serverType) {
      processor = updateSaslProcessor(serverType, processor);
    }

    return startTServer(serverType, new TimedProcessor(conf, processor, serverName, threadName), serverName, threadName, numThreads, numSTThreads,
        timeBetweenThreadChecks, maxMessageSize, sslParams, saslParams, serverSocketTimeout, addresses);
  }

  /**
   * @see #startTServer(ThriftServerType, TimedProcessor, TProtocolFactory, String, String, int, int, long, long, SslConnectionParams,
   *      SaslServerConnectionParams, long, HostAndPort...)
   */
  public static ServerAddress startTServer(ThriftServerType serverType, TimedProcessor processor, String serverName, String threadName, int numThreads,
      int numSTThreads, long timeBetweenThreadChecks, long maxMessageSize, SslConnectionParams sslParams, SaslServerConnectionParams saslParams,
      long serverSocketTimeout, HostAndPort... addresses) throws TTransportException {
    return startTServer(serverType, processor, ThriftUtil.protocolFactory(), serverName, threadName, numThreads, numSTThreads, timeBetweenThreadChecks,
        maxMessageSize, sslParams, saslParams, serverSocketTimeout, addresses);
  }

  /**
   * Start the appropriate Thrift server (SSL or non-blocking server) for the given parameters. Non-null SSL parameters will cause an SSL server to be started.
   *
   * @return A ServerAddress encapsulating the Thrift server created and the host/port which it is bound to.
   */
  public static ServerAddress startTServer(ThriftServerType serverType, TimedProcessor processor, TProtocolFactory protocolFactory, String serverName,
      String threadName, int numThreads, int numSTThreads, long timeBetweenThreadChecks, long maxMessageSize, SslConnectionParams sslParams,
      SaslServerConnectionParams saslParams, long serverSocketTimeout, HostAndPort... addresses) throws TTransportException {

    // This is presently not supported. It's hypothetically possible, I believe, to work, but it would require changes in how the transports
    // work at the Thrift layer to ensure that both the SSL and SASL handshakes function. SASL's quality of protection addresses privacy issues.
    checkArgument(!(sslParams != null && saslParams != null), "Cannot start a Thrift server using both SSL and SASL");

    ServerAddress serverAddress = null;
    for (HostAndPort address : addresses) {
      try {
        switch (serverType) {
          case SSL:
            log.debug("Instantiating SSL Thrift server");
            serverAddress = createSslThreadPoolServer(address, processor, protocolFactory, serverSocketTimeout, sslParams, serverName, numThreads,
                numSTThreads, timeBetweenThreadChecks);
            break;
          case SASL:
            log.debug("Instantiating SASL Thrift server");
            serverAddress = createSaslThreadPoolServer(address, processor, protocolFactory, serverSocketTimeout, saslParams, serverName, threadName,
                numThreads, numSTThreads, timeBetweenThreadChecks);
            break;
          case THREADPOOL:
            log.debug("Instantiating unsecure TThreadPool Thrift server");
            serverAddress = createBlockingServer(address, processor, protocolFactory, maxMessageSize, serverName, numThreads, numSTThreads,
                timeBetweenThreadChecks);
            break;
          case CUSTOM_HS_HA: // Intentional passthrough -- Our custom wrapper around HsHa is the default
          default:
            log.debug("Instantiating default, unsecure custom half-async Thrift server");
            serverAddress = createNonBlockingServer(address, processor, protocolFactory, serverName, threadName, numThreads, numSTThreads,
                timeBetweenThreadChecks, maxMessageSize);
        }
        break;
      } catch (TTransportException e) {
        log.warn("Error attempting to create server at {}. Error: {}", address.toString(), e.getMessage());
      }
    }
    if (null == serverAddress) {
      throw new TTransportException("Unable to create server on addresses: " + Arrays.toString(addresses));
    }

    final TServer finalServer = serverAddress.server;
    Runnable serveTask = new Runnable() {
      @Override
      public void run() {
        try {
          finalServer.serve();
        } catch (Error e) {
          Halt.halt("Unexpected error in TThreadPoolServer " + e + ", halting.", 1);
        }
      }
    };

    serveTask = new LoggingRunnable(TServerUtils.log, serveTask);
    Thread thread = new Daemon(serveTask, threadName);
    thread.start();

    // check for the special "bind to everything address"
    if (serverAddress.address.getHost().equals("0.0.0.0")) {
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

  /**
   * Wrap the provided processor in the {@link UGIAssumingProcessor} so Kerberos authentication works. Requires the <code>serverType</code> to be
   * {@link ThriftServerType#SASL} and throws an exception when it is not.
   *
   * @return A {@link UGIAssumingProcessor} which wraps the provided processor
   */
  private static TProcessor updateSaslProcessor(ThriftServerType serverType, TProcessor processor) {
    checkArgument(ThriftServerType.SASL == serverType);

    // Wrap the provided processor in our special processor which proxies the provided UGI on the logged-in UGI
    // Important that we have Timed -> UGIAssuming -> [provided] to make sure that the metrics are still reported
    // as the logged-in user.
    log.info("Wrapping {} in UGIAssumingProcessor", processor.getClass());

    return new UGIAssumingProcessor(processor);
  }
}

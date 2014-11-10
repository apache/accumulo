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
package org.apache.accumulo.core.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

public class ThriftUtil {
  private static final Logger log = Logger.getLogger(ThriftUtil.class);

  public static class TraceProtocol extends TCompactProtocol {

    @Override
    public void writeMessageBegin(TMessage message) throws TException {
      Trace.start("client:" + message.name);
      super.writeMessageBegin(message);
    }

    @Override
    public void writeMessageEnd() throws TException {
      super.writeMessageEnd();
      Span currentTrace = Trace.currentTrace();
      if (currentTrace != null)
        currentTrace.stop();
    }

    public TraceProtocol(TTransport transport) {
      super(transport);
    }
  }

  public static class TraceProtocolFactory extends TCompactProtocol.Factory {
    private static final long serialVersionUID = 1L;

    @Override
    public TProtocol getProtocol(TTransport trans) {
      return new TraceProtocol(trans);
    }
  }

  static private TProtocolFactory protocolFactory = new TraceProtocolFactory();
  static private TTransportFactory transportFactory = new TFramedTransport.Factory(Integer.MAX_VALUE);

  static public <T extends TServiceClient> T createClient(TServiceClientFactory<T> factory, TTransport transport) {
    return factory.getClient(protocolFactory.getProtocol(transport), protocolFactory.getProtocol(transport));
  }

  static public <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, HostAndPort address, AccumuloConfiguration conf)
      throws TTransportException {
    return createClient(factory, ThriftTransportPool.getInstance().getTransportWithDefaultTimeout(address, conf));
  }

  static public <T extends TServiceClient> T getClientNoTimeout(TServiceClientFactory<T> factory, String address, AccumuloConfiguration configuration)
      throws TTransportException {
    return getClient(factory, address, 0, configuration);
  }

  static public <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, String address, Property timeoutProperty,
      AccumuloConfiguration configuration) throws TTransportException {
    long timeout = configuration.getTimeInMillis(timeoutProperty);
    TTransport transport = ThriftTransportPool.getInstance().getTransport(address, timeout, SslConnectionParams.forClient(configuration));
    return createClient(factory, transport);
  }

  static public <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, String address, long timeout, AccumuloConfiguration configuration)
      throws TTransportException {
    TTransport transport = ThriftTransportPool.getInstance().getTransport(address, timeout, SslConnectionParams.forClient(configuration));
    return createClient(factory, transport);
  }

  static public void returnClient(TServiceClient iface) { // Eew... the typing here is horrible
    if (iface != null) {
      ThriftTransportPool.getInstance().returnTransport(iface.getInputProtocol().getTransport());
    }
  }

  static public TabletClientService.Client getTServerClient(String address, AccumuloConfiguration conf) throws TTransportException {
    return getClient(new TabletClientService.Client.Factory(), address, Property.GENERAL_RPC_TIMEOUT, conf);
  }

  static public TabletClientService.Client getTServerClient(String address, AccumuloConfiguration conf, long timeout) throws TTransportException {
    return getClient(new TabletClientService.Client.Factory(), address, timeout, conf);
  }

  public static void execute(String address, AccumuloConfiguration conf, ClientExec<TabletClientService.Client> exec) throws AccumuloException,
      AccumuloSecurityException {
    while (true) {
      TabletClientService.Client client = null;
      try {
        exec.execute(client = getTServerClient(address, conf));
        break;
      } catch (TTransportException tte) {
        log.debug("getTServerClient request failed, retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          returnClient(client);
      }
    }
  }

  public static <T> T execute(String address, AccumuloConfiguration conf, ClientExecReturn<T,TabletClientService.Client> exec) throws AccumuloException,
      AccumuloSecurityException {
    while (true) {
      TabletClientService.Client client = null;
      try {
        return exec.execute(client = getTServerClient(address, conf));
      } catch (TTransportException tte) {
        log.debug("getTServerClient request failed, retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          returnClient(client);
      }
    }
  }

  /**
   * create a transport that is not pooled
   */
  public static TTransport createTransport(HostAndPort address, AccumuloConfiguration conf) throws TException {
    return createClientTransport(address, (int) conf.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT), SslConnectionParams.forClient(conf));
  }

  public static TTransportFactory transportFactory() {
    return transportFactory;
  }

  private final static Map<Integer,TTransportFactory> factoryCache = new HashMap<Integer,TTransportFactory>();

  synchronized public static TTransportFactory transportFactory(int maxFrameSize) {
    TTransportFactory factory = factoryCache.get(maxFrameSize);
    if (factory == null) {
      factory = new TFramedTransport.Factory(maxFrameSize);
      factoryCache.put(maxFrameSize, factory);
    }
    return factory;
  }

  synchronized public static TTransportFactory transportFactory(long maxFrameSize) {
    if (maxFrameSize > Integer.MAX_VALUE || maxFrameSize < 1)
      throw new RuntimeException("Thrift transport frames are limited to " + Integer.MAX_VALUE);
    return transportFactory((int) maxFrameSize);
  }

  public static TProtocolFactory protocolFactory() {
    return protocolFactory;
  }

  public static TServerSocket getServerSocket(int port, int timeout, InetAddress address, SslConnectionParams params) throws TTransportException {
    TServerSocket tServerSock;
    if (params.useJsse()) {
      tServerSock = TSSLTransportFactory.getServerSocket(port, timeout, params.isClientAuth(), address);
    } else {
      tServerSock = TSSLTransportFactory.getServerSocket(port, timeout, address, params.getTTransportParams());
    }

    ServerSocket serverSock = tServerSock.getServerSocket();
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

  public static TTransport createClientTransport(HostAndPort address, int timeout, SslConnectionParams sslParams) throws TTransportException {
    boolean success = false;
    TTransport transport = null;
    try {
      if (sslParams != null) {
        // TSSLTransportFactory handles timeout 0 -> forever natively
        if (sslParams.useJsse()) {
          transport = TSSLTransportFactory.getClientSocket(address.getHostText(), address.getPort(), timeout);
        } else {
          // JDK6's factory doesn't appear to pass the protocol onto the Socket properly so we have
          // to do some magic to make sure that happens. Not an issue in JDK7

          // Taken from thrift-0.9.1 to make the SSLContext
          SSLContext sslContext = createSSLContext(sslParams);

          // Create the factory from it
          SSLSocketFactory sslSockFactory = sslContext.getSocketFactory();

          // Wrap the real factory with our own that will set the protocol on the Socket before returning it
          ProtocolOverridingSSLSocketFactory wrappingSslSockFactory = new ProtocolOverridingSSLSocketFactory(sslSockFactory,
              new String[] {sslParams.getClientProtocol()});

          // Create the TSocket from that
          transport = createClient(wrappingSslSockFactory, address.getHostText(), address.getPort(), timeout);
        }
        // TSSLTransportFactory leaves transports open, so no need to open here
      } else if (timeout == 0) {
        transport = new TSocket(address.getHostText(), address.getPort());
        transport.open();
      } else {
        try {
          transport = TTimeoutTransport.create(address, timeout);
        } catch (IOException ex) {
          throw new TTransportException(ex);
        }
        transport.open();
      }
      transport = ThriftUtil.transportFactory().getTransport(transport);
      success = true;
    } finally {
      if (!success && transport != null) {
        transport.close();
      }
    }
    return transport;
  }

  /**
   * Lifted from TSSLTransportFactory in Thrift-0.9.1. The method to create a client socket with an SSLContextFactory object is not visibile to us. Have to use
   * SslConnectionParams instead of TSSLTransportParameters because no getters exist on TSSLTransportParameters.
   *
   * @param params
   *          Parameters to use to create the SSLContext
   */
  private static SSLContext createSSLContext(SslConnectionParams params) throws TTransportException {
    SSLContext ctx;
    try {
      ctx = SSLContext.getInstance(params.getClientProtocol());
      TrustManagerFactory tmf = null;
      KeyManagerFactory kmf = null;

      if (params.isTrustStoreSet()) {
        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore ts = KeyStore.getInstance(params.getTrustStoreType());
        ts.load(new FileInputStream(params.getTrustStorePath()), params.getTrustStorePass().toCharArray());
        tmf.init(ts);
      }

      if (params.isKeyStoreSet()) {
        kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance(params.getKeyStoreType());
        ks.load(new FileInputStream(params.getKeyStorePath()), params.getKeyStorePass().toCharArray());
        kmf.init(ks, params.getKeyStorePass().toCharArray());
      }

      if (params.isKeyStoreSet() && params.isTrustStoreSet()) {
        ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
      } else if (params.isKeyStoreSet()) {
        ctx.init(kmf.getKeyManagers(), null, null);
      } else {
        ctx.init(null, tmf.getTrustManagers(), null);
      }

    } catch (Exception e) {
      throw new TTransportException("Error creating the transport", e);
    }
    return ctx;
  }

  /**
   * Lifted from Thrift-0.9.1 because it was private. Create an SSLSocket with the given factory, host:port, and timeout.
   *
   * @param factory
   *          Factory to create the socket from
   * @param host
   *          Destination host
   * @param port
   *          Destination port
   * @param timeout
   *          Socket timeout
   */
  private static TSocket createClient(SSLSocketFactory factory, String host, int port, int timeout) throws TTransportException {
    try {
      SSLSocket socket = (SSLSocket) factory.createSocket(host, port);
      socket.setSoTimeout(timeout);
      return new TSocket(socket);
    } catch (Exception e) {
      throw new TTransportException("Could not connect to " + host + " on port " + port, e);
    }
  }

  /**
   * JDK6's SSLSocketFactory doesn't seem to properly set the protocols on the Sockets that it creates which causes an SSLv2 client hello message during
   * handshake, even when only TLSv1 is enabled. This only appears to be an issue on the client sockets, not the server sockets.
   *
   * This class wraps the SSLSocketFactory ensuring that the Socket is properly configured.
   * http://www.coderanch.com/t/637177/Security/Disabling-handshake-message-Java
   *
   * This class can be removed when JDK6 support is officially unsupported by Accumulo
   */
  private static class ProtocolOverridingSSLSocketFactory extends SSLSocketFactory {

    private final SSLSocketFactory delegate;
    private final String[] enabledProtocols;

    public ProtocolOverridingSSLSocketFactory(final SSLSocketFactory delegate, final String[] enabledProtocols) {
      Preconditions.checkNotNull(enabledProtocols);
      Preconditions.checkArgument(0 != enabledProtocols.length, "Expected at least one protocol");
      this.delegate = delegate;
      this.enabledProtocols = enabledProtocols;
    }

    @Override
    public String[] getDefaultCipherSuites() {
      return delegate.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return delegate.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket(final Socket socket, final String host, final int port, final boolean autoClose) throws IOException {
      final Socket underlyingSocket = delegate.createSocket(socket, host, port, autoClose);
      return overrideProtocol(underlyingSocket);
    }

    @Override
    public Socket createSocket(final String host, final int port) throws IOException, UnknownHostException {
      final Socket underlyingSocket = delegate.createSocket(host, port);
      return overrideProtocol(underlyingSocket);
    }

    @Override
    public Socket createSocket(final String host, final int port, final InetAddress localAddress, final int localPort) throws IOException, UnknownHostException {
      final Socket underlyingSocket = delegate.createSocket(host, port, localAddress, localPort);
      return overrideProtocol(underlyingSocket);
    }

    @Override
    public Socket createSocket(final InetAddress host, final int port) throws IOException {
      final Socket underlyingSocket = delegate.createSocket(host, port);
      return overrideProtocol(underlyingSocket);
    }

    @Override
    public Socket createSocket(final InetAddress host, final int port, final InetAddress localAddress, final int localPort) throws IOException {
      final Socket underlyingSocket = delegate.createSocket(host, port, localAddress, localPort);
      return overrideProtocol(underlyingSocket);
    }

    /**
     * Set the {@link javax.net.ssl.SSLSocket#getEnabledProtocols() enabled protocols} to {@link #enabledProtocols} if the <code>socket</code> is a
     * {@link SSLSocket}
     *
     * @param socket
     *          The Socket
     */
    private Socket overrideProtocol(final Socket socket) {
      if (socket instanceof SSLSocket) {
        ((SSLSocket) socket).setEnabledProtocols(enabledProtocols);
      }
      return socket;
    }
  }
}

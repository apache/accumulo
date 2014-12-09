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
package org.apache.accumulo.core.rpc;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

/**
 * Factory methods for creating Thrift client objects
 */
public class ThriftUtil {
  private static final Logger log = LoggerFactory.getLogger(ThriftUtil.class);

  private static final TraceProtocolFactory protocolFactory = new TraceProtocolFactory();
  private static final TFramedTransport.Factory transportFactory = new TFramedTransport.Factory(Integer.MAX_VALUE);
  private static final Map<Integer,TTransportFactory> factoryCache = new HashMap<Integer,TTransportFactory>();

  public static final String GSSAPI = "GSSAPI";

  /**
   * An instance of {@link TraceProtocolFactory}
   *
   * @return The default Thrift TProtocolFactory for RPC
   */
  public static TProtocolFactory protocolFactory() {
    return protocolFactory;
  }

  /**
   * An instance of {@link org.apache.thrift.transport.TFramedTransport.Factory}
   *
   * @return The default Thrift TTransportFactory for RPC
   */
  public static TTransportFactory transportFactory() {
    return transportFactory;
  }

  /**
   * Create a Thrift client using the given factory and transport
   */
  public static <T extends TServiceClient> T createClient(TServiceClientFactory<T> factory, TTransport transport) {
    return factory.getClient(protocolFactory.getProtocol(transport), protocolFactory.getProtocol(transport));
  }

  /**
   * Create a Thrift client using the given factory with a pooled transport (if available), the address, and client context with no timeout.
   *
   * @param factory
   *          Thrift client factory
   * @param address
   *          Server address for client to connect to
   * @param context
   *          RPC options
   */
  public static <T extends TServiceClient> T getClientNoTimeout(TServiceClientFactory<T> factory, HostAndPort address, ClientContext context)
      throws TTransportException {
    return getClient(factory, address, context, 0);
  }

  /**
   * Create a Thrift client using the given factory with a pooled transport (if available), the address and client context. Client timeout is extracted from the
   * ClientContext
   *
   * @param factory
   *          Thrift client factory
   * @param address
   *          Server address for client to connect to
   * @param context
   *          RPC options
   */
  public static <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, HostAndPort address, ClientContext context) throws TTransportException {
    TTransport transport = ThriftTransportPool.getInstance().getTransport(address, context.getClientTimeoutInMillis(), context);
    return createClient(factory, transport);
  }

  /**
   * Create a Thrift client using the given factory with a pooled transport (if available) using the address, client context and timeou
   *
   * @param factory
   *          Thrift client factory
   * @param address
   *          Server address for client to connect to
   * @param context
   *          RPC options
   * @param timeout
   *          Socket timeout which overrides the ClientContext timeout
   */
  private static <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, HostAndPort address, ClientContext context, long timeout)
      throws TTransportException {
    TTransport transport = ThriftTransportPool.getInstance().getTransport(address, timeout, context);
    return createClient(factory, transport);
  }

  /**
   * Return the transport used by the client to the shared pool.
   *
   * @param iface
   *          The Client being returned or null.
   */
  public static void returnClient(TServiceClient iface) { // Eew... the typing here is horrible
    if (iface != null) {
      ThriftTransportPool.getInstance().returnTransport(iface.getInputProtocol().getTransport());
    }
  }

  /**
   * Create a TabletServer Thrift client
   *
   * @param address
   *          Server address for client to connect to
   * @param context
   *          RPC options
   */
  public static TabletClientService.Client getTServerClient(HostAndPort address, ClientContext context) throws TTransportException {
    return getClient(new TabletClientService.Client.Factory(), address, context);
  }

  /**
   * Create a TabletServer Thrift client
   *
   * @param address
   *          Server address for client to connect to
   * @param context
   *          Options for connecting to the server
   * @param timeout
   *          Socket timeout which overrides the ClientContext timeout
   */
  public static TabletClientService.Client getTServerClient(HostAndPort address, ClientContext context, long timeout) throws TTransportException {
    return getClient(new TabletClientService.Client.Factory(), address, context, timeout);
  }

  /**
   * Execute the provided closure against a TabletServer at the given address. If a Thrift transport exception occurs, the operation will be automatically
   * retried.
   *
   * @param address
   *          TabletServer address
   * @param context
   *          RPC options
   * @param exec
   *          The closure to execute
   */
  public static void execute(HostAndPort address, ClientContext context, ClientExec<TabletClientService.Client> exec) throws AccumuloException,
      AccumuloSecurityException {
    while (true) {
      TabletClientService.Client client = null;
      try {
        exec.execute(client = getTServerClient(address, context));
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

  /**
   * Execute the provided closure against the TabletServer at the given address, and return the result of the closure to the client. If a Thrift transport
   * exception occurs, the operation will be automatically retried.
   *
   * @param address
   *          TabletServer address
   * @param context
   *          RPC options
   * @param exec
   *          Closure with a return value to execute
   * @return The result from the closure
   */
  public static <T> T execute(HostAndPort address, ClientContext context, ClientExecReturn<T,TabletClientService.Client> exec) throws AccumuloException,
      AccumuloSecurityException {
    while (true) {
      TabletClientService.Client client = null;
      try {
        return exec.execute(client = getTServerClient(address, context));
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
   * Create a transport that is not pooled
   *
   * @param address
   *          Server address to open the transport to
   * @param context
   *          RPC options
   */
  public static TTransport createTransport(HostAndPort address, ClientContext context) throws TException {
    return createClientTransport(address, (int) context.getClientTimeoutInMillis(), context.getClientSslParams(), context.getClientSaslParams());
  }

  /**
   * Get an instance of the TTransportFactory with the provided maximum frame size
   *
   * @param maxFrameSize
   *          Maximum Thrift message frame size
   * @return A, possibly cached, TTransportFactory with the requested maximum frame size
   */
  public static synchronized TTransportFactory transportFactory(int maxFrameSize) {
    TTransportFactory factory = factoryCache.get(maxFrameSize);
    if (factory == null) {
      factory = new TFramedTransport.Factory(maxFrameSize);
      factoryCache.put(maxFrameSize, factory);
    }
    return factory;
  }

  /**
   * @see #transportFactory(int)
   */
  public static synchronized TTransportFactory transportFactory(long maxFrameSize) {
    if (maxFrameSize > Integer.MAX_VALUE || maxFrameSize < 1)
      throw new RuntimeException("Thrift transport frames are limited to " + Integer.MAX_VALUE);
    return transportFactory((int) maxFrameSize);
  }

  /**
   * Create a TTransport for clients to the given address with the provided socket timeout and session-layer configuration
   *
   * @param address
   *          Server address to connect to
   * @param timeout
   *          Client socket timeout
   * @param sslParams
   *          RPC options for SSL servers
   * @param saslParams
   *          RPC options for SASL servers
   * @return An open TTransport which must be closed when finished
   */
  public static TTransport createClientTransport(HostAndPort address, int timeout, SslConnectionParams sslParams, SaslConnectionParams saslParams)
      throws TTransportException {
    boolean success = false;
    TTransport transport = null;
    try {
      if (sslParams != null) {
        // The check in AccumuloServerContext ensures that servers are brought up with sane configurations, but we also want to validate clients
        if (null != saslParams) {
          throw new IllegalStateException("Cannot use both SSL and SASL");
        }

        log.trace("Creating SSL client transport");

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
          // TSSLTransportFactory leaves transports open, so no need to open here
        }

        transport = ThriftUtil.transportFactory().getTransport(transport);
      } else if (null != saslParams) {
        if (!UserGroupInformation.isSecurityEnabled()) {
          throw new IllegalStateException("Expected Kerberos security to be enabled if SASL is in use");
        }

        log.trace("Creating SASL connection to {}:{}", address.getHostText(), address.getPort());

        transport = new TSocket(address.getHostText(), address.getPort());

        try {
          // Log in via UGI, ensures we have logged in with our KRB credentials
          final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

          // Is this pricey enough that we want to cache it?
          final String hostname = InetAddress.getByName(address.getHostText()).getCanonicalHostName();

          log.trace("Opening transport to server as {} to {}/{}", currentUser, saslParams.getKerberosServerPrimary(), hostname);

          // Create the client SASL transport using the information for the server
          // Despite the 'protocol' argument seeming to be useless, it *must* be the primary of the server being connected to
          transport = new TSaslClientTransport(GSSAPI, null, saslParams.getKerberosServerPrimary(), hostname, saslParams.getSaslProperties(), null, transport);

          // Wrap it all in a processor which will run with a doAs the current user
          transport = new UGIAssumingTransport(transport, currentUser);

          // Open the transport
          transport.open();
        } catch (IOException e) {
          log.warn("Failed to open SASL transport", e);
          throw new TTransportException(e);
        }
      } else {
        log.trace("Opening normal transport");
        if (timeout == 0) {
          transport = new TSocket(address.getHostText(), address.getPort());
          transport.open();
        } else {
          try {
            transport = TTimeoutTransport.create(address, timeout);
          } catch (IOException ex) {
            log.warn("Failed to open transport to " + address);
            throw new TTransportException(ex);
          }

          // Open the transport
          transport.open();
        }
        transport = ThriftUtil.transportFactory().getTransport(transport);
      }
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
}

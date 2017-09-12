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
import java.util.Random;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.rpc.SaslConnectionParams.SaslMechanism;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
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

/**
 * Factory methods for creating Thrift client objects
 */
public class ThriftUtil {
  private static final Logger log = LoggerFactory.getLogger(ThriftUtil.class);

  private static final TraceProtocolFactory protocolFactory = new TraceProtocolFactory();
  private static final TFramedTransport.Factory transportFactory = new TFramedTransport.Factory(Integer.MAX_VALUE);
  private static final Map<Integer,TTransportFactory> factoryCache = new HashMap<>();

  public static final String GSSAPI = "GSSAPI", DIGEST_MD5 = "DIGEST-MD5";

  private static final Random SASL_BACKOFF_RAND = new Random();
  private static final int RELOGIN_MAX_BACKOFF = 5000;

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
  public static <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, HostAndPort address, ClientContext context, long timeout)
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
   * Create a transport that is not pooled
   *
   * @param address
   *          Server address to open the transport to
   * @param context
   *          RPC options
   */
  public static TTransport createTransport(HostAndPort address, ClientContext context) throws TException {
    return createClientTransport(address, (int) context.getClientTimeoutInMillis(), context.getClientSslParams(), context.getSaslParams());
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
          transport = TSSLTransportFactory.getClientSocket(address.getHost(), address.getPort(), timeout);
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
          transport = createClient(wrappingSslSockFactory, address.getHost(), address.getPort(), timeout);
          // TSSLTransportFactory leaves transports open, so no need to open here
        }

        transport = ThriftUtil.transportFactory().getTransport(transport);
      } else if (null != saslParams) {
        if (!UserGroupInformation.isSecurityEnabled()) {
          throw new IllegalStateException("Expected Kerberos security to be enabled if SASL is in use");
        }

        log.trace("Creating SASL connection to {}:{}", address.getHost(), address.getPort());

        // Make sure a timeout is set
        try {
          transport = TTimeoutTransport.create(address, timeout);
        } catch (IOException e) {
          log.warn("Failed to open transport to {}", address);
          throw new TTransportException(e);
        }

        try {
          // Log in via UGI, ensures we have logged in with our KRB credentials
          final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
          final UserGroupInformation userForRpc;
          if (AuthenticationMethod.PROXY == currentUser.getAuthenticationMethod()) {
            // A "proxy" user is when the real (Kerberos) credentials are for a user
            // other than the one we're acting as. When we make an RPC though, we need to make sure
            // that the current user is the user that has some credentials.
            if (currentUser.getRealUser() != null) {
              userForRpc = currentUser.getRealUser();
              log.trace("{} is a proxy user, using real user instead {}", currentUser, userForRpc);
            } else {
              // The current user has no credentials, let it fail naturally at the RPC layer (no ticket)
              // We know this won't work, but we can't do anything else
              log.warn("The current user is a proxy user but there is no underlying real user (likely that RPCs will fail): {}", currentUser);
              userForRpc = currentUser;
            }
          } else {
            // The normal case: the current user has its own ticket
            userForRpc = currentUser;
          }

          // Is this pricey enough that we want to cache it?
          final String hostname = InetAddress.getByName(address.getHost()).getCanonicalHostName();

          final SaslMechanism mechanism = saslParams.getMechanism();

          log.trace("Opening transport to server as {} to {}/{} using {}", userForRpc, saslParams.getKerberosServerPrimary(), hostname, mechanism);

          // Create the client SASL transport using the information for the server
          // Despite the 'protocol' argument seeming to be useless, it *must* be the primary of the server being connected to
          transport = new TSaslClientTransport(mechanism.getMechanismName(), null, saslParams.getKerberosServerPrimary(), hostname,
              saslParams.getSaslProperties(), saslParams.getCallbackHandler(), transport);

          // Wrap it all in a processor which will run with a doAs the current user
          transport = new UGIAssumingTransport(transport, userForRpc);

          // Open the transport
          transport.open();
        } catch (TTransportException e) {
          log.warn("Failed to open SASL transport", e);

          // We might have had a valid ticket, but it expired. We'll let the caller retry, but we will attempt to re-login to make the next attempt work.
          // Sadly, we have no way to determine the actual reason we got this TTransportException other than inspecting the exception msg.
          log.debug("Caught TTransportException opening SASL transport, checking if re-login is necessary before propagating the exception.");
          attemptClientReLogin();

          throw e;
        } catch (IOException e) {
          log.warn("Failed to open SASL transport", e);
          throw new TTransportException(e);
        }
      } else {
        log.trace("Opening normal transport");
        if (timeout == 0) {
          transport = new TSocket(address.getHost(), address.getPort());
          transport.open();
        } else {
          try {
            transport = TTimeoutTransport.create(address, timeout);
          } catch (IOException ex) {
            log.warn("Failed to open transport to {}", address);
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
   * Some wonderful snippets of documentation from HBase on performing the re-login client-side (as well as server-side) in the following paragraph. We want to
   * attempt a re-login to automatically refresh the client's Krb "credentials" (remember, a server might also be a client, master sending RPC to tserver), but
   * we have to take care to avoid Kerberos' replay attack protection.
   * <p>
   * If multiple clients with the same principal try to connect to the same server at the same time, the server assumes a replay attack is in progress. This is
   * a feature of kerberos. In order to work around this, what is done is that the client backs off randomly and tries to initiate the connection again. The
   * other problem is to do with ticket expiry. To handle that, a relogin is attempted.
   */
  static void attemptClientReLogin() {
    try {
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      if (null == loginUser || !loginUser.hasKerberosCredentials()) {
        // We should have already checked that we're logged in and have credentials. A precondition-like check.
        throw new RuntimeException("Expected to find Kerberos UGI credentials, but did not");
      }
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      // A Proxy user is the "effective user" (in name only), riding on top of the "real user"'s Krb credentials.
      UserGroupInformation realUser = currentUser.getRealUser();

      // re-login only in case it is the login user or superuser.
      if (loginUser.equals(currentUser) || loginUser.equals(realUser)) {
        if (UserGroupInformation.isLoginKeytabBased()) {
          log.info("Performing keytab-based Kerberos re-login");
          loginUser.reloginFromKeytab();
        } else {
          log.info("Performing ticket-cache-based Kerberos re-login");
          loginUser.reloginFromTicketCache();
        }

        // Avoid the replay attack protection, sleep 1 to 5000ms
        try {
          Thread.sleep((SASL_BACKOFF_RAND.nextInt(RELOGIN_MAX_BACKOFF) + 1));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      } else {
        log.debug("Not attempting Kerberos re-login: loginUser={}, currentUser={}, realUser={}", loginUser, currentUser, realUser);
      }
    } catch (IOException e) {
      // The inability to check is worrisome and deserves a RuntimeException instead of a propagated IO-like Exception.
      log.warn("Failed to check (and/or perform) Kerberos client re-login", e);
      throw new RuntimeException(e);
    }
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
        try (FileInputStream fis = new FileInputStream(params.getTrustStorePath())) {
          ts.load(fis, params.getTrustStorePass().toCharArray());
        }
        tmf.init(ts);
      }

      if (params.isKeyStoreSet()) {
        kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance(params.getKeyStoreType());
        try (FileInputStream fis = new FileInputStream(params.getKeyStorePath())) {
          ks.load(fis, params.getKeyStorePass().toCharArray());
        }
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
    SSLSocket socket = null;
    try {
      socket = (SSLSocket) factory.createSocket(host, port);
      socket.setSoTimeout(timeout);
      return new TSocket(socket);
    } catch (Exception e) {
      try {
        if (socket != null)
          socket.close();
      } catch (IOException ioe) {}

      throw new TTransportException("Could not connect to " + host + " on port " + port, e);
    }
  }
}

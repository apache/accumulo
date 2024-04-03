/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.rpc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.channels.ClosedByInterruptException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.rpc.SaslConnectionParams.SaslMechanism;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TProtocolFactory;
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
  private static final AccumuloTFramedTransportFactory transportFactory =
      new AccumuloTFramedTransportFactory(Integer.MAX_VALUE);
  private static final Map<Integer,TTransportFactory> factoryCache = new HashMap<>();

  public static final String GSSAPI = "GSSAPI", DIGEST_MD5 = "DIGEST-MD5";

  private static final SecureRandom random = new SecureRandom();
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
   * An instance of {@link org.apache.thrift.transport.layered.TFramedTransport.Factory}
   *
   * @return The default Thrift TTransportFactory for RPC
   */
  public static TTransportFactory transportFactory() {
    return transportFactory;
  }

  /**
   * Create a Thrift client using the given factory and transport
   */
  public static <T extends TServiceClient> T createClient(ThriftClientTypes<T> type,
      TTransport transport) {
    return type.getClient(protocolFactory.getProtocol(transport));
  }

  /**
   * Create a Thrift client using the given factory with a pooled transport (if available), the
   * address, and client context with no timeout.
   *
   * @param type Thrift client type
   * @param address Server address for client to connect to
   * @param context RPC options
   */
  public static <T extends TServiceClient> T getClientNoTimeout(ThriftClientTypes<T> type,
      HostAndPort address, ClientContext context) throws TTransportException {
    return getClient(type, address, context, 0);
  }

  /**
   * Create a Thrift client using the given factory with a pooled transport (if available), the
   * address and client context. Client timeout is extracted from the ClientContext
   *
   * @param type Thrift client type
   * @param address Server address for client to connect to
   * @param context RPC options
   */
  public static <T extends TServiceClient> T getClient(ThriftClientTypes<T> type,
      HostAndPort address, ClientContext context) throws TTransportException {
    TTransport transport = context.getTransportPool().getTransport(type, address,
        context.getClientTimeoutInMillis(), context, true);
    return createClient(type, transport);
  }

  /**
   * Create a Thrift client using the given factory with a pooled transport (if available) using the
   * address, client context and timeout
   *
   * @param type Thrift client type
   * @param address Server address for client to connect to
   * @param context RPC options
   * @param timeout Socket timeout which overrides the ClientContext timeout
   */
  public static <T extends TServiceClient> T getClient(ThriftClientTypes<T> type,
      HostAndPort address, ClientContext context, long timeout) throws TTransportException {
    TTransport transport =
        context.getTransportPool().getTransport(type, address, timeout, context, true);
    return createClient(type, transport);
  }

  public static void close(TServiceClient client, ClientContext context) {
    if (client != null && client.getInputProtocol() != null
        && client.getInputProtocol().getTransport() != null) {
      context.getTransportPool().returnTransport(client.getInputProtocol().getTransport());
    } else {
      log.debug("Attempt to close null connection to a server", new Exception());
    }
  }

  /**
   * Return the transport used by the client to the shared pool.
   *
   * @param iface The Client being returned or null.
   */
  public static void returnClient(TServiceClient iface, ClientContext context) {
    if (iface != null) {
      context.getTransportPool().returnTransport(iface.getInputProtocol().getTransport());
    }
  }

  /**
   * Create a transport that is not pooled
   *
   * @param address Server address to open the transport to
   * @param context RPC options
   */
  public static TTransport createTransport(HostAndPort address, ClientContext context)
      throws TException {
    return createClientTransport(address, (int) context.getClientTimeoutInMillis(),
        context.getClientSslParams(), context.getSaslParams());
  }

  /**
   * Get an instance of the TTransportFactory with the provided maximum frame size
   *
   * @param maxFrameSize Maximum Thrift message frame size
   * @return A, possibly cached, TTransportFactory with the requested maximum frame size
   */
  public static synchronized TTransportFactory transportFactory(long maxFrameSize) {
    if (maxFrameSize > Integer.MAX_VALUE || maxFrameSize < 1) {
      throw new RuntimeException("Thrift transport frames are limited to " + Integer.MAX_VALUE);
    }
    int maxFrameSize1 = (int) maxFrameSize;
    TTransportFactory factory = factoryCache.get(maxFrameSize1);
    if (factory == null) {
      factory = new AccumuloTFramedTransportFactory(maxFrameSize1);
      factoryCache.put(maxFrameSize1, factory);
    }
    return factory;
  }

  /**
   * Create a TTransport for clients to the given address with the provided socket timeout and
   * session-layer configuration
   *
   * @param address Server address to connect to
   * @param timeout Client socket timeout
   * @param sslParams RPC options for SSL servers
   * @param saslParams RPC options for SASL servers
   * @return An open TTransport which must be closed when finished
   */
  public static TTransport createClientTransport(HostAndPort address, int timeout,
      SslConnectionParams sslParams, SaslConnectionParams saslParams) throws TTransportException {
    boolean success = false;
    TTransport transport = null;
    try {
      if (sslParams != null) {
        // The check in AccumuloServerContext ensures that servers are brought up with sane
        // configurations, but we also want to validate clients
        if (saslParams != null) {
          throw new IllegalStateException("Cannot use both SSL and SASL");
        }

        log.trace("Creating SSL client transport");

        // TSSLTransportFactory handles timeout 0 -> forever natively
        if (sslParams.useJsse()) {
          transport =
              TSSLTransportFactory.getClientSocket(address.getHost(), address.getPort(), timeout);
        } else {
          transport = TSSLTransportFactory.getClientSocket(address.getHost(), address.getPort(),
              timeout, sslParams.getTSSLTransportParameters());
        }
        // TSSLTransportFactory leaves transports open, so no need to open here

        transport = ThriftUtil.transportFactory().getTransport(transport);
      } else if (saslParams != null) {
        if (!UserGroupInformation.isSecurityEnabled()) {
          throw new IllegalStateException(
              "Expected Kerberos security to be enabled if SASL is in use");
        }

        log.trace("Creating SASL connection to {}:{}", address.getHost(), address.getPort());

        // Make sure a timeout is set
        try {
          transport = TTimeoutTransport.create(address, timeout);
        } catch (TTransportException e) {
          log.warn("Failed to open transport to {}", address);
          throw e;
        }

        try {
          // Log in via UGI, ensures we have logged in with our KRB credentials
          final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
          final UserGroupInformation userForRpc;
          if (currentUser.getAuthenticationMethod() == AuthenticationMethod.PROXY) {
            // A "proxy" user is when the real (Kerberos) credentials are for a user
            // other than the one we're acting as. When we make an RPC though, we need to make sure
            // that the current user is the user that has some credentials.
            if (currentUser.getRealUser() != null) {
              userForRpc = currentUser.getRealUser();
              log.trace("{} is a proxy user, using real user instead {}", currentUser, userForRpc);
            } else {
              // The current user has no credentials, let it fail naturally at the RPC layer (no
              // ticket)
              // We know this won't work, but we can't do anything else
              log.warn("The current user is a proxy user but there is no"
                  + " underlying real user (likely that RPCs will fail): {}", currentUser);
              userForRpc = currentUser;
            }
          } else {
            // The normal case: the current user has its own ticket
            userForRpc = currentUser;
          }

          // Is this pricey enough that we want to cache it?
          final String hostname = InetAddress.getByName(address.getHost()).getCanonicalHostName();

          final SaslMechanism mechanism = saslParams.getMechanism();

          log.trace("Opening transport to server as {} to {}/{} using {}", userForRpc,
              saslParams.getKerberosServerPrimary(), hostname, mechanism);

          // Create the client SASL transport using the information for the server
          // Despite the 'protocol' argument seeming to be useless, it *must* be the primary of the
          // server being connected to
          transport = new TSaslClientTransport(mechanism.getMechanismName(), null,
              saslParams.getKerberosServerPrimary(), hostname, saslParams.getSaslProperties(),
              saslParams.getCallbackHandler(), transport);

          // Wrap it all in a processor which will run with a doAs the current user
          transport = new UGIAssumingTransport(transport, userForRpc);

          // Open the transport
          transport.open();
        } catch (TTransportException e) {
          log.warn("Failed to open SASL transport", e);

          // We might have had a valid ticket, but it expired. We'll let the caller retry, but we
          // will attempt to re-login to make the next attempt work.
          // Sadly, we have no way to determine the actual reason we got this TTransportException
          // other than inspecting the exception msg.
          log.debug("Caught TTransportException opening SASL transport,"
              + " checking if re-login is necessary before propagating the exception.");
          attemptClientReLogin();

          throw e;
        } catch (IOException e) {
          log.warn("Failed to open SASL transport", e);
          ThriftUtil.checkIOExceptionCause(e);
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
          } catch (TTransportException ex) {
            log.warn("Failed to open transport to {}", address);
            throw ex;
          }

          // Open the transport
          transport.open();
        }
        transport = ThriftUtil.transportFactory().getTransport(transport);
      }
      success = true;
      return transport;
    } finally {
      if (!success && transport != null) {
        transport.close();
      }
    }
  }

  /**
   * Some wonderful snippets of documentation from HBase on performing the re-login client-side (as
   * well as server-side) in the following paragraph. We want to attempt a re-login to automatically
   * refresh the client's Krb "credentials" (remember, a server might also be a client, manager
   * sending RPC to tserver), but we have to take care to avoid Kerberos' replay attack protection.
   * <p>
   * If multiple clients with the same principal try to connect to the same server at the same time,
   * the server assumes a replay attack is in progress. This is a feature of kerberos. In order to
   * work around this, what is done is that the client backs off randomly and tries to initiate the
   * connection again. The other problem is to do with ticket expiry. To handle that, a relogin is
   * attempted.
   */
  private static void attemptClientReLogin() {
    try {
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      if (loginUser == null || !loginUser.hasKerberosCredentials()) {
        // We should have already checked that we're logged in and have credentials. A
        // precondition-like check.
        throw new RuntimeException("Expected to find Kerberos UGI credentials, but did not");
      }
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      // A Proxy user is the "effective user" (in name only), riding on top of the "real user"'s Krb
      // credentials.
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
          Thread.sleep(random.nextInt(RELOGIN_MAX_BACKOFF) + 1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      } else {
        log.debug("Not attempting Kerberos re-login: loginUser={}, currentUser={}, realUser={}",
            loginUser, currentUser, realUser);
      }
    } catch (IOException e) {
      // The inability to check is worrisome and deserves a RuntimeException instead of a propagated
      // IO-like Exception.
      log.warn("Failed to check (and/or perform) Kerberos client re-login", e);
      throw new RuntimeException(e);
    }
  }

  public static void checkIOExceptionCause(IOException e) {
    if (e instanceof ClosedByInterruptException) {
      Thread.currentThread().interrupt();
      throw new UncheckedIOException(e);
    }
  }
}

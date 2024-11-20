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
package org.apache.accumulo.manager.http;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.List;

import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.security.SystemCredentials.SystemToken;
import org.eclipse.jetty.security.AbstractLoginService;
import org.eclipse.jetty.security.ConfigurableSpnegoLoginService;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.RolePrincipal;
import org.eclipse.jetty.security.UserPrincipal;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.security.authentication.AuthorizationService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.security.authentication.ConfigurableSpnegoAuthenticator;
import org.eclipse.jetty.server.AbstractConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Credential;
import org.eclipse.jetty.util.security.Password;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

// TODO: I couldn't figure out why spotbugs is giving this error, it needs to be investigated more
@SuppressFBWarnings(value = "SE_BAD_FIELD",
    justification = "This seems to be serialization related and possibly a false positive")
public class EmbeddedRpcWebServer {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedRpcWebServer.class);

  private final Server server;
  private final ServerConnector connector;
  private final ServletContextHandler handler;
  private final boolean ssl;

  public EmbeddedRpcWebServer(Manager manager, int port) {
    server = new Server();
    final ServerContext context = manager.getContext();
    ssl = context.getThriftServerType() == ThriftServerType.SSL;

    ConstraintSecurityHandler securityHandler = null;
    if (context.getThriftServerType() == ThriftServerType.SASL) {
      // TODO: This is set up so it works with the miniaccumulo cluster test KRB
      // We would need to make this a bit more configurable for a real system
      // but is fine for the proof of concept
      String realm = "EXAMPLE.com";
      HashLoginService authorizationService = new HashLoginService(realm);
      UserStore userStore = new UserStore();
      userStore.addUser(
          context.getConfiguration().get(Property.GENERAL_KERBEROS_PRINCIPAL).split("@")[0],
          new Password(""), List.of("system").toArray(new String[] {}));
      authorizationService.setUserStore(userStore);
      ConfigurableSpnegoLoginService loginService = new ConfigurableSpnegoLoginService(realm,
          AuthorizationService.from(authorizationService, ""));
      loginService.addBean(authorizationService);
      loginService.setKeyTabPath(
          Path.of(context.getConfiguration().getPath(Property.GENERAL_KERBEROS_KEYTAB)));
      loginService.setServiceName("accumulo");
      try {
        loginService.setHostName(InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
      server.addBean(loginService);

      securityHandler = new ConstraintSecurityHandler();
      securityHandler.addConstraintMapping(getConstraintMapping("system"));
      ConfigurableSpnegoAuthenticator authenticator = new ConfigurableSpnegoAuthenticator();
      securityHandler.setAuthenticator(authenticator);
      securityHandler.setLoginService(loginService);
    } else if (!ssl) {
      securityHandler = new ConstraintSecurityHandler();
      securityHandler.addConstraintMapping(getConstraintMapping("system"));
      securityHandler.setAuthenticator(new BasicAuthenticator());
      securityHandler.setLoginService(new AbstractLoginService() {
        @Override
        public String getName() {
          return "accumulo";
        }

        @Override
        protected List<RolePrincipal> loadRoleInfo(UserPrincipal user) {
          // TODO: check credentials if can map to system or something else
          // We need to verify that the principal here is authorized for whatever
          // level access (system or user) so eventually UserPrincipal or
          // this method needs to make checks to verify a user is authorized using
          // context.getSecurityOperation().canPerformSystemActions() or
          // other security methods
          return List.of(new RolePrincipal("system"));
        }

        @Override
        protected UserPrincipal loadUserInfo(String username) {

          return new UserPrincipal(username, new Credential() {
            @Override
            public boolean check(Object credentials) {
              try {
                // Authenticate the user with the passed in serialized password
                SystemToken passwordToken = new SystemToken(((String) credentials).getBytes(UTF_8));
                var creds = new SystemCredentials(context.getInstanceID(), username, passwordToken);
                return context.getSecurityOperation().authenticateUser(context.rpcCreds(),
                    creds.toThrift(context.getInstanceID()));
              } catch (ThriftSecurityException e) {
                LOG.debug(e.getMessage());
                return false;
              }
            }
          });
        }
      });
    }

    connector = new ServerConnector(server, getConnectionFactories(context, ssl));
    connector.setHost(manager.getHostname());
    connector.setPort(port);

    handler =
        new ServletContextHandler(ServletContextHandler.SESSIONS | ServletContextHandler.SECURITY);
    if (securityHandler != null) {
      handler.setSecurityHandler(securityHandler);
    }
    handler.getSessionHandler().getSessionCookieConfig().setHttpOnly(true);
    handler.setContextPath("/");
  }

  private static AbstractConnectionFactory[] getConnectionFactories(ServerContext context,
      boolean ssl) {

    if (ssl) {
      HttpConfiguration httpConfig = new HttpConfiguration();
      SecureRequestCustomizer customizer = new SecureRequestCustomizer();
      customizer.setSniHostCheck(false);
      httpConfig.addCustomizer(customizer);
      final HttpConnectionFactory httpFactory = new HttpConnectionFactory(httpConfig);

      LOG.debug("Configuring Jetty to use TLS");
      final SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();

      SslConnectionParams sslParams = context.getServerSslParams();
      sslContextFactory.setKeyStorePath(sslParams.getKeyStorePath());
      sslContextFactory.setKeyStorePassword(sslParams.getKeyStorePass());
      sslContextFactory.setKeyStoreType(sslParams.getKeyStoreType());
      sslContextFactory.setTrustStorePath(sslParams.getTrustStorePath());
      sslContextFactory.setTrustStorePassword(sslParams.getTrustStorePass());
      sslContextFactory.setTrustStoreType(sslParams.getTrustStoreType());

      final String includedCiphers = context.getConfiguration().get(Property.RPC_SSL_CIPHER_SUITES);
      if (!Property.RPC_SSL_CIPHER_SUITES.getDefaultValue().equals(includedCiphers)) {
        sslContextFactory.setIncludeCipherSuites(includedCiphers.split(","));
      }

      final String[] includeProtocols = sslParams.getServerProtocols();
      if (includeProtocols != null && includeProtocols.length > 0) {
        sslContextFactory.setIncludeProtocols(includeProtocols);
      }

      SslConnectionFactory sslFactory =
          new SslConnectionFactory(sslContextFactory, httpFactory.getProtocol());
      return new AbstractConnectionFactory[] {sslFactory, httpFactory};
    } else {
      LOG.debug("Not configuring Jetty to use TLS");
      return new AbstractConnectionFactory[] {new HttpConnectionFactory()};
    }
  }

  public void addServlet(ServletHolder restServlet, String where) {
    handler.addServlet(restServlet, where);
  }

  public int getPort() {
    return connector.getLocalPort();
  }

  public boolean isSsl() {
    return ssl;
  }

  public void start() {
    try {
      server.addConnector(connector);
      server.setHandler(handler);
      server.start();
    } catch (Exception e) {
      stop();
      throw new RuntimeException(e);
    }
  }

  public void stop() {
    try {
      server.stop();
      server.join();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isRunning() {
    return server.isRunning();
  }

  private ConstraintMapping getConstraintMapping(String... roles) {
    Constraint constraint = new Constraint();
    constraint.setAuthenticate(true);
    constraint.setRoles(roles);
    ConstraintMapping mapping = new ConstraintMapping();
    mapping.setPathSpec("/*");
    mapping.setConstraint(constraint);
    return mapping;
  }
}

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
package org.apache.accumulo.server.rpc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.clientImpl.DelegationTokenImpl;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.rpc.SaslConnectionParams.SaslMechanism;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.server.security.SystemCredentials.SystemToken;
import org.apache.accumulo.server.security.UserImpersonation;
import org.apache.accumulo.server.security.UserImpersonation.UsersWithHosts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts the TCredentials object from the RPC argument list and asserts that the Accumulo
 * principal is equal to the Kerberos 'primary' component of the Kerberos principal (e.g. Accumulo
 * principal of "frank" equals "frank" from "frank/hostname@DOMAIN").
 */
public class TCredentialsUpdatingInvocationHandler<I> implements InvocationHandler {
  private static final Logger log =
      LoggerFactory.getLogger(TCredentialsUpdatingInvocationHandler.class);

  private static final ConcurrentHashMap<String,
      Class<? extends AuthenticationToken>> TOKEN_CLASS_CACHE = new ConcurrentHashMap<>();
  private final I instance;
  private final UserImpersonation impersonation;

  protected TCredentialsUpdatingInvocationHandler(final I serverInstance,
      AccumuloConfiguration conf) {
    instance = serverInstance;
    impersonation = new UserImpersonation(conf);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    updateArgs(args);

    return invokeMethod(method, args);
  }

  /**
   * Try to find a TCredentials object in the argument list, and, when the AuthenticationToken is a
   * KerberosToken, set the principal from the SASL server as the TCredentials principal. This
   * ensures that users can't spoof a different principal into the Credentials than what they used
   * to authenticate.
   */
  protected void updateArgs(Object[] args) throws ThriftSecurityException {
    // If we don't have at least two args
    if (args == null || args.length < 2) {
      return;
    }

    TCredentials tcreds = null;
    if (args[0] != null && args[0] instanceof TCredentials) {
      tcreds = (TCredentials) args[0];
    } else if (args[1] != null && args[1] instanceof TCredentials) {
      tcreds = (TCredentials) args[1];
    }

    // If we don't find a tcredentials in the first two positions
    if (tcreds == null) {
      // Not all calls require authentication (e.g. closeMultiScan). We need to let these pass
      // through.
      log.trace("Did not find a TCredentials object in the first two positions"
          + " of the argument list, not updating principal");
      return;
    }

    Class<? extends AuthenticationToken> tokenClass = getTokenClassFromName(tcreds.tokenClassName);

    // The Accumulo principal extracted from the SASL transport
    final String principal = UGIAssumingProcessor.rpcPrincipal();

    // If we authenticated the user over DIGEST-MD5 and they have a DelegationToken, the principals
    // should match
    if (UGIAssumingProcessor.rpcMechanism() == SaslMechanism.DIGEST_MD5
        && DelegationTokenImpl.class.isAssignableFrom(tokenClass)) {
      if (!principal.equals(tcreds.principal)) {
        log.warn("{} issued RPC with delegation token over DIGEST-MD5 as the "
            + "Accumulo principal {}. Disallowing RPC", principal, tcreds.principal);
        throw new ThriftSecurityException("RPC principal did not match provided Accumulo principal",
            SecurityErrorCode.BAD_CREDENTIALS);
      }
      return;
    }

    // If the authentication token isn't a KerberosToken
    if (!KerberosToken.class.isAssignableFrom(tokenClass)
        && !SystemToken.class.isAssignableFrom(tokenClass)) {
      // Don't include messages about SystemToken since it's internal
      log.debug("Will not update principal on authentication tokens other than"
          + " KerberosToken. Received {}", tokenClass);
      throw new ThriftSecurityException("Did not receive a valid token",
          SecurityErrorCode.BAD_CREDENTIALS);
    }

    if (principal == null) {
      log.debug(
          "Found KerberosToken in TCredentials, but did not receive principal from SASL processor");
      throw new ThriftSecurityException("Did not extract principal from Thrift SASL processor",
          SecurityErrorCode.BAD_CREDENTIALS);
    }

    // The principal from the SASL transport should match what the user requested as their Accumulo
    // principal
    if (!principal.equals(tcreds.principal)) {
      UsersWithHosts usersWithHosts = impersonation.get(principal);
      if (usersWithHosts == null) {
        principalMismatch(principal, tcreds.principal);
      }
      if (!usersWithHosts.getUsers().contains(tcreds.principal)) {
        principalMismatch(principal, tcreds.principal);
      }
      String clientAddr = TServerUtils.clientAddress.get();
      if (!usersWithHosts.getHosts().contains(clientAddr)) {
        final String msg = "Principal in credentials object allowed mismatched"
            + " Kerberos principals, but not on " + clientAddr;
        log.warn(msg);
        throw new ThriftSecurityException(msg, SecurityErrorCode.BAD_CREDENTIALS);
      }
    }
  }

  protected void principalMismatch(String expected, String actual) throws ThriftSecurityException {
    final String msg = "Principal in credentials object should match kerberos principal. Expected '"
        + expected + "' but was '" + actual + "'";
    log.warn(msg);
    throw new ThriftSecurityException(msg, SecurityErrorCode.BAD_CREDENTIALS);
  }

  protected Class<? extends AuthenticationToken> getTokenClassFromName(String tokenClassName) {
    Class<? extends AuthenticationToken> typedClz = TOKEN_CLASS_CACHE.get(tokenClassName);
    if (typedClz == null) {
      Class<?> clz;
      try {
        clz = Class.forName(tokenClassName);
      } catch (ClassNotFoundException e) {
        log.debug("Could not create class from token name: {}", tokenClassName, e);
        return null;
      }
      typedClz = clz.asSubclass(AuthenticationToken.class);
    }
    // return the current one and throw away the one we just created if some other thread created it
    // first
    Class<? extends AuthenticationToken> current =
        TOKEN_CLASS_CACHE.putIfAbsent(tokenClassName, typedClz);
    return current != null ? current : typedClz;
  }

  private Object invokeMethod(Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(instance, args);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    }
  }

  /**
   * Visibile for testing
   */
  protected ConcurrentHashMap<String,Class<? extends AuthenticationToken>> getTokenCache() {
    return TOKEN_CLASS_CACHE;
  }
}

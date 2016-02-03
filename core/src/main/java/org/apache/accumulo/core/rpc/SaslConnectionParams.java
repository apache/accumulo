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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.impl.DelegationTokenImpl;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection parameters for setting up a TSaslTransportFactory
 */
public class SaslConnectionParams {
  private static final Logger log = LoggerFactory.getLogger(SaslConnectionParams.class);

  /**
   * Enumeration around {@link Sasl#QOP}
   */
  public enum QualityOfProtection {
    AUTH("auth"), AUTH_INT("auth-int"), AUTH_CONF("auth-conf");

    private final String quality;

    private QualityOfProtection(String quality) {
      this.quality = quality;
    }

    public String getQuality() {
      return quality;
    }

    public static QualityOfProtection get(String name) {
      if (AUTH.quality.equals(name)) {
        return AUTH;
      } else if (AUTH_INT.quality.equals(name)) {
        return AUTH_INT;
      } else if (AUTH_CONF.quality.equals(name)) {
        return AUTH_CONF;
      }

      throw new IllegalArgumentException("No value for " + name);
    }

    @Override
    public String toString() {
      return quality;
    }
  }

  /**
   * The SASL mechanism to use for authentication
   */
  public enum SaslMechanism {
    GSSAPI("GSSAPI"), // Kerberos
    DIGEST_MD5("DIGEST-MD5"); // Delegation Tokens

    private final String mechanismName;

    private SaslMechanism(String mechanismName) {
      this.mechanismName = mechanismName;
    }

    public String getMechanismName() {
      return mechanismName;
    }

    public static SaslMechanism get(String mechanismName) {
      if (GSSAPI.mechanismName.equals(mechanismName)) {
        return GSSAPI;
      } else if (DIGEST_MD5.mechanismName.equals(mechanismName)) {
        return DIGEST_MD5;
      }

      throw new IllegalArgumentException("No value for " + mechanismName);
    }
  }

  private static String defaultRealm;

  static {
    try {
      defaultRealm = KerberosUtil.getDefaultRealm();
    } catch (Exception ke) {
      log.debug("Kerberos krb5 configuration not found, setting default realm to empty");
      defaultRealm = "UNKNOWN";
    }
  }

  protected String principal;
  protected QualityOfProtection qop;
  protected String kerberosServerPrimary;
  protected SaslMechanism mechanism;
  protected CallbackHandler callbackHandler;
  protected final Map<String,String> saslProperties;

  public SaslConnectionParams(AccumuloConfiguration conf, AuthenticationToken token) {
    this(new ClientConfiguration(createMapConfiguration(conf)), token);
  }

  private static MapConfiguration createMapConfiguration(AccumuloConfiguration conf) {
    MapConfiguration mapConf = new MapConfiguration(getProperties(conf));
    mapConf.setListDelimiter('\0');
    return mapConf;
  }

  public SaslConnectionParams(ClientConfiguration conf, AuthenticationToken token) {
    requireNonNull(conf, "Configuration was null");
    requireNonNull(token, "AuthenticationToken was null");

    saslProperties = new HashMap<>();
    updatePrincipalFromUgi();
    updateFromConfiguration(conf);
    updateFromToken(token);
  }

  protected void updateFromToken(AuthenticationToken token) {
    if (token instanceof KerberosToken) {
      mechanism = SaslMechanism.GSSAPI;
      // No callbackhandlers necessary for GSSAPI
      callbackHandler = null;
    } else if (token instanceof DelegationTokenImpl) {
      mechanism = SaslMechanism.DIGEST_MD5;
      callbackHandler = new SaslClientDigestCallbackHandler((DelegationTokenImpl) token);
    } else {
      throw new IllegalArgumentException("Cannot determine SASL mechanism for token class: " + token.getClass());
    }
  }

  protected static Map<String,String> getProperties(AccumuloConfiguration conf) {
    final Map<String,String> clientProperties = new HashMap<>();

    // Servers will only have the full principal in their configuration -- parse the
    // primary and realm from it.
    final String serverPrincipal = conf.get(Property.GENERAL_KERBEROS_PRINCIPAL);

    final KerberosName krbName;
    try {
      krbName = new KerberosName(serverPrincipal);
      clientProperties.put(ClientProperty.KERBEROS_SERVER_PRIMARY.getKey(), krbName.getServiceName());
    } catch (Exception e) {
      // bad value or empty, assume we're not using kerberos
    }

    HashSet<String> clientKeys = new HashSet<>();
    for (ClientProperty prop : ClientProperty.values()) {
      clientKeys.add(prop.getKey());
    }

    String key;
    for (Entry<String,String> entry : conf) {
      key = entry.getKey();
      if (clientKeys.contains(key)) {
        clientProperties.put(key, entry.getValue());
      }
    }

    return clientProperties;
  }

  protected void updatePrincipalFromUgi() {
    // Ensure we're using Kerberos auth for Hadoop UGI
    if (!UserGroupInformation.isSecurityEnabled()) {
      throw new RuntimeException("Cannot use SASL if Hadoop security is not enabled");
    }

    // Get the current user
    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get current user", e);
    }

    // The full name is our principal
    this.principal = currentUser.getUserName();
    if (null == this.principal) {
      throw new RuntimeException("Got null username from " + currentUser);
    }

  }

  protected void updateFromConfiguration(ClientConfiguration conf) {
    // Get the quality of protection to use
    final String qopValue = conf.get(ClientProperty.RPC_SASL_QOP);
    this.qop = QualityOfProtection.get(qopValue);

    // Add in the SASL properties to a map so we don't have to repeatedly construct this map
    this.saslProperties.put(Sasl.QOP, this.qop.getQuality());

    // The primary from the KRB principal on each server (e.g. primary/instance@realm)
    this.kerberosServerPrimary = conf.get(ClientProperty.KERBEROS_SERVER_PRIMARY);
  }

  public Map<String,String> getSaslProperties() {
    return Collections.unmodifiableMap(saslProperties);
  }

  /**
   * The quality of protection used with SASL. See {@link Sasl#QOP} for more information.
   */
  public QualityOfProtection getQualityOfProtection() {
    return qop;
  }

  /**
   * The 'primary' component from the Kerberos principals that servers are configured to use.
   */
  public String getKerberosServerPrimary() {
    return kerberosServerPrimary;
  }

  /**
   * The principal of the logged in user for SASL
   */
  public String getPrincipal() {
    return principal;
  }

  /**
   * The SASL mechanism to use for authentication
   */
  public SaslMechanism getMechanism() {
    return mechanism;
  }

  /**
   * The SASL callback handler for this mechanism, may be null.
   */
  public CallbackHandler getCallbackHandler() {
    return callbackHandler;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder(23, 29);
    hcb.append(kerberosServerPrimary).append(saslProperties).append(qop.hashCode()).append(principal).append(mechanism).append(callbackHandler);
    return hcb.toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SaslConnectionParams) {
      SaslConnectionParams other = (SaslConnectionParams) o;
      if (!kerberosServerPrimary.equals(other.kerberosServerPrimary)) {
        return false;
      }
      if (qop != other.qop) {
        return false;
      }
      if (!principal.equals(other.principal)) {
        return false;
      }
      if (!mechanism.equals(other.mechanism)) {
        return false;
      }
      if (null == callbackHandler) {
        if (null != other.callbackHandler) {
          return false;
        }
      } else if (!callbackHandler.equals(other.callbackHandler)) {
        return false;
      }

      return saslProperties.equals(other.saslProperties);
    }

    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(64);
    sb.append("SaslConnectionParams[").append("kerberosServerPrimary=").append(kerberosServerPrimary).append(", qualityOfProtection=").append(qop);
    sb.append(", principal=").append(principal).append(", mechanism=").append(mechanism).append(", callbackHandler=").append(callbackHandler).append("]");
    return sb.toString();
  }

  public static String getDefaultRealm() {
    return defaultRealm;
  }
}

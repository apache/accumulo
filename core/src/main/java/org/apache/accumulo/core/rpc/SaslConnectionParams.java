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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import javax.security.sasl.Sasl;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
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
    AUTH("auth"),
    AUTH_INT("auth-int"),
    AUTH_CONF("auth-conf");

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

  private static String defaultRealm;

  static {
    try {
      defaultRealm = KerberosUtil.getDefaultRealm();
    } catch (Exception ke) {
      log.debug("Kerberos krb5 configuration not found, setting default realm to empty");
      defaultRealm = "UNKNOWN";
    }
  }

  private String principal;
  private QualityOfProtection qop;
  private String kerberosServerPrimary;
  private final Map<String,String> saslProperties;

  private SaslConnectionParams() {
    saslProperties = new HashMap<>();
  }

  /**
   * Generate an {@link SaslConnectionParams} instance given the provided {@link AccumuloConfiguration}. The provided configuration is converted into a
   * {@link ClientConfiguration}, ignoring any properties which are not {@link ClientProperty}s. If SASL is not being used, a null object will be returned.
   * Callers should strive to use {@link #forConfig(ClientConfiguration)}; server processes are the only intended consumers of this method.
   *
   * @param conf
   *          The configuration for clients to communicate with Accumulo
   * @return An {@link SaslConnectionParams} instance or null if SASL is not enabled
   */
  public static SaslConnectionParams forConfig(AccumuloConfiguration conf) {
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

    ClientConfiguration clientConf = new ClientConfiguration(new MapConfiguration(clientProperties));
    return forConfig(clientConf);
  }

  /**
   * Generate an {@link SaslConnectionParams} instance given the provided {@link ClientConfiguration}. If SASL is not being used, a null object will be
   * returned.
   *
   * @param conf
   *          The configuration for clients to communicate with Accumulo
   * @return An {@link SaslConnectionParams} instance or null if SASL is not enabled
   */
  public static SaslConnectionParams forConfig(ClientConfiguration conf) {
    if (!Boolean.parseBoolean(conf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED))) {
      return null;
    }

    SaslConnectionParams params = new SaslConnectionParams();

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
    params.principal = currentUser.getUserName();
    if (null == params.principal) {
      throw new RuntimeException("Got null username from " + currentUser);
    }

    // Get the quality of protection to use
    final String qopValue = conf.get(ClientProperty.RPC_SASL_QOP);
    params.qop = QualityOfProtection.get(qopValue);

    // Add in the SASL properties to a map so we don't have to repeatedly construct this map
    params.saslProperties.put(Sasl.QOP, params.qop.getQuality());

    // The primary from the KRB principal on each server (e.g. primary/instance@realm)
    params.kerberosServerPrimary = conf.get(ClientProperty.KERBEROS_SERVER_PRIMARY);

    return params;
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

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder(23,29);
    hcb.append(kerberosServerPrimary).append(saslProperties).append(qop.hashCode()).append(principal);
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

      return saslProperties.equals(other.saslProperties);
    }

    return false;
  }

  public static String getDefaultRealm() {
    return defaultRealm;
  }
}

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
package org.apache.accumulo.harness.conf;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.harness.AccumuloClusterIT.ClusterType;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Extract configuration properties for a MiniAccumuloCluster from Java properties
 */
public class AccumuloMiniClusterConfiguration extends AccumuloClusterPropertyConfiguration {
  private static final Logger log = LoggerFactory.getLogger(AccumuloMiniClusterConfiguration.class);
  private static final String TRUE = Boolean.toString(true);

  public static final String ACCUMULO_MINI_PRINCIPAL_KEY = ACCUMULO_MINI_PREFIX + "principal";
  public static final String ACCUMULO_MINI_PRINCIPAL_DEFAULT = "root";
  public static final String ACCUMULO_MINI_PASSWORD_KEY = ACCUMULO_MINI_PREFIX + "password";
  public static final String ACCUMULO_MINI_PASSWORD_DEFAULT = "rootPassword1";

  private final Map<String,String> conf;
  private final boolean saslEnabled;
  private ClientConfiguration clientConf;

  public AccumuloMiniClusterConfiguration() {
    ClusterType type = getClusterType();
    if (ClusterType.MINI != type) {
      throw new IllegalStateException("Expected only to see mini cluster state");
    }

    this.conf = getConfiguration(type);
    this.saslEnabled = TRUE.equals(System.getProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION));
    log.debug("SASL is {}enabled", (saslEnabled ? "" : "not "));
  }

  @Override
  public String getAdminPrincipal() {
    if (saslEnabled) {
      return AccumuloClusterIT.getKdc().getRootUser().getPrincipal();
    } else {
      String principal = conf.get(ACCUMULO_MINI_PRINCIPAL_KEY);
      if (null == principal) {
        principal = ACCUMULO_MINI_PRINCIPAL_DEFAULT;
      }

      return principal;
    }
  }

  @Override
  public AuthenticationToken getAdminToken() {
    if (saslEnabled) {
      // Turn on Kerberos authentication so UGI acts properly
      final Configuration conf = new Configuration(false);
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      UserGroupInformation.setConfiguration(conf);

      ClusterUser rootUser = AccumuloClusterIT.getKdc().getRootUser();
      try {
        return new KerberosToken(rootUser.getPrincipal(), rootUser.getKeytab(), true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      String password = conf.get(ACCUMULO_MINI_PASSWORD_KEY);
      if (null == password) {
        password = ACCUMULO_MINI_PASSWORD_DEFAULT;
      }

      return new PasswordToken(password);
    }
  }

  @Override
  public ClusterType getClusterType() {
    return ClusterType.MINI;
  }

  @Override
  public ClientConfiguration getClientConf() {
    return clientConf;
  }

  public void setClientConf(ClientConfiguration conf) {
    Preconditions.checkNotNull(conf, "Client configuration was null");
    this.clientConf = conf;
  }
}

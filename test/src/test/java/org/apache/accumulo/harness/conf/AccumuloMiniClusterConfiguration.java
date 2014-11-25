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

import java.util.Map;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.harness.AccumuloClusterIT.ClusterType;

/**
 * Extract configuration properties for a MiniAccumuloCluster from Java properties
 */
public class AccumuloMiniClusterConfiguration extends AccumuloClusterPropertyConfiguration {

  public static final String ACCUMULO_MINI_PRINCIPAL_KEY = ACCUMULO_MINI_PREFIX + "principal";
  public static final String ACCUMULO_MINI_PRINCIPAL_DEFAULT = "root";
  public static final String ACCUMULO_MINI_PASSWORD_KEY = ACCUMULO_MINI_PREFIX + "password";
  public static final String ACCUMULO_MINI_PASSWORD_DEFAULT = "rootPassword1";

  private Map<String,String> conf;

  public AccumuloMiniClusterConfiguration() {
    ClusterType type = getClusterType();
    if (ClusterType.MINI != type) {
      throw new IllegalStateException("Expected only to see mini cluster state");
    }

    this.conf = getConfiguration(type);
  }

  @Override
  public String getPrincipal() {
    String principal = conf.get(ACCUMULO_MINI_PRINCIPAL_KEY);
    if (null == principal) {
      principal = ACCUMULO_MINI_PRINCIPAL_DEFAULT;
    }

    return principal;
  }

  @Override
  public AuthenticationToken getToken() {
    String password = conf.get(ACCUMULO_MINI_PASSWORD_KEY);
    if (null == password) {
      password = ACCUMULO_MINI_PASSWORD_DEFAULT;
    }

    return new PasswordToken(password);
  }

  @Override
  public ClusterType getClusterType() {
    return ClusterType.MINI;
  }

}

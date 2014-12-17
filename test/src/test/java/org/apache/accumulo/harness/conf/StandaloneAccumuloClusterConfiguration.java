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

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.harness.AccumuloClusterIT.ClusterType;

/**
 * Extract connection information to a standalone Accumulo instance from Java properties
 */
public class StandaloneAccumuloClusterConfiguration extends AccumuloClusterPropertyConfiguration {

  public static final String ACCUMULO_STANDALONE_PRINCIPAL_KEY = ACCUMULO_STANDALONE_PREFIX + "principal";
  public static final String ACCUMULO_STANDALONE_PRINCIPAL_DEFAULT = "root";
  public static final String ACCUMULO_STANDALONE_PASSWORD_KEY = ACCUMULO_STANDALONE_PREFIX + "password";
  public static final String ACCUMULO_STANDALONE_PASSWORD_DEFAULT = "rootPassword1";
  public static final String ACCUMULO_STANDALONE_ZOOKEEPERS_KEY = ACCUMULO_STANDALONE_PREFIX + "zookeepers";
  public static final String ACCUMULO_STANDALONE_ZOOKEEPERS_DEFAULT = "localhost";
  public static final String ACCUMULO_STANDALONE_INSTANCE_NAME_KEY = ACCUMULO_STANDALONE_PREFIX + "instance.name";
  public static final String ACCUMULO_STANDALONE_INSTANCE_NAME_DEFAULT = "accumulo";

  public static final String ACCUMULO_STANDALONE_HOME = ACCUMULO_STANDALONE_PREFIX + "home";
  public static final String ACCUMULO_STANDALONE_CONF = ACCUMULO_STANDALONE_PREFIX + "conf";
  public static final String ACCUMULO_STANDALONE_HADOOP_CONF = ACCUMULO_STANDALONE_PREFIX + "hadoop.conf";

  private Map<String,String> conf;

  public StandaloneAccumuloClusterConfiguration() {
    ClusterType type = getClusterType();
    if (ClusterType.STANDALONE != type) {
      throw new IllegalStateException("Expected only to see standalone cluster state");
    }

    this.conf = getConfiguration(type);
  }

  @Override
  public String getPrincipal() {
    String principal = conf.get(ACCUMULO_STANDALONE_PRINCIPAL_KEY);
    if (null == principal) {
      principal = ACCUMULO_STANDALONE_PRINCIPAL_DEFAULT;
    }
    return principal;
  }

  public String getPassword() {
    String password = conf.get(ACCUMULO_STANDALONE_PASSWORD_KEY);
    if (null == password) {
      password = ACCUMULO_STANDALONE_PASSWORD_DEFAULT;
    }
    return password;
  }

  @Override
  public AuthenticationToken getToken() {
    return new PasswordToken(getPassword());
  }

  public String getZooKeepers() {
    String zookeepers = conf.get(ACCUMULO_STANDALONE_ZOOKEEPERS_KEY);
    if (null == zookeepers) {
      zookeepers = ACCUMULO_STANDALONE_ZOOKEEPERS_DEFAULT;
    }
    return zookeepers;
  }

  public String getInstanceName() {
    String instanceName = conf.get(ACCUMULO_STANDALONE_INSTANCE_NAME_KEY);
    if (null == instanceName) {
      instanceName = ACCUMULO_STANDALONE_INSTANCE_NAME_DEFAULT;
    }
    return instanceName;
  }

  public Instance getInstance() {
    return new ZooKeeperInstance(getInstanceName(), getZooKeepers());
  }

  @Override
  public ClusterType getClusterType() {
    return ClusterType.STANDALONE;
  }

  public String getHadoopConfDir() {
    return conf.get(ACCUMULO_STANDALONE_HADOOP_CONF);
  }

  public String getAccumuloHome() {
    return conf.get(ACCUMULO_STANDALONE_HOME);
  }

  public String getAccumuloConfDir() {
    return conf.get(ACCUMULO_STANDALONE_CONF);
  }
}

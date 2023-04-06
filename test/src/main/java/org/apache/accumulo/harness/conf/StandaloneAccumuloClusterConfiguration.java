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
package org.apache.accumulo.harness.conf;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.harness.AccumuloClusterHarness.ClusterType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Extract connection information to a standalone Accumulo instance from Java properties
 */
public class StandaloneAccumuloClusterConfiguration extends AccumuloClusterPropertyConfiguration {
  private static final Logger log =
      LoggerFactory.getLogger(StandaloneAccumuloClusterConfiguration.class);

  public static final String ACCUMULO_STANDALONE_ADMIN_PRINCIPAL_KEY =
      ACCUMULO_STANDALONE_PREFIX + "admin.principal";
  public static final String ACCUMULO_STANDALONE_ADMIN_PRINCIPAL_DEFAULT = "root";
  public static final String ACCUMULO_STANDALONE_PASSWORD_KEY =
      ACCUMULO_STANDALONE_PREFIX + "admin.password";
  public static final String ACCUMULO_STANDALONE_PASSWORD_DEFAULT = "rootPassword1";
  public static final String ACCUMULO_STANDALONE_ADMIN_KEYTAB_KEY =
      ACCUMULO_STANDALONE_PREFIX + "admin.keytab";
  public static final String ACCUMULO_STANDALONE_ZOOKEEPERS_KEY =
      ACCUMULO_STANDALONE_PREFIX + "zookeepers";
  public static final String ACCUMULO_STANDALONE_ZOOKEEPERS_DEFAULT = "localhost";
  public static final String ACCUMULO_STANDALONE_INSTANCE_NAME_KEY =
      ACCUMULO_STANDALONE_PREFIX + "instance.name";
  public static final String ACCUMULO_STANDALONE_INSTANCE_NAME_DEFAULT = "accumulo";
  public static final String ACCUMULO_STANDALONE_TMP_DIR_KEY =
      ACCUMULO_STANDALONE_PREFIX + "tmpdir";
  public static final String ACCUMULO_STANDALONE_TMP_DIR_DEFAULT = "/tmp";
  public static final String ACCUMULO_STANDALONE_SERVER_USER =
      ACCUMULO_STANDALONE_PREFIX + "server.user";
  public static final String ACCUMULO_STANDALONE_SERVER_USER_DEFAULT = "accumulo";

  // A set of users we can use to connect to this instances
  public static final String ACCUMULO_STANDALONE_USER_KEY = ACCUMULO_STANDALONE_PREFIX + "users.";
  // Keytabs for the users
  public static final String ACCUMULO_STANDALONE_USER_KEYTABS_KEY =
      ACCUMULO_STANDALONE_PREFIX + "keytabs.";
  // Passwords for the users
  public static final String ACCUMULO_STANDALONE_USER_PASSWORDS_KEY =
      ACCUMULO_STANDALONE_PREFIX + "passwords.";

  public static final String ACCUMULO_STANDALONE_HOME = ACCUMULO_STANDALONE_PREFIX + "home";
  public static final String ACCUMULO_STANDALONE_CLIENT_CONF =
      ACCUMULO_STANDALONE_PREFIX + "client.conf";
  public static final String ACCUMULO_STANDALONE_SERVER_CONF =
      ACCUMULO_STANDALONE_PREFIX + "server.conf";
  public static final String ACCUMULO_STANDALONE_CLIENT_CMD_PREFIX =
      ACCUMULO_STANDALONE_PREFIX + "client.cmd.prefix";
  public static final String ACCUMULO_STANDALONE_SERVER_CMD_PREFIX =
      ACCUMULO_STANDALONE_PREFIX + "server.cmd.prefix";
  public static final String ACCUMULO_STANDALONE_HADOOP_CONF =
      ACCUMULO_STANDALONE_PREFIX + "hadoop.conf";

  private Map<String,String> conf;
  private String serverUser;
  private ClientInfo clientInfo;
  private List<ClusterUser> clusterUsers;
  private File clientPropsFile;

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  public StandaloneAccumuloClusterConfiguration(File clientPropsFile) {
    ClusterType type = getClusterType();
    if (type != ClusterType.STANDALONE) {
      throw new IllegalStateException("Expected only to see standalone cluster state");
    }

    this.conf = getConfiguration(type);
    this.clientPropsFile = clientPropsFile;
    clientInfo = ClientInfo.from(Accumulo.newClientProperties()
        .to(getInstanceName(), getZooKeepers()).as(getAdminPrincipal(), getAdminToken()).build());

    // The user Accumulo is running as
    serverUser = conf.get(ACCUMULO_STANDALONE_SERVER_USER);
    if (serverUser == null) {
      serverUser = ACCUMULO_STANDALONE_SERVER_USER_DEFAULT;
    }

    clusterUsers = new ArrayList<>();
    for (Entry<String,String> entry : conf.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(ACCUMULO_STANDALONE_USER_KEY)) {
        String suffix = key.substring(ACCUMULO_STANDALONE_USER_KEY.length());
        String keytab = conf.get(ACCUMULO_STANDALONE_USER_KEYTABS_KEY + suffix);
        if (keytab != null) {
          File keytabFile = new File(keytab);
          assertTrue(keytabFile.exists() && keytabFile.isFile(),
              "Keytab doesn't exist: " + keytabFile);
          clusterUsers.add(new ClusterUser(entry.getValue(), keytabFile));
        } else {
          String password = conf.get(ACCUMULO_STANDALONE_USER_PASSWORDS_KEY + suffix);
          if (password == null) {
            throw new IllegalArgumentException(
                "Missing password or keytab configuration for user with offset " + suffix);
          }
          clusterUsers.add(new ClusterUser(entry.getValue(), password));
        }
      }
    }
    log.info("Initialized Accumulo users with Kerberos keytabs: {}", clusterUsers);
  }

  @Override
  public String getAdminPrincipal() {
    String principal = conf.get(ACCUMULO_STANDALONE_ADMIN_PRINCIPAL_KEY);
    if (principal == null) {
      principal = ACCUMULO_STANDALONE_ADMIN_PRINCIPAL_DEFAULT;
    }
    return principal;
  }

  public ClientInfo getClientInfo() {
    return clientInfo;
  }

  public String getPassword() {
    String password = conf.get(ACCUMULO_STANDALONE_PASSWORD_KEY);
    if (password == null) {
      password = ACCUMULO_STANDALONE_PASSWORD_DEFAULT;
    }
    return password;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  public File getAdminKeytab() {
    String keytabPath = conf.get(ACCUMULO_STANDALONE_ADMIN_KEYTAB_KEY);
    if (keytabPath == null || keytabPath.isEmpty()) {
      return null;
    }
    File keytab = new File(keytabPath);
    if (!keytab.exists() || !keytab.isFile()) {
      throw new RuntimeException(keytabPath + " should be a regular file");
    }
    return keytab;
  }

  @Override
  public AuthenticationToken getAdminToken() {
    File keytab = getAdminKeytab();
    if (keytab != null) {
      try {
        UserGroupInformation.loginUserFromKeytab(getAdminPrincipal(), keytab.getAbsolutePath());
        return new KerberosToken();
      } catch (IOException e) {
        // The user isn't logged in
        throw new RuntimeException("Failed to create KerberosToken", e);
      }
    } else {
      return new PasswordToken(getPassword());
    }
  }

  public String getZooKeepers() {
    String zookeepers = conf.get(ACCUMULO_STANDALONE_ZOOKEEPERS_KEY);
    if (zookeepers == null) {
      zookeepers = ACCUMULO_STANDALONE_ZOOKEEPERS_DEFAULT;
    }
    return zookeepers;
  }

  public String getInstanceName() {
    String instanceName = conf.get(ACCUMULO_STANDALONE_INSTANCE_NAME_KEY);
    if (instanceName == null) {
      instanceName = ACCUMULO_STANDALONE_INSTANCE_NAME_DEFAULT;
    }
    return instanceName;
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

  public String getClientAccumuloConfDir() {
    return conf.get(ACCUMULO_STANDALONE_CLIENT_CONF);
  }

  public String getServerAccumuloConfDir() {
    return conf.get(ACCUMULO_STANDALONE_SERVER_CONF);
  }

  public String getServerCmdPrefix() {
    return conf.get(ACCUMULO_STANDALONE_SERVER_CMD_PREFIX);
  }

  public String getClientCmdPrefix() {
    return conf.get(ACCUMULO_STANDALONE_CLIENT_CMD_PREFIX);
  }

  public File getClientPropsFile() {
    return clientPropsFile;
  }

  public Path getTmpDirectory() {
    String tmpDir = conf.get(ACCUMULO_STANDALONE_TMP_DIR_KEY);
    if (tmpDir == null) {
      tmpDir = ACCUMULO_STANDALONE_TMP_DIR_DEFAULT;
    }
    return new Path(tmpDir);
  }

  public List<ClusterUser> getUsers() {
    return Collections.unmodifiableList(clusterUsers);
  }

  /**
   * @return The user Accumulo is running as
   */
  public String getAccumuloServerUser() {
    return serverUser;
  }
}

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
package org.apache.accumulo.harness;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.minikdc.MiniKdc.JAVA_SECURITY_KRB5_CONF;
import static org.apache.hadoop.minikdc.MiniKdc.SUN_SECURITY_KRB5_DEBUG;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.security.handler.KerberosAuthenticator;
import org.apache.accumulo.server.security.handler.KerberosAuthorizor;
import org.apache.accumulo.server.security.handler.KerberosPermissionHandler;
import org.apache.accumulo.test.functional.NativeMapIT;
import org.apache.accumulo.test.util.CertUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Harness that sets up a MiniAccumuloCluster in a manner expected for Accumulo integration tests.
 */
public class MiniClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(MiniClusterHarness.class);

  private static final String PROP_PREFIX = "org.apache.accumulo.test.functional.";
  public static final String USE_SSL_FOR_IT_OPTION = PROP_PREFIX + "useSslForIT";
  public static final String USE_CRED_PROVIDER_FOR_IT_OPTION = PROP_PREFIX + "useCredProviderForIT";
  public static final String USE_KERBEROS_FOR_IT_OPTION = PROP_PREFIX + "useKrbForIT";
  public static final String TRUE = Boolean.toString(true);

  public MiniAccumuloClusterImpl create(AccumuloITBase testBase, AuthenticationToken token,
      TestingKdc kdc, MiniClusterConfigurationCallback configCallback) throws Exception {
    return create(testBase.getClass().getName(), testBase.testName(), token, configCallback, kdc);
  }

  public MiniAccumuloClusterImpl create(String testClassName, String testMethodName,
      AuthenticationToken token, MiniClusterConfigurationCallback configCallback, TestingKdc kdc)
      throws Exception {
    requireNonNull(token);
    checkArgument(token instanceof PasswordToken || token instanceof KerberosToken,
        "A PasswordToken or KerberosToken is required");

    String rootPasswd;
    if (token instanceof PasswordToken) {
      rootPasswd = new String(((PasswordToken) token).getPassword(), UTF_8);
    } else {
      rootPasswd = UUID.randomUUID().toString();
    }

    File baseDir = AccumuloClusterHarness.createTestDir(testClassName + "_" + testMethodName);
    MiniAccumuloConfigImpl cfg = new MiniAccumuloConfigImpl(baseDir, rootPasswd);

    // Enable native maps by default
    cfg.setNativeLibPaths(NativeMapIT.nativeMapLocation().getAbsolutePath());
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, Boolean.TRUE.toString());

    Configuration coreSite = new Configuration(false);

    // Setup SSL and credential providers if the properties request such
    configureForEnvironment(cfg, AccumuloClusterHarness.getSslDir(baseDir), coreSite, kdc);

    // Invoke the callback for tests to configure MAC before it starts
    configCallback.configureMiniCluster(cfg, coreSite);

    MiniAccumuloClusterImpl miniCluster = new MiniAccumuloClusterImpl(cfg);

    // Write out any configuration items to a file so HDFS will pick them up automatically (from the
    // classpath)
    if (coreSite.size() > 0) {
      File csFile = new File(miniCluster.getConfig().getConfDir(), "core-site.xml");
      if (csFile.exists()) {
        throw new RuntimeException(csFile + " already exist");
      }

      OutputStream out = new BufferedOutputStream(
          new FileOutputStream(new File(miniCluster.getConfig().getConfDir(), "core-site.xml")));
      coreSite.writeXml(out);
      out.close();
    }

    return miniCluster;
  }

  protected void configureForEnvironment(MiniAccumuloConfigImpl cfg, File folder,
      Configuration coreSite, TestingKdc kdc) {
    if (TRUE.equals(System.getProperty(USE_SSL_FOR_IT_OPTION))) {
      configureForSsl(cfg, folder);
    }
    if (TRUE.equals(System.getProperty(USE_CRED_PROVIDER_FOR_IT_OPTION))) {
      cfg.setUseCredentialProvider(true);
    }

    if (TRUE.equals(System.getProperty(USE_KERBEROS_FOR_IT_OPTION))) {
      if (TRUE.equals(System.getProperty(USE_SSL_FOR_IT_OPTION))) {
        throw new RuntimeException("Cannot use both SSL and Kerberos");
      }

      try {
        configureForKerberos(cfg, coreSite, kdc);
      } catch (Exception e) {
        throw new RuntimeException("Failed to initialize KDC", e);
      }
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths provided by test")
  protected void configureForSsl(MiniAccumuloConfigImpl cfg, File folder) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    if (TRUE.equals(siteConfig.get(Property.INSTANCE_RPC_SSL_ENABLED.getKey()))) {
      // already enabled; don't mess with it
      return;
    }

    File sslDir = new File(folder, "ssl");
    assertTrue(sslDir.mkdirs() || sslDir.isDirectory());
    File rootKeystoreFile = new File(sslDir, "root-" + cfg.getInstanceName() + ".jks");
    File localKeystoreFile = new File(sslDir, "local-" + cfg.getInstanceName() + ".jks");
    File publicTruststoreFile = new File(sslDir, "public-" + cfg.getInstanceName() + ".jks");
    final String rootKeystorePassword = "root_keystore_password",
        truststorePassword = "truststore_password";
    try {
      new CertUtils(Property.RPC_SSL_KEYSTORE_TYPE.getDefaultValue(),
          "o=Apache Accumulo,cn=MiniAccumuloCluster", "RSA", 4096, "SHA512WITHRSA").createAll(
              rootKeystoreFile, localKeystoreFile, publicTruststoreFile, cfg.getInstanceName(),
              rootKeystorePassword, cfg.getRootPassword(), truststorePassword);
    } catch (Exception e) {
      throw new RuntimeException("error creating MAC keystore", e);
    }

    siteConfig.put(Property.INSTANCE_RPC_SSL_ENABLED.getKey(), "true");
    siteConfig.put(Property.RPC_SSL_KEYSTORE_PATH.getKey(), localKeystoreFile.getAbsolutePath());
    siteConfig.put(Property.RPC_SSL_KEYSTORE_PASSWORD.getKey(), cfg.getRootPassword());
    siteConfig.put(Property.RPC_SSL_TRUSTSTORE_PATH.getKey(),
        publicTruststoreFile.getAbsolutePath());
    siteConfig.put(Property.RPC_SSL_TRUSTSTORE_PASSWORD.getKey(), truststorePassword);
    cfg.setSiteConfig(siteConfig);

    cfg.setClientProperty(ClientProperty.SSL_ENABLED, "true");
    cfg.setClientProperty(ClientProperty.SSL_KEYSTORE_PATH, localKeystoreFile.getAbsolutePath());
    cfg.setClientProperty(ClientProperty.SSL_KEYSTORE_PASSWORD.getKey(), cfg.getRootPassword());
    cfg.setClientProperty(ClientProperty.SSL_TRUSTSTORE_PATH.getKey(),
        publicTruststoreFile.getAbsolutePath());
    cfg.setClientProperty(ClientProperty.SSL_TRUSTSTORE_PASSWORD.getKey(), truststorePassword);
  }

  protected void configureForKerberos(MiniAccumuloConfigImpl cfg, Configuration coreSite,
      TestingKdc kdc) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    if (TRUE.equals(siteConfig.get(Property.INSTANCE_RPC_SSL_ENABLED.getKey()))) {
      throw new RuntimeException("Cannot use both SSL and SASL/Kerberos");
    }

    if (TRUE.equals(siteConfig.get(Property.INSTANCE_RPC_SASL_ENABLED.getKey()))) {
      // already enabled
      return;
    }

    if (kdc == null) {
      throw new IllegalStateException("MiniClusterKdc was null");
    }

    log.info("Enabling Kerberos/SASL for minicluster");

    // Turn on SASL and set the keytab/principal information
    cfg.setProperty(Property.INSTANCE_RPC_SASL_ENABLED, "true");
    ClusterUser serverUser = kdc.getAccumuloServerUser();
    cfg.setProperty(Property.GENERAL_KERBEROS_KEYTAB, serverUser.getKeytab().getAbsolutePath());
    cfg.setProperty(Property.GENERAL_KERBEROS_PRINCIPAL, serverUser.getPrincipal());
    cfg.setProperty(Property.INSTANCE_SECURITY_AUTHENTICATOR,
        KerberosAuthenticator.class.getName());
    cfg.setProperty(Property.INSTANCE_SECURITY_AUTHORIZOR, KerberosAuthorizor.class.getName());
    cfg.setProperty(Property.INSTANCE_SECURITY_PERMISSION_HANDLER,
        KerberosPermissionHandler.class.getName());

    // Pass down some KRB5 debug properties
    Map<String,String> systemProperties = cfg.getSystemProperties();
    systemProperties.put(JAVA_SECURITY_KRB5_CONF, System.getProperty(JAVA_SECURITY_KRB5_CONF, ""));
    systemProperties.put(SUN_SECURITY_KRB5_DEBUG,
        System.getProperty(SUN_SECURITY_KRB5_DEBUG, "false"));
    cfg.setSystemProperties(systemProperties);

    // Make sure UserGroupInformation will do the correct login
    coreSite.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");

    cfg.setRootUserName(kdc.getRootUser().getPrincipal());

    cfg.setClientProperty(ClientProperty.SASL_ENABLED, "true");
  }
}

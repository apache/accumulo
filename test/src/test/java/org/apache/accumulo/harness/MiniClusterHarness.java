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
package org.apache.accumulo.harness;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.NativeMapIT;
import org.apache.accumulo.test.util.CertUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

/**
 * Harness that sets up a MiniAccumuloCluster in a manner expected for Accumulo integration tests.
 */
public class MiniClusterHarness {

  private static final AtomicLong COUNTER = new AtomicLong(0);

  public static final String USE_SSL_FOR_IT_OPTION = "org.apache.accumulo.test.functional.useSslForIT",
      USE_CRED_PROVIDER_FOR_IT_OPTION = "org.apache.accumulo.test.functional.useCredProviderForIT", TRUE = Boolean.toString(true);

  /**
   * Create a MiniAccumuloCluster using the given Token as the credentials for the root user.
   */
  public MiniAccumuloClusterImpl create(AuthenticationToken token) throws Exception {
    return create(MiniClusterHarness.class.getName(), Long.toString(COUNTER.incrementAndGet()), token);
  }

  public MiniAccumuloClusterImpl create(AccumuloIT testBase, AuthenticationToken token) throws Exception {
    return create(testBase.getClass().getName(), testBase.testName.getMethodName(), token);
  }

  public MiniAccumuloClusterImpl create(AccumuloClusterIT testBase, AuthenticationToken token) throws Exception {
    return create(testBase.getClass().getName(), testBase.testName.getMethodName(), token, testBase);
  }

  public MiniAccumuloClusterImpl create(AccumuloClusterIT testBase, AuthenticationToken token, MiniClusterConfigurationCallback callback) throws Exception {
    return create(testBase.getClass().getName(), testBase.testName.getMethodName(), token, testBase);
  }

  public MiniAccumuloClusterImpl create(String testClassName, String testMethodName, AuthenticationToken token) throws Exception {
    return create(testClassName, testMethodName, token, MiniClusterConfigurationCallback.NO_CALLBACK);
  }

  public MiniAccumuloClusterImpl create(String testClassName, String testMethodName, AuthenticationToken token, MiniClusterConfigurationCallback configCallback)
      throws Exception {
    Preconditions.checkNotNull(token);
    Preconditions.checkArgument(PasswordToken.class.isAssignableFrom(token.getClass()));

    String passwd = new String(((PasswordToken) token).getPassword(), Charsets.UTF_8);
    MiniAccumuloConfigImpl cfg = new MiniAccumuloConfigImpl(AccumuloClusterIT.createTestDir(testClassName + "_" + testMethodName), passwd);

    // Enable native maps by default
    cfg.setNativeLibPaths(NativeMapIT.nativeMapLocation().getAbsolutePath());
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, Boolean.TRUE.toString());

    // Setup SSL and credential providers if the properties request such
    configureForEnvironment(cfg, getClass(), AccumuloClusterIT.createSharedTestDir(this.getClass().getName() + "-ssl"));

    Configuration coreSite = new Configuration(false);

    // Invoke the callback for tests to configure MAC before it starts
    configCallback.configureMiniCluster(cfg, coreSite);

    MiniAccumuloClusterImpl miniCluster = new MiniAccumuloClusterImpl(cfg);

    // Write out any configuration items to a file so HDFS will pick them up automatically (from the classpath)
    if (coreSite.size() > 0) {
      File csFile = new File(miniCluster.getConfig().getConfDir(), "core-site.xml");
      if (csFile.exists())
        throw new RuntimeException(csFile + " already exist");

      OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(miniCluster.getConfig().getConfDir(), "core-site.xml")));
      coreSite.writeXml(out);
      out.close();
    }

    return miniCluster;
  }

  protected void configureForEnvironment(MiniAccumuloConfigImpl cfg, Class<?> testClass, File folder) {
    if (TRUE.equals(System.getProperty(USE_SSL_FOR_IT_OPTION))) {
      configureForSsl(cfg, folder);
    }
    if (TRUE.equals(System.getProperty(USE_CRED_PROVIDER_FOR_IT_OPTION))) {
      cfg.setUseCredentialProvider(true);
    }
  }

  protected void configureForSsl(MiniAccumuloConfigImpl cfg, File folder) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    if (TRUE.equals(siteConfig.get(Property.INSTANCE_RPC_SSL_ENABLED.getKey()))) {
      // already enabled; don't mess with it
      return;
    }

    File sslDir = new File(folder, "ssl");
    sslDir.mkdirs();
    File rootKeystoreFile = new File(sslDir, "root-" + cfg.getInstanceName() + ".jks");
    File localKeystoreFile = new File(sslDir, "local-" + cfg.getInstanceName() + ".jks");
    File publicTruststoreFile = new File(sslDir, "public-" + cfg.getInstanceName() + ".jks");
    final String rootKeystorePassword = "root_keystore_password", truststorePassword = "truststore_password";
    try {
      new CertUtils(Property.RPC_SSL_KEYSTORE_TYPE.getDefaultValue(), "o=Apache Accumulo,cn=MiniAccumuloCluster", "RSA", 2048, "sha1WithRSAEncryption")
          .createAll(rootKeystoreFile, localKeystoreFile, publicTruststoreFile, cfg.getInstanceName(), rootKeystorePassword, cfg.getRootPassword(),
              truststorePassword);
    } catch (Exception e) {
      throw new RuntimeException("error creating MAC keystore", e);
    }

    siteConfig.put(Property.INSTANCE_RPC_SSL_ENABLED.getKey(), "true");
    siteConfig.put(Property.RPC_SSL_KEYSTORE_PATH.getKey(), localKeystoreFile.getAbsolutePath());
    siteConfig.put(Property.RPC_SSL_KEYSTORE_PASSWORD.getKey(), cfg.getRootPassword());
    siteConfig.put(Property.RPC_SSL_TRUSTSTORE_PATH.getKey(), publicTruststoreFile.getAbsolutePath());
    siteConfig.put(Property.RPC_SSL_TRUSTSTORE_PASSWORD.getKey(), truststorePassword);
    cfg.setSiteConfig(siteConfig);
  }
}

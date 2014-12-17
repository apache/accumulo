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

import java.io.File;
import java.util.Random;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Convenience class which starts a single MAC instance for a test to leverage.
 *
 * There isn't a good way to build this off of the {@link AccumuloClusterIT} (as would be the logical place) because we need to start the MiniAccumuloCluster in
 * a static BeforeClass-annotated method. Because it is static and invoked before any other BeforeClass methods in the implementation, the actual test classes
 * can't expose any information to tell the base class that it is to perform the one-MAC-per-class semantics.
 */
public abstract class SharedMiniClusterIT extends AccumuloIT {

  private static String rootPassword;
  private static AuthenticationToken token;
  private static MiniAccumuloClusterImpl cluster;

  @BeforeClass
  public static void startMiniCluster() throws Exception {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    baseDir.mkdirs();

    // Make a shared MAC instance instead of spinning up one per test method
    MiniClusterHarness harness = new MiniClusterHarness();

    rootPassword = "rootPasswordShared1";
    token = new PasswordToken(rootPassword);

    cluster = harness.create(SharedMiniClusterIT.class.getName(), System.currentTimeMillis() + "_" + new Random().nextInt(Short.MAX_VALUE), token);
    cluster.start();
  }

  @AfterClass
  public static void stopMiniCluster() throws Exception {
    if (null != cluster) {
      cluster.stop();
    }
  }

  public static String getRootPassword() {
    return rootPassword;
  }

  public static AuthenticationToken getToken() {
    return token;
  }

  public static MiniAccumuloClusterImpl getCluster() {
    return cluster;
  }

  public static File getMiniClusterDir() {
    return cluster.getConfig().getDir();
  }

  public static Connector getConnector() {
    try {
      return getCluster().getConnector("root", getToken());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

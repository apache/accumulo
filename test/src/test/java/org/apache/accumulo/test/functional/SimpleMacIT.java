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
package org.apache.accumulo.test.functional;

import java.io.File;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.MiniAccumuloInstance;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;

public class SimpleMacIT extends AbstractMacIT {
  public static final Logger log = Logger.getLogger(SimpleMacIT.class);

  private static File folder;
  private static MiniAccumuloCluster cluster = null;

  @BeforeClass
  public static synchronized void setUp() throws Exception {
    if (getInstanceOneConnector() == null && cluster == null) {
      folder = createSharedTestDir(SimpleMacIT.class.getName());
      MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder, ROOT_PASSWORD);
      cluster = new MiniAccumuloCluster(cfg);
      cluster.start();
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          cleanUp(cluster);
        }
      });
    }
  }

  @Override
  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    Connector conn = getInstanceOneConnector();
    return conn == null ? cluster.getConnector("root", ROOT_PASSWORD) : conn;
  }

  @Override
  public String rootPath() {
    return (getInstanceOneConnector() == null ? cluster.getConfig().getDir() : getInstanceOnePath()).getAbsolutePath();
  }

  private static Connector getInstanceOneConnector() {
    try {
      return new MiniAccumuloInstance("instance1", getInstanceOnePath()).getConnector("root", new PasswordToken(ROOT_PASSWORD));
    } catch (Exception e) {
      return null;
    }
  }

  private static File getInstanceOnePath() {
    return new File(System.getProperty("user.dir") + "/accumulo-maven-plugin/instance1");
  }

}

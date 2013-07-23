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

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;

public class SimpleMacIT extends AbstractMacIT {
  public static final Logger log = Logger.getLogger(SimpleMacIT.class);
      
  static private TemporaryFolder folder = new TemporaryFolder();
  static private MiniAccumuloCluster cluster;
  
  @BeforeClass
  synchronized public static void setUp() throws Exception {
    if (cluster == null) {
      folder.create();
      MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("mac"), ROOT_PASSWORD);
      cluster = new MiniAccumuloCluster(cfg);
      cluster.start();
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          cleanUp(cluster, folder);
        }
      });
    }
  }
  
  @Override
  public MiniAccumuloCluster getCluster() {
    return cluster;
  }
  
  @AfterClass
  public static void tearDown() throws Exception {
  }
}

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
import org.junit.After;
import org.junit.Before;
import org.junit.rules.TemporaryFolder;
import java.io.File;

public class ConfigurableMacIT extends AbstractMacIT {
  public static final Logger log = Logger.getLogger(ConfigurableMacIT.class);
  
  public TemporaryFolder folder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));
  public MiniAccumuloCluster cluster;
  
  @Before
  public void setUp() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder(this.getClass().getSimpleName()), ROOT_PASSWORD);
    configure(cfg);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }
  
  public void configure(MiniAccumuloConfig cfg) {
  }
  
  @After
  public void tearDown() throws Exception {
    cleanUp(cluster, folder);
  }

  @Override
  public MiniAccumuloCluster getCluster() {
    return cluster;
  }
  
}

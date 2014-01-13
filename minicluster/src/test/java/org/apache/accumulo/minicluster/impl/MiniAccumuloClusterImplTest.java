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
package org.apache.accumulo.minicluster.impl;

import java.io.File;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.minicluster.ServerType;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MiniAccumuloClusterImplTest {

  public static File testDir;

  private static MiniAccumuloClusterImpl accumulo;

  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);

    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    baseDir.mkdirs();
    testDir = new File(baseDir, MiniAccumuloClusterImplTest.class.getName());
    FileUtils.deleteQuietly(testDir);
    testDir.mkdir();

    MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(testDir, "superSecret").setJDWPEnabled(true);
    accumulo = new MiniAccumuloClusterImpl(config);
    accumulo.start();
  }

  @Test(timeout = 10000)
  public void testAccurateProcessListReturned() throws Exception {
    Map<ServerType,Collection<ProcessReference>> procs = accumulo.getProcesses();

    Assert.assertTrue(procs.containsKey(ServerType.GARBAGE_COLLECTOR));

    for (ServerType t : new ServerType[] {ServerType.MASTER, ServerType.TABLET_SERVER, ServerType.ZOOKEEPER}) {
      Assert.assertTrue(procs.containsKey(t));
      Collection<ProcessReference> procRefs = procs.get(t);
      Assert.assertTrue(1 <= procRefs.size());

      for (ProcessReference procRef : procRefs) {
        Assert.assertNotNull(procRef);
      }
    }
  }

  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
  }

}

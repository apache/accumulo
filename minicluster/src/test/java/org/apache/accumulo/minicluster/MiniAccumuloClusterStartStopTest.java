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
package org.apache.accumulo.minicluster;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.Connector;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class MiniAccumuloClusterStartStopTest {

  private File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests/" + this.getClass().getName());
  private File testDir;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void createMacDir() throws IOException {
    baseDir.mkdirs();
    testDir = new File(baseDir, testName.getMethodName());
    FileUtils.deleteQuietly(testDir);
    testDir.mkdir();
  }

  @Test
  public void multipleStartsDoesntThrowAnException() throws Exception {
    MiniAccumuloCluster accumulo = new MiniAccumuloCluster(testDir, "superSecret");

    // In 1.6.0, multiple start's did not throw an exception as advertised
    try {
      accumulo.start();
      accumulo.start();
    } finally {
      accumulo.stop();
    }
  }

  @Test
  public void multipleStopsIsAllowed() throws Exception {
    MiniAccumuloCluster accumulo = new MiniAccumuloCluster(testDir, "superSecret");
    accumulo.start();

    Connector conn = accumulo.getConnector("root", "superSecret");
    conn.tableOperations().create("foo");

    accumulo.stop();
    accumulo.stop();
  }
}

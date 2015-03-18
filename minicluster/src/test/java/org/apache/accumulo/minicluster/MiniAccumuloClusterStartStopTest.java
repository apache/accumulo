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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.Connector;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniAccumuloClusterStartStopTest {

  private static final Logger log = LoggerFactory.getLogger(MiniAccumuloClusterStartStopTest.class);
  private File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests/" + this.getClass().getName());
  private MiniAccumuloCluster accumulo;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setupTestCluster() throws IOException {
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());
    File testDir = new File(baseDir, testName.getMethodName());
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir());
    accumulo = new MiniAccumuloCluster(testDir, "superSecret");
  }

  @After
  public void teardownTestCluster() {
    if (accumulo != null) {
      try {
        accumulo.stop();
      } catch (IOException | InterruptedException e) {
        log.warn("Failure during tear down", e);
      }
    }
  }

  @Test
  public void multipleStartsDoesntThrowAnException() throws Exception {
    // In 1.6.0, multiple start's did not throw an exception as advertised
    accumulo.start();
    try {
      accumulo.start();
    } finally {
      accumulo.stop();
    }
  }

  @Test
  public void multipleStopsIsAllowed() throws Exception {
    accumulo.start();

    Connector conn = accumulo.getConnector("root", "superSecret");
    conn.tableOperations().create("foo");

    accumulo.stop();
    accumulo.stop();
  }
}

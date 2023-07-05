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
package org.apache.accumulo.minicluster;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class MiniAccumuloClusterStartStopTest extends WithTestNames {

  private static final Logger log = LoggerFactory.getLogger(MiniAccumuloClusterStartStopTest.class);
  private File baseDir =
      new File(System.getProperty("user.dir") + "/target/mini-tests/" + this.getClass().getName());
  private MiniAccumuloCluster accumulo;

  @BeforeEach
  public void setupTestCluster() throws IOException {
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());
    File testDir = new File(baseDir, testName());
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir());
    accumulo = new MiniAccumuloCluster(testDir, "superSecret");
  }

  @AfterEach
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

    try (AccumuloClient client = Accumulo.newClient().from(accumulo.getClientProperties())
        .as("root", "superSecret").build()) {
      client.tableOperations().create("foo");
    }

    accumulo.stop();
    accumulo.stop();
  }
}

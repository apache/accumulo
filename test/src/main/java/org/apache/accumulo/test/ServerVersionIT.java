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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ServerVersionIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    startMiniCluster();
  }

  @AfterAll
  public static void tearDown() {
    stopMiniCluster();
  }

  @Test
  public void test() throws Exception {

    File root = getCluster().getConfig().getAccumuloDir();
    String path = root + "/version/";
    Path rootPath = new Path(path + AccumuloDataVersion.get());
    FileSystem fs = getCluster().getFileSystem();

    // Check to make sure cluster is running as expected
    assertTrue(fs.exists(rootPath));

    getCluster().stop();

    // Create a new Path to simulate accumulo update
    Path newPath = new Path(path + AccumuloDataVersion.get() + 1);
    fs.createNewFile(newPath);

    // Check to see if there are errors when restarting servers given new file system.
    getCluster().start();

    // Check to see if both paths exist at the sametime
    assertTrue(fs.exists(rootPath));
    assertTrue(fs.exists(newPath));

  }
}

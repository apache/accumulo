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

import static org.easymock.EasyMock.createMock;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
      Path rootPath =  new Path(path + AccumuloDataVersion.get());

      // Check to make sure cluster is running as expected
      assertTrue(getCluster().getFileSystem().exists(rootPath));

      getCluster().stop();

      //Create a new Path to simulate accumulo update
      Path newPath = new Path(path + AccumuloDataVersion.get()+1);
      getCluster().getFileSystem().createNewFile(newPath);

      //Check to see if there are errors when restarting servers given new file system.
      for(ServerType st : ServerType.values()) {
        assertThrows(IOException.class, () -> getCluster().getClusterControl().start(st));
      }

      //Check to see if both paths exist at the sametime
      assertTrue(getCluster().getFileSystem().exists(rootPath));
      assertTrue(getCluster().getFileSystem().exists(newPath));

  }
}

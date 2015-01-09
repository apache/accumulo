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
package org.apache.accumulo.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class ServerConstantsTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Test
  public void testCheckBaseDirs() throws IOException {
    String uuid1 = UUID.randomUUID().toString();
    String uuid2 = UUID.randomUUID().toString();

    verifyAllPass(init(folder.newFolder(), Arrays.asList(uuid1), Arrays.asList(ServerConstants.DATA_VERSION)));
    verifyAllPass(init(folder.newFolder(), Arrays.asList(uuid1, uuid1), Arrays.asList(ServerConstants.DATA_VERSION, ServerConstants.DATA_VERSION)));

    verifyError(init(folder.newFolder(), Arrays.asList((String) null), Arrays.asList((Integer) null)));
    verifyError(init(folder.newFolder(), Arrays.asList(uuid1, uuid2), Arrays.asList(ServerConstants.DATA_VERSION, ServerConstants.DATA_VERSION)));
    verifyError(init(folder.newFolder(), Arrays.asList(uuid1, uuid1), Arrays.asList(ServerConstants.DATA_VERSION, ServerConstants.DATA_VERSION - 1)));
    verifyError(init(folder.newFolder(), Arrays.asList(uuid1, uuid2), Arrays.asList(ServerConstants.DATA_VERSION, ServerConstants.DATA_VERSION - 1)));
    verifyError(init(folder.newFolder(), Arrays.asList(uuid1, uuid2, null),
        Arrays.asList(ServerConstants.DATA_VERSION, ServerConstants.DATA_VERSION, ServerConstants.DATA_VERSION)));

    verifySomePass(
        init(folder.newFolder(), Arrays.asList(uuid1, uuid1, null), Arrays.asList(ServerConstants.DATA_VERSION, ServerConstants.DATA_VERSION, null)), 2);
  }

  private void verifyAllPass(ArrayList<String> paths) {
    Assert.assertEquals(paths, Arrays.asList(ServerConstants.checkBaseUris(paths.toArray(new String[paths.size()]), true)));
    Assert.assertEquals(paths, Arrays.asList(ServerConstants.checkBaseUris(paths.toArray(new String[paths.size()]), false)));
  }

  private void verifySomePass(ArrayList<String> paths, int numExpected) {
    Assert.assertEquals(paths.subList(0, 2), Arrays.asList(ServerConstants.checkBaseUris(paths.toArray(new String[paths.size()]), true)));
    try {
      ServerConstants.checkBaseUris(paths.toArray(new String[paths.size()]), false);
      Assert.fail();
    } catch (Exception e) {}
  }

  private void verifyError(ArrayList<String> paths) {
    try {
      ServerConstants.checkBaseUris(paths.toArray(new String[paths.size()]), true);
      Assert.fail();
    } catch (Exception e) {}

    try {
      ServerConstants.checkBaseUris(paths.toArray(new String[paths.size()]), false);
      Assert.fail();
    } catch (Exception e) {}
  }

  private ArrayList<String> init(File newFile, List<String> uuids, List<Integer> dataVersions) throws IllegalArgumentException, IOException {
    String base = newFile.toURI().toString();

    LocalFileSystem fs = FileSystem.getLocal(new Configuration());

    ArrayList<String> accumuloPaths = new ArrayList<String>();

    for (int i = 0; i < uuids.size(); i++) {
      String volume = "v" + i;

      String accumuloPath = base + "/" + volume + "/accumulo";
      accumuloPaths.add(accumuloPath);

      if (uuids.get(i) != null) {
        fs.mkdirs(new Path(accumuloPath + "/" + ServerConstants.INSTANCE_ID_DIR));
        fs.createNewFile(new Path(accumuloPath + "/" + ServerConstants.INSTANCE_ID_DIR + "/" + uuids.get(i)));
      }

      if (dataVersions.get(i) != null) {
        fs.mkdirs(new Path(accumuloPath + "/" + ServerConstants.VERSION_DIR));
        fs.createNewFile(new Path(accumuloPath + "/" + ServerConstants.VERSION_DIR + "/" + dataVersions.get(i)));
      }
    }

    return accumuloPaths;
  }

}

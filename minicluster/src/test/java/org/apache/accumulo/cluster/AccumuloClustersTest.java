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
package org.apache.accumulo.cluster;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AccumuloClustersTest {

  @Test
  public void testReturnType() throws IOException {
    MiniAccumuloConfigImpl cfg = createMock(MiniAccumuloConfigImpl.class);
    MiniAccumuloClusterImpl cluster = createMock(MiniAccumuloClusterImpl.class);

    expect(cfg.build()).andReturn(cluster);
    replay(cfg);
    cfg.build();
  }

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Test
  public void testFactoryReturn() throws IOException {
    File dir = tmpDir.newFolder();
    try {
      MiniAccumuloConfig cfg = new MiniAccumuloConfig(dir, "foo");
      Assert.assertEquals(MiniAccumuloCluster.class, AccumuloClusters.createMiniCluster(cfg).getClass());
      Assert.assertTrue(FileUtils.deleteQuietly(dir));
      Assert.assertTrue(dir.mkdirs());
      MiniAccumuloConfigImpl cfgImpl = new MiniAccumuloConfigImpl(dir, "foo");
      Assert.assertEquals(MiniAccumuloClusterImpl.class, AccumuloClusters.create(cfgImpl).getClass());
    } finally {
      FileUtils.deleteQuietly(dir);
    }
  }
}

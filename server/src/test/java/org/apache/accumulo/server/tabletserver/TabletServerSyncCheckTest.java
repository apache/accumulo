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
package org.apache.accumulo.server.tabletserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

public class TabletServerSyncCheckTest {
  private static final String DFS_DURABLE_SYNC = "dfs.durable.sync", DFS_SUPPORT_APPEND = "dfs.support.append";

  @Test(expected = RuntimeException.class)
  public void testFailureOnExplicitSyncFalseConf() {
    Configuration conf = new Configuration();
    conf.set(DFS_DURABLE_SYNC, "false");

    FileSystem fs = new TestFileSystem(conf);

    TabletServer.ensureHdfsSyncIsEnabled(fs);
  }

  @Test(expected = RuntimeException.class)
  public void testFailureOnExplicitAppendFalseConf() {
    Configuration conf = new Configuration();
    conf.set(DFS_SUPPORT_APPEND, "false");

    FileSystem fs = new TestFileSystem(conf);

    TabletServer.ensureHdfsSyncIsEnabled(fs);
  }

  @Test(expected = RuntimeException.class)
  public void testFailureOnExplicitSyncAndAppendFalseConf() {
    Configuration conf = new Configuration();
    conf.set(DFS_SUPPORT_APPEND, "false");
    conf.set(DFS_DURABLE_SYNC, "false");

    FileSystem fs = new TestFileSystem(conf);

    TabletServer.ensureHdfsSyncIsEnabled(fs);
  }

  private class TestFileSystem extends DistributedFileSystem {
    protected final Configuration conf;

    public TestFileSystem(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

  }
}

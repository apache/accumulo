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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MINI_CLUSTER_ONLY)
public class ClassLoaderIT extends AccumuloClusterHarness {

  private static final long ZOOKEEPER_PROPAGATION_TIME = 10_000;

  private String rootPath;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @BeforeEach
  public void checkCluster() {
    assumeTrue(getClusterType() == ClusterType.MINI);
    MiniAccumuloClusterImpl mac = (MiniAccumuloClusterImpl) getCluster();
    rootPath = mac.getConfig().getDir().getAbsolutePath();
  }

  private static void copyStreamToFileSystem(FileSystem fs, String jarName, Path path)
      throws IOException {
    byte[] buffer = new byte[10 * 1024];
    try (FSDataOutputStream dest = fs.create(path);
        InputStream stream = ClassLoaderIT.class.getResourceAsStream(jarName)) {
      while (true) {
        int n = stream.read(buffer, 0, buffer.length);
        if (n <= 0) {
          break;
        }
        dest.write(buffer, 0, n);
      }
    }
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation("row1");
        m.put("cf", "col1", "Test");
        bw.addMutation(m);
      }
      scanCheck(c, tableName, "Test");
      FileSystem fs = getCluster().getFileSystem();
      Path jarPath = new Path(rootPath + "/lib/ext/Test.jar");
      copyStreamToFileSystem(fs, "/org/apache/accumulo/test/TestCombinerX.jar", jarPath);
      sleepUninterruptibly(1, TimeUnit.SECONDS);
      IteratorSetting is = new IteratorSetting(10, "TestCombiner",
          "org.apache.accumulo.test.functional.TestCombiner");
      Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf")));
      c.tableOperations().attachIterator(tableName, is, EnumSet.of(IteratorScope.scan));
      sleepUninterruptibly(ZOOKEEPER_PROPAGATION_TIME, TimeUnit.MILLISECONDS);
      scanCheck(c, tableName, "TestX");
      fs.delete(jarPath, true);
      copyStreamToFileSystem(fs, "/org/apache/accumulo/test/TestCombinerY.jar", jarPath);
      sleepUninterruptibly(5, TimeUnit.SECONDS);
      scanCheck(c, tableName, "TestY");
      fs.delete(jarPath, true);
    }
  }

  private void scanCheck(AccumuloClient c, String tableName, String expected) throws Exception {
    try (Scanner bs = c.createScanner(tableName, Authorizations.EMPTY)) {
      String actual = getOnlyElement(bs).getValue().toString();
      assertEquals(expected, actual);
    }
  }

}

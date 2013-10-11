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
package org.apache.accumulo.test.functional;

import java.io.File;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.rules.TestName;

public abstract class AbstractMacIT {
  public static final Logger log = Logger.getLogger(AbstractMacIT.class);

  public static final String ROOT_PASSWORD = "testRootPassword1";
  public static final ScannerOpts SOPTS = new ScannerOpts();
  public static final BatchWriterOpts BWOPTS = new BatchWriterOpts();

  @Rule
  public TestName testName = new TestName();

  protected static void cleanUp(MiniAccumuloCluster cluster) {
    if (cluster != null)
      try {
        cluster.stop();
      } catch (Exception e) {}
  }

  static AtomicInteger tableCount = new AtomicInteger();

  protected static File createSharedTestDir(String name) {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    baseDir.mkdirs();
    if (name != null)
      baseDir = new File(baseDir, name);
    File testDir = new File(baseDir, System.currentTimeMillis() + "_" + new Random().nextInt(Short.MAX_VALUE));
    FileUtils.deleteQuietly(testDir);
    testDir.mkdir();
    return testDir;
  }

  protected File createTestDir(String name) {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    baseDir.mkdirs();
    if (name == null)
      return baseDir;
    File testDir = new File(baseDir, name);
    FileUtils.deleteQuietly(testDir);
    testDir.mkdir();
    return testDir;
  }

  public String[] getTableNames(int num) {
    if (num == 1)
      return new String[] {testName.getMethodName()};
    String[] names = new String[num];
    for (int i = 0; i < num; i++)
      names[i] = this.getClass().getSimpleName() + "_" + testName.getMethodName() + i;
    return names;
  }

  public abstract Connector getConnector() throws AccumuloException, AccumuloSecurityException;

  public abstract String rootPath();

}

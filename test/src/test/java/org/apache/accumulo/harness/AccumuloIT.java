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
package org.apache.accumulo.harness;

import java.io.File;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Methods, setup and/or infrastructure which are common to any Accumulo integration test.
 */
public class AccumuloIT {
  private static final Logger log = LoggerFactory.getLogger(AccumuloIT.class);

  @Rule
  public TestName testName = new TestName();

  public String[] getUniqueNames(int num) {
    String[] names = new String[num];
    for (int i = 0; i < num; i++)
      names[i] = this.getClass().getSimpleName() + "_" + testName.getMethodName() + i;
    return names;
  }

  public static File createSharedTestDir(String name) {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    baseDir.mkdirs();
    if (name != null)
      baseDir = new File(baseDir, name);
    File testDir = new File(baseDir, System.currentTimeMillis() + "_" + new Random().nextInt(Short.MAX_VALUE));
    FileUtils.deleteQuietly(testDir);
    testDir.mkdir();
    return testDir;
  }

  public static File createTestDir(String name) {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    baseDir.mkdirs();
    if (name == null)
      return baseDir;
    File testDir = new File(baseDir, name);
    FileUtils.deleteQuietly(testDir);
    testDir.mkdir();
    return testDir;
  }

  /**
   * If a given IT test has a method that takes longer than a class-set default timeout, declare it failed.
   *
   * Note that this provides a upper bound on test times, even in the presence of Test annotations with a timeout. That is, the Test annotatation can make the
   * timing tighter but will not be able to allow a timeout that takes longer.
   *
   * Defaults to no timeout and can be changed via two mechanisms
   *
   * 1) A given IT class can override the defaultTimeoutSeconds method if test methods in that class should have a timeout. 2) The system property
   * "timeout.factor" is used as a multiplier for the class provided default
   *
   * Note that if either of these values is '0' tests will run with no timeout. The default class level timeout is set to 0.
   *
   */
  @Rule
  public Timeout testsShouldTimeout() {
    int waitLonger = 0;
    try {
      waitLonger = Integer.parseInt(System.getProperty("timeout.factor"));
    } catch (NumberFormatException exception) {
      log.warn("Could not parse timeout.factor, defaulting to no timeout.");
    }
    return new Timeout(waitLonger * defaultTimeoutSeconds() * 1000);
  }

  /**
   * time to wait per-method before declaring a timeout, in seconds.
   */
  protected int defaultTimeoutSeconds() {
    return 0;
  }
}

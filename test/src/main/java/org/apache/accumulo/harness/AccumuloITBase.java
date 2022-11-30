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
package org.apache.accumulo.harness;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Methods, setup and/or infrastructure which are common to any Accumulo integration test.
 */
public class AccumuloITBase extends WithTestNames {
  public static final SecureRandom random = new SecureRandom();
  private static final Logger log = LoggerFactory.getLogger(AccumuloITBase.class);

  public static final String STANDALONE_CAPABLE_CLUSTER = "StandaloneCapableCluster";
  public static final String SUNNY_DAY = "SunnyDay";
  public static final String MINI_CLUSTER_ONLY = "MiniClusterOnly";
  public static final String ZOOKEEPER_TESTING_SERVER = "ZooKeeperTestingServer";

  protected <T> T getOnlyElement(Collection<T> c) {
    return c.stream().collect(onlyElement());
  }

  protected Entry<Key,Value> getOnlyElement(Scanner s) {
    return s.stream().collect(onlyElement());
  }

  public String[] getUniqueNames(int num) {
    String[] names = new String[num];
    for (int i = 0; i < num; i++) {
      names[i] = this.getClass().getSimpleName() + "_" + testName() + i;
    }
    return names;
  }

  /**
   * Determines an appropriate directory name for holding generated ssl files for a test. The
   * directory returned will have the same name as the provided directory, but with the suffix
   * "-ssl" appended. This new directory is not created here, but is expected to be created as
   * needed.
   *
   * @param baseDir the original directory, which the new directory will be created next to; it
   *        should exist
   * @return the new directory (is not created)
   */
  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  public static File getSslDir(File baseDir) {
    assertTrue(baseDir.exists() && baseDir.isDirectory());
    return new File(baseDir.getParentFile(), baseDir.getName() + "-ssl");
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  public static File createTestDir(String name) {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());
    if (name == null) {
      return baseDir;
    }
    File testDir = new File(baseDir, name);
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir());
    return testDir;
  }

  /**
   * If a given IT test has a method that takes longer than a class-set default timeout, declare it
   * failed.
   * <p>
   * Note that this provides an upper bound on test times, even in the presence of Test annotations
   * with a timeout. That is, the Test annotation can make the timing tighter but will not be able
   * to allow a timeout that takes longer.
   * <p>
   * Defaults to no timeout and can be changed via two mechanisms
   * <p>
   * 1) A given IT class can override the defaultTimeoutSeconds method if test methods in that class
   * should have a timeout. 2) The system property "timeout.factor" is used as a multiplier for the
   * class provided default
   * <p>
   * Note that if either of these values is '0' tests will run with (effectively) no timeout (a
   * timeout of 5 days will be applied). The default class level timeout is set to 0.
   *
   */
  @RegisterExtension
  Timeout timeout = Timeout.from(() -> {
    assertFalse(defaultTimeout().isZero(), "defaultTimeout should not return 0");

    int timeoutFactor = 0;
    try {
      String timeoutString = System.getProperty("timeout.factor");
      if (timeoutString != null && !timeoutString.isEmpty()) {
        timeoutFactor = Integer.parseInt(timeoutString);
      }
    } catch (NumberFormatException exception) {
      log.warn("Could not parse timeout.factor, defaulting to no timeout.");
    }

    // if the user sets a timeout factor of 0, apply a very long timeout (effectively no timeout)
    if (timeoutFactor == 0) {
      return Duration.ofDays(5);
    }

    return defaultTimeout().multipliedBy(timeoutFactor);
  });

  /**
   * Time to wait per-method before declaring a timeout, in seconds.
   */
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(10);
  }

  @SuppressFBWarnings(value = "UI_INHERITANCE_UNSAFE_GETRESOURCE", justification = "for testing")
  protected File initJar(String jarResourcePath, String namePrefix, String testDir)
      throws IOException {
    var testFileDir = new File(testDir);
    File jar = File.createTempFile(namePrefix, ".jar", testFileDir);
    var url = this.getClass().getResource(jarResourcePath);
    if (url == null) {
      throw new IllegalStateException("Can't find the jar: " + jarResourcePath);
    }
    FileUtils.copyInputStreamToFile(url.openStream(), jar);
    jar.deleteOnExit();

    return jar;
  }
}

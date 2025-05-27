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
package org.apache.accumulo.suites;

import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

/**
 * This test suite is used to run applicable ITs against a single, shared cluster, starting and
 * stopping the cluster only once for the duration of the suite. This avoids starting and stopping a
 * cluster per IT, providing some speedup. An IT is applicable if:
 * <p>
 * 1) It is a subclass of {@link SharedMiniClusterBase}, meaning it starts and stops a single
 * cluster for the entire IT.
 * <p>
 * 2) It does not start the cluster with any custom config (i.e., it does not use
 * {@link SharedMiniClusterBase#startMiniClusterWithConfig(MiniClusterConfigurationCallback)})
 * <p>
 * An IT which meets this criteria should have the class name end with "IT_SimpleSuite" to be added
 * to the suite.
 *
 * IMPORTANT NOTE: Only the concrete classes should be marked with "IT_SimpleSuite". For example,
 * marking an abstract class will not add its implementations to the suite.
 */
@Suite
// look in this package and subpackages
@SelectPackages("org.apache.accumulo.test")
// need to override the default pattern ".*Test"
// avoid ending in "IT" so they are not picked up in the build. Only want this to run.
@IncludeClassNamePatterns("^.+IT_SimpleSuite$")
public class SimpleSharedMacTestSuiteIT extends SharedMiniClusterBase {

  @BeforeSuite
  public static void beforeAllTests() throws Exception {
    SharedMiniClusterBase.startMiniCluster();

    // Disable tests that are run as part of this suite
    // from stopping MiniAccumuloCluster in there JUnit
    // lifecycle methods (e.g. AfterEach, AfterAll)
    SharedMiniClusterBase.STOP_DISABLED.set(true);
  }

  @AfterSuite
  public static void afterAllTests() throws Exception {
    SharedMiniClusterBase.STOP_DISABLED.set(false);
    SharedMiniClusterBase.stopMiniCluster();
  }
}

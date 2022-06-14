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
package org.apache.accumulo.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

// This is only for the unit tests and integration tests in this module
// It must be copied for use in other modules, because tests in one module
// don't have dependencies on other modules, and we can't put this in a
// regular, non-test jar, because we don't want to add a dependency on
// JUnit in a non-test jar
public class WithTestNames {

  private String testName;

  @BeforeEach
  public void setTestName(TestInfo info) {
    testName = info.getTestMethod().get().getName();
  }

  protected String testName() {
    return testName;
  }

}

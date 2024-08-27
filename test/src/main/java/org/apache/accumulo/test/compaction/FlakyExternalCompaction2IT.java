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
package org.apache.accumulo.test.compaction;

import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.ample.FlakyAmpleManager;
import org.apache.accumulo.test.ample.FlakyAmpleServerContext;
import org.apache.accumulo.test.ample.FlakyAmpleTserver;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Run all of ExternalCompaction2 ITs using a flaky Ample impl that will cause random UNKNOWN status
 * to be returned when submitting conditional mutations so that rejection handlers can be tested.
 * See {@link FlakyAmpleServerContext} for more info.
 *
 * The tests in this IT primarily test cancelling compactions and the flaky Ample impl will test
 * that rejection handlers such as the handler used for
 * CompactionCoordinator.compactionFailedForLevel work.
 */
public class FlakyExternalCompaction2IT extends ExternalCompaction2BaseIT {

  static class FlakyExternalCompaction2Config extends ExternalCompaction2Config {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      super.configureMiniCluster(cfg, coreSite);
      cfg.setServerClass(ServerType.MANAGER, FlakyAmpleManager.class);
      cfg.setServerClass(ServerType.TABLET_SERVER, FlakyAmpleTserver.class);
    }
  }

  @BeforeAll
  public static void setup() throws Exception {
    startMiniClusterWithConfig(new FlakyExternalCompaction2Config());
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }
}

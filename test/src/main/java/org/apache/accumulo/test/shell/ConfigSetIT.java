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
package org.apache.accumulo.test.shell;

import static org.apache.accumulo.core.conf.Property.MONITOR_RESOURCES_EXTERNAL;
import static org.apache.accumulo.core.conf.Property.TSERV_COMPACTION_SERVICE_ROOT_EXECUTORS;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MINI_CLUSTER_ONLY)
public class ConfigSetIT extends SharedMiniClusterBase {
  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private static final Logger log = LoggerFactory.getLogger(ConfigSetIT.class);

  @Test
  @SuppressWarnings("removal")
  public void setInvalidJson() throws Exception {
    log.debug("Starting setInvalidJson test ------------------");

    String validJson =
        "[{'name':'small','type':'internal','maxSize':'64M','numThreads':2},{'name':'huge','type':'internal','numThreads':2}]"
            .replaceAll("'", "\"");

    // missing first value
    String invalidJson = "notJson";

    try (AccumuloClient client =
        getCluster().createAccumuloClient("root", new PasswordToken(getRootPassword()))) {
      client.instanceOperations().setProperty(TSERV_COMPACTION_SERVICE_ROOT_EXECUTORS.getKey(),
          validJson);
      assertThrows(AccumuloException.class, () -> client.instanceOperations()
          .setProperty(MONITOR_RESOURCES_EXTERNAL.getKey(), invalidJson));

    }
  }
}

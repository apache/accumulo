/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic functional test to verify enabling legacy metrics does not kill master.
 */
public class LegacyMetricsIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(LegacyMetricsIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.GENERAL_LEGACY_METRICS, "true");
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  /**
   * Validates that the expected metrics are published - this excludes the dynamic metrics derived
   * from operation types.
   */
  @Test
  public void useMaster() {

    boolean legacyMetricsEnabled =
        cluster.getSiteConfiguration().getBoolean(Property.GENERAL_LEGACY_METRICS);

    assertTrue(legacyMetricsEnabled);

    List<String> tables = new ArrayList<>();

    // number of tables / concurrent compactions used during testing.
    int tableCount = 4;
    for (int i = 0; i < tableCount; i++) {
      String uniqueName = getUniqueNames(1)[0] + "_" + i;
      tables.add(uniqueName);
      try {
        getConnector().tableOperations().create(uniqueName);
      } catch (AccumuloException | AccumuloSecurityException | TableExistsException ex) {
        log.debug("Failed to create table: {}", uniqueName, ex);
        fail("failed to create table: " + uniqueName);
      }
    }

    // use calls that should need the master

    try {

      getConnector().instanceOperations().waitForBalance();

      Map<String,String> configs = getConnector().instanceOperations().getSystemConfiguration();
      assertFalse("master config should not be empty", configs.isEmpty());

    } catch (AccumuloException | AccumuloSecurityException ex) {
      fail("Could not get config from master");
    }

    // clean-up cancel running compactions
    for (String t : tables) {
      try {
        log.debug("Delete test table: {}", t);
        getConnector().tableOperations().delete(t);
      } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException ex) {
        log.debug("Exception thrown deleting table during test clean-up", ex);
      }
    }
  }
}

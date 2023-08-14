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

import java.io.File;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.spi.fs.DelegatingChooser;
import org.apache.accumulo.core.spi.fs.PreferredVolumeChooser;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// This covers issues like that reported in https://github.com/apache/accumulo/issues/3674
// where a failing minor compaction leaves the Tablet in a half-closed state that prevents it
// from unloading and the TServer from shutting down normally.
// This test recreates that scenario by setting an invalid context and verifies that the
// tablet can recover and unload after the context is set correctly.
public class HalfClosedTablet2IT extends SharedMiniClusterBase {

  public static class HalfClosedTablet2ITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.setNumTservers(1);
      cfg.setProperty(Property.GENERAL_VOLUME_CHOOSER, DelegatingChooser.class.getName());
      cfg.setProperty("general.custom.volume.chooser.default",
          PreferredVolumeChooser.class.getName());
      cfg.setProperty("general.custom.volume.preferred.default",
          new File(cfg.getDir().getAbsolutePath(), "/accumulo").toURI().toString());
    }
  }

  @BeforeAll
  public static void startup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new HalfClosedTablet2ITConfiguration());
  }

  @AfterAll
  public static void shutdown() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testInvalidContextCausesVolumeChooserFailure() throws Exception {

    // In this scenario an invalid context causes the VolumeChooser impl to not
    // be loaded, which causes the MinorCompactionTask to fail to create an
    // output file. This failure previously caused the minor compaction thread to die.

    String tableName = getUniqueNames(1)[0];
    try (final var client = Accumulo.newClient().from(getClientProps()).build()) {

      final var tops = client.tableOperations();
      tops.create(tableName);
      TableId tableId = TableId.of(tops.tableIdMap().get(tableName));

      try (final var bw = client.createBatchWriter(tableName)) {
        final var m1 = new Mutation("a");
        final var m2 = new Mutation("b");
        m1.put(new Text("cf"), new Text(), new Value());
        m2.put(new Text("cf"), new Text(), new Value());
        bw.addMutation(m1);
        bw.addMutation(m2);
      }

      HalfClosedTabletIT.setInvalidClassLoaderContextPropertyWithoutValidation(
          getCluster().getServerContext(), tableId);

      // Need to wait for TabletServer to pickup configuration change
      Thread.sleep(3000);

      tops.flush(tableName);

      // minc should fail until invalid context is removed, so there should be no files
      FunctionalTestUtils.checkRFiles(client, tableName, 1, 1, 0, 0);

      HalfClosedTabletIT.removeInvalidClassLoaderContextProperty(client, tableName);

      // Minc should have completed successfully
      Wait.waitFor(() -> HalfClosedTabletIT.tabletHasExpectedRFiles(client, tableName, 1, 1, 1, 1),
          340_000);

      // offline the table which will unload the tablets. If the context property is not
      // removed above, then this test will fail because the tablets will not be able to be
      // unloaded
      tops.offline(tableName);

      Wait.waitFor(() -> HalfClosedTabletIT.countHostedTablets(client, tableId) == 0L, 340_000);

    }

  }

}

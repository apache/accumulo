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
package org.apache.accumulo.test;

import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP2;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP3;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP4;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP5;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP6;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP7;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.ListCompactors;
import org.apache.accumulo.test.compaction.ExternalCompactionTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ListCompactorsIT extends SharedMiniClusterBase {

  private static final class ListCompactorsITConfig implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
    }
  }

  private static class ListCompactorsWrapper extends ListCompactors {
    @Override
    protected Map<ResourceGroupId,List<ServerId>> listCompactorsByQueue(ServerContext context) {
      return super.listCompactorsByQueue(context);
    }
  }

  @BeforeAll
  public static void beforeAll() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new ListCompactorsITConfig());
  }

  @AfterAll
  public static void afterAll() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testListCompactors() throws Exception {
    final Map<ResourceGroupId,List<ServerId>> compactors =
        new ListCompactorsWrapper().listCompactorsByQueue(getCluster().getServerContext());
    System.out.println(compactors);
    assertEquals(9, compactors.size());
    assertTrue(compactors.containsKey(ResourceGroupId.DEFAULT));
    assertTrue(compactors.containsKey(ResourceGroupId.of(GROUP1)));
    assertTrue(compactors.containsKey(ResourceGroupId.of(GROUP2)));
    assertTrue(compactors.containsKey(ResourceGroupId.of(GROUP3)));
    assertTrue(compactors.containsKey(ResourceGroupId.of(GROUP4)));
    assertTrue(compactors.containsKey(ResourceGroupId.of(GROUP5)));
    assertTrue(compactors.containsKey(ResourceGroupId.of(GROUP6)));
    assertTrue(compactors.containsKey(ResourceGroupId.of(GROUP7)));
    assertTrue(compactors.containsKey(ResourceGroupId.of(GROUP8)));
  }

}

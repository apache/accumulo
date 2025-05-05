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


import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;
import static org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel.EVENTUAL;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.CloneConfiguration;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.client.summary.CountingSummarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint;
import org.apache.accumulo.core.data.constraints.VisibilityConstraint;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.spi.scan.ConfigurableScanServerSelector;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

@Tag(SUNNY_DAY)
public class ComprehensiveIT extends ComprehensiveBaseIT {
 private static class ComprehensiveITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.setNumScanServers(1);
      cfg.setProperty(Property.SSERV_CACHED_TABLET_METADATA_EXPIRATION, "5s");
    }
  }
  
  @BeforeAll
  public static void setup() throws Exception {
    ComprehensiveITConfiguration c = new ComprehensiveITConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.securityOperations().changeUserAuthorizations("root", AUTHORIZATIONS);
    }
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testEventualScan() throws Exception {
    Properties props = new Properties();
    props.putAll(getClientProps());
    props.put(ClientProperty.SCAN_SERVER_SELECTOR.getKey(),
        ConfigurableScanServerSelector.class.getName());
    props.put(ClientProperty.SCAN_SERVER_SELECTOR_OPTS_PREFIX.getKey() + "profiles",
        "[{'isDefault':true,'timeToWaitForScanServers' : '10d','maxBusyTimeout':'5m','busyTimeoutMultiplier':4,'attemptPlans':[{\"servers\":\"100%\", \"busyTimeout\":\"3ms\"}]}]");

    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      String table = getUniqueNames(1)[0];
      client.tableOperations().create(table);

      getCluster().getClusterControl().start(ServerType.SCAN_SERVER);
      Wait.waitFor(() -> !client.instanceOperations().getScanServers().isEmpty());

      // generate 100 rows of data
      write(client, table, generateMutations(0, 100, tr -> true));
      client.tableOperations().flush(table, null, null, true);

      // should see all data that was flushed in eventual scan
      verifyData(client, table, AUTHORIZATIONS, generateKeys(0, 100),
          scanner -> scanner.setConsistencyLevel(EVENTUAL));
      // should not see data with col vis set
      verifyData(client, table, Authorizations.EMPTY, generateKeys(0, 100, tr -> tr.vis.isEmpty()),
          scanner -> scanner.setConsistencyLevel(EVENTUAL));

      // write some more rows after 100 and verify those are not seen by eventual scan until table
      // is flushed.
      write(client, table, generateMutations(100, 200, tr -> true));
      verifyData(client, table, AUTHORIZATIONS, generateKeys(0, 100),
          scanner -> scanner.setConsistencyLevel(EVENTUAL));

      client.tableOperations().flush(table, null, null, true);
      // wait for the eventual scan to see the new data
      final int initialSize = generateKeys(0, 100).size();
      Wait.waitFor(() -> {
        try (var scanner = client.createScanner(table, AUTHORIZATIONS)) {
          scanner.setConsistencyLevel(EVENTUAL);
          return scan(scanner).size() > initialSize;
        }
      });

      verifyData(client, table, AUTHORIZATIONS, generateKeys(0, 200),
          scanner -> scanner.setConsistencyLevel(EVENTUAL));

    } finally {
      getCluster().getClusterControl().stop(ServerType.SCAN_SERVER);
    }
  }
}

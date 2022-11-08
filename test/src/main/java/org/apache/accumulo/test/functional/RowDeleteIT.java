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

import static org.apache.accumulo.test.functional.FunctionalTestUtils.checkRFiles;
import static org.apache.accumulo.test.functional.FunctionalTestUtils.nm;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterators;

public class RowDeleteIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "50ms");
    cfg.setSiteConfig(siteConfig);
  }

  @Test
  public void run() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      NewTableConfiguration ntc = new NewTableConfiguration();
      IteratorSetting setting = new IteratorSetting(30, RowDeletingIterator.class);
      ntc.attachIterator(setting, EnumSet.of(IteratorScope.majc));
      ntc.setLocalityGroups(Map.of("lg1", Set.of(new Text("foo"))));
      ntc.setProperties(Map.of(Property.TABLE_MAJC_RATIO.getKey(), "100"));
      c.tableOperations().create(tableName, ntc);

      try (BatchWriter bw = c.createBatchWriter(tableName);
          Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {

        bw.addMutation(nm("r1", "foo", "cf1", "v1"));
        bw.addMutation(nm("r1", "bar", "cf1", "v2"));

        bw.flush();
        c.tableOperations().flush(tableName, null, null, true);

        checkRFiles(c, tableName, 1, 1, 1, 1);

        int count;
        count = Iterators.size(scanner.iterator());
        assertEquals(2, count, "count == " + count);

        bw.addMutation(nm("r1", "", "", RowDeletingIterator.DELETE_ROW_VALUE));

        bw.flush();
        c.tableOperations().flush(tableName, null, null, true);

        checkRFiles(c, tableName, 1, 1, 2, 2);

        count = Iterators.size(scanner.iterator());
        assertEquals(3, count, "count == " + count);

        c.tableOperations().compact(tableName, null, null, false, true);

        checkRFiles(c, tableName, 1, 1, 0, 0);

        assertTrue(scanner.stream().findAny().isEmpty());
      }
    }
  }
}

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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.balancer.RegexGroupBalancer;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class RegexGroupBalanceIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void beforeClusterStart(MiniAccumuloConfigImpl cfg) {
    cfg.setNumTservers(4);
  }

  @Test
  public void testBalancing() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String tablename = getUniqueNames(1)[0];

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("01a"));
      splits.add(new Text("01m"));
      splits.add(new Text("01z"));

      splits.add(new Text("02a"));
      splits.add(new Text("02f"));
      splits.add(new Text("02r"));
      splits.add(new Text("02z"));

      splits.add(new Text("03a"));
      splits.add(new Text("03f"));
      splits.add(new Text("03m"));
      splits.add(new Text("03r"));

      HashMap<String,String> props = new HashMap<>();
      props.put(RegexGroupBalancer.REGEX_PROPERTY, "(\\d\\d).*");
      props.put(RegexGroupBalancer.DEFAUT_GROUP_PROPERTY, "03");
      props.put(RegexGroupBalancer.WAIT_TIME_PROPERTY, "50ms");
      props.put(Property.TABLE_LOAD_BALANCER.getKey(), RegexGroupBalancer.class.getName());

      client.tableOperations().create(tablename,
          new NewTableConfiguration().setProperties(props).withSplits(splits));

      while (true) {
        Thread.sleep(250);

        Table<String,String,MutableInt> groupLocationCounts = getCounts(client, tablename);

        boolean allGood = true;
        allGood &= checkGroup(groupLocationCounts, "01", 1, 1, 3);
        allGood &= checkGroup(groupLocationCounts, "02", 1, 1, 4);
        allGood &= checkGroup(groupLocationCounts, "03", 1, 2, 4);
        allGood &= checkTabletsPerTserver(groupLocationCounts, 3, 3, 4);

        if (allGood) {
          break;
        }
      }

      splits.clear();
      splits.add(new Text("01b"));
      splits.add(new Text("01f"));
      splits.add(new Text("01l"));
      splits.add(new Text("01r"));
      client.tableOperations().addSplits(tablename, splits);

      while (true) {
        Thread.sleep(250);

        Table<String,String,MutableInt> groupLocationCounts = getCounts(client, tablename);

        boolean allGood = true;
        allGood &= checkGroup(groupLocationCounts, "01", 1, 2, 4);
        allGood &= checkGroup(groupLocationCounts, "02", 1, 1, 4);
        allGood &= checkGroup(groupLocationCounts, "03", 1, 2, 4);
        allGood &= checkTabletsPerTserver(groupLocationCounts, 4, 4, 4);

        if (allGood) {
          break;
        }
      }

      // merge group 01 down to one tablet
      client.tableOperations().merge(tablename, null, new Text("01z"));

      while (true) {
        Thread.sleep(250);

        Table<String,String,MutableInt> groupLocationCounts = getCounts(client, tablename);

        boolean allGood = true;
        allGood &= checkGroup(groupLocationCounts, "01", 1, 1, 1);
        allGood &= checkGroup(groupLocationCounts, "02", 1, 1, 4);
        allGood &= checkGroup(groupLocationCounts, "03", 1, 2, 4);
        allGood &= checkTabletsPerTserver(groupLocationCounts, 2, 3, 4);

        if (allGood) {
          break;
        }
      }
    }
  }

  private boolean checkTabletsPerTserver(Table<String,String,MutableInt> groupLocationCounts,
      int minTabletPerTserver, int maxTabletsPerTserver, int totalTservser) {
    // check that each tserver has between min and max tablets
    for (Map<String,MutableInt> groups : groupLocationCounts.columnMap().values()) {
      int sum = 0;
      for (MutableInt mi : groups.values()) {
        sum += mi.intValue();
      }

      if (sum < minTabletPerTserver || sum > maxTabletsPerTserver) {
        return false;
      }
    }

    return groupLocationCounts.columnKeySet().size() == totalTservser;
  }

  private boolean checkGroup(Table<String,String,MutableInt> groupLocationCounts, String group,
      int min, int max, int tsevers) {
    Collection<MutableInt> counts = groupLocationCounts.row(group).values();
    if (counts.isEmpty()) {
      return min == 0 && max == 0 && tsevers == 0;
    }
    return min == Collections.min(counts).intValue() && max == Collections.max(counts).intValue()
        && counts.size() == tsevers;
  }

  private Table<String,String,MutableInt> getCounts(AccumuloClient client, String tablename)
      throws TableNotFoundException {
    try (Scanner s = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tablename));
      s.setRange(TabletsSection.getRange(tableId));

      Table<String,String,MutableInt> groupLocationCounts = HashBasedTable.create();

      for (Entry<Key,Value> entry : s) {
        String group = entry.getKey().getRow().toString();
        if (group.endsWith("<")) {
          group = "03";
        } else {
          group = group.substring(tableId.canonical().length() + 1).substring(0, 2);
        }
        String loc = new TServerInstance(entry.getValue(), entry.getKey().getColumnQualifier())
            .getHostPortSession();

        MutableInt count = groupLocationCounts.get(group, loc);
        if (count == null) {
          count = new MutableInt(0);
          groupLocationCounts.put(group, loc, count);
        }

        count.increment();
      }
      return groupLocationCounts;
    }
  }
}

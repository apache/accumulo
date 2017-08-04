/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.test.functional;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.master.balancer.RegexGroupBalancer;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class RegexGroupBalanceIT extends ConfigurableMacBase {

  @Override
  public void beforeClusterStart(MiniAccumuloConfigImpl cfg) throws Exception {
    cfg.setNumTservers(4);
  }

  @Test(timeout = 120000)
  public void testBalancing() throws Exception {
    Connector conn = getConnector();
    String tablename = getUniqueNames(1)[0];
    conn.tableOperations().create(tablename);

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

    conn.tableOperations().setProperty(tablename, RegexGroupBalancer.REGEX_PROPERTY, "(\\d\\d).*");
    conn.tableOperations().setProperty(tablename, RegexGroupBalancer.DEFAUT_GROUP_PROPERTY, "03");
    conn.tableOperations().setProperty(tablename, RegexGroupBalancer.WAIT_TIME_PROPERTY, "50ms");
    conn.tableOperations().setProperty(tablename, Property.TABLE_LOAD_BALANCER.getKey(), RegexGroupBalancer.class.getName());

    conn.tableOperations().addSplits(tablename, splits);

    while (true) {
      Thread.sleep(250);

      Table<String,String,MutableInt> groupLocationCounts = getCounts(conn, tablename);

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
    conn.tableOperations().addSplits(tablename, splits);

    while (true) {
      Thread.sleep(250);

      Table<String,String,MutableInt> groupLocationCounts = getCounts(conn, tablename);

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
    conn.tableOperations().merge(tablename, null, new Text("01z"));

    while (true) {
      Thread.sleep(250);

      Table<String,String,MutableInt> groupLocationCounts = getCounts(conn, tablename);

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

  private boolean checkTabletsPerTserver(Table<String,String,MutableInt> groupLocationCounts, int minTabletPerTserver, int maxTabletsPerTserver,
      int totalTservser) {
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

  private boolean checkGroup(Table<String,String,MutableInt> groupLocationCounts, String group, int min, int max, int tsevers) {
    Collection<MutableInt> counts = groupLocationCounts.row(group).values();
    if (counts.size() == 0) {
      return min == 0 && max == 0 && tsevers == 0;
    }
    return min == Collections.min(counts).intValue() && max == Collections.max(counts).intValue() && counts.size() == tsevers;
  }

  private Table<String,String,MutableInt> getCounts(Connector conn, String tablename) throws TableNotFoundException {
    Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
    org.apache.accumulo.core.client.impl.Table.ID tableId = org.apache.accumulo.core.client.impl.Table.ID
        .of(conn.tableOperations().tableIdMap().get(tablename));
    s.setRange(MetadataSchema.TabletsSection.getRange(tableId));

    Table<String,String,MutableInt> groupLocationCounts = HashBasedTable.create();

    for (Entry<Key,Value> entry : s) {
      String group = entry.getKey().getRow().toString();
      if (group.endsWith("<")) {
        group = "03";
      } else {
        group = group.substring(tableId.canonicalID().length() + 1).substring(0, 2);
      }
      String loc = new TServerInstance(entry.getValue(), entry.getKey().getColumnQualifier()).toString();

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

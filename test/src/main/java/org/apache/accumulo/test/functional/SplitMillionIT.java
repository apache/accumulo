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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.admin.CloneConfiguration;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class SplitMillionIT extends ConfigurableMacBase {

  private static final Logger log = LoggerFactory.getLogger(SplitMillionIT.class);

  public static class XFilter extends Filter {
    private static final ByteSequence X_BS = new ArrayByteSequence("x");

    @Override
    public boolean accept(Key k, Value v) {
      return !k.getColumnQualifierData().equals(X_BS);
    }
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setMemory(ServerType.MANAGER, 1, MemoryUnit.GIGABYTE);
    cfg.setMemory(ServerType.TABLET_SERVER, 1, MemoryUnit.GIGABYTE);
  }

  @SuppressFBWarnings(value = {"PREDICTABLE_RANDOM", "DMI_RANDOM_USED_ONLY_ONCE"},
      justification = "predictable random is ok for testing")
  @Test
  public void testOneMillionTablets() throws Exception {

    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);

      // pre split the metadata table
      var tableId = getServerContext().getTableId(tableName);
      SortedSet<Text> metaSplits = new TreeSet<>();
      for (int i = 1; i < 10; i++) {
        String metaSplit = String.format("%s;%010d", tableId, 100_000_000 / 10 * i);
        metaSplits.add(new Text(metaSplit));
      }
      c.tableOperations().addSplits(SystemTables.METADATA.tableName(), metaSplits);

      SortedSet<Text> splits = new TreeSet<>();

      for (int i = 100; i < 100_000_000; i += 100) {
        String split = String.format("%010d", i);

        splits.add(new Text(split));

        if (splits.size() >= 10000) {
          addSplits(c, tableName, splits, log);
        }
      }

      if (!splits.isEmpty()) {
        addSplits(c, tableName, splits, log);
      }

      var rows = IntStream
          .concat(new Random().ints(98, 0, 100_000_000).flatMap(i -> IntStream.of(i, i + 1)),
              IntStream.of(0, 1, 99_999_998, 99_999_999))
          .toArray();

      long t1 = System.currentTimeMillis();
      try (var scanner = c.createBatchScanner(tableName)) {
        var ranges = Arrays.stream(rows).mapToObj(rowInt -> String.format("%010d", rowInt))
            .map(Range::new).collect(Collectors.toList());
        scanner.setRanges(ranges);
        assertEquals(0, scanner.stream().count());
      }
      long t2 = System.currentTimeMillis();
      log.info("Time to scan {} rows {}ms", rows.length, (t2 - t1));

      t1 = System.currentTimeMillis();
      try (var writer = c.createBatchWriter(tableName)) {
        for (var rowInt : rows) {
          var row = String.format("%010d", rowInt);
          Mutation m = new Mutation(row);
          m.put("c", "x", "200");
          m.put("c", "y", "900");
          m.put("c", "z", "300");
          writer.addMutation(m);
        }
      }
      t2 = System.currentTimeMillis();
      log.info("Time to write {} rows {}ms", rows.length, (t2 - t1));

      // read and write to a few of the 1 million tablets. The following should touch the first,
      // last, and a few middle tablets.
      for (var rowInt : rows) {
        var row = String.format("%010d", rowInt);

        long t3 = System.currentTimeMillis();
        verifyRow(c, tableName, row, Map.of("x", "200", "y", "900", "z", "300"));
        long t4 = System.currentTimeMillis();
        log.info("Row: {}  scan: {}ms", row, t4 - t3);
      }

      long count;
      t1 = System.currentTimeMillis();
      try (var tabletInformation =
          c.tableOperations().getTabletInformation(tableName, new Range())) {
        count = tabletInformation.count();
      }
      t2 = System.currentTimeMillis();
      assertEquals(1_000_000, count);
      log.info("Time to scan all tablets information : {}ms", t2 - t1);

      t1 = System.currentTimeMillis();
      var iterSetting = new IteratorSetting(100, XFilter.class);
      c.tableOperations().compact(tableName,
          new CompactionConfig().setIterators(List.of(iterSetting)).setWait(true).setFlush(true));
      t2 = System.currentTimeMillis();
      log.info("Time to compact all tablets : {}ms", t2 - t1);

      var expected = Map.of("y", "900", "z", "300");
      vefifyData(rows, c, tableName, expected, ConsistencyLevel.IMMEDIATE);

      // We are done with tableName, except for deleting it later. Modify the tablets
      // availability to UNHOSTED so that when the clone operation happens below none
      // of the tablets for the clone table will be hosted. The subsequent merge operation
      // is a metadata-only operation unless the tablet is hosted. If the tablet is hosted
      // then the tablet has to be closed making the merge operation take longer.
      c.tableOperations().setTabletAvailability(tableName, new Range(),
          TabletAvailability.UNHOSTED);

      // clone the table to test cloning with lots of tablets and also to give merge its own table
      // to work on
      var cloneName = tableName + "_clone";
      t1 = System.currentTimeMillis();
      c.tableOperations().clone(tableName, cloneName, CloneConfiguration.builder().build());
      t2 = System.currentTimeMillis();
      log.info("Time to clone table : {}ms", t2 - t1);
      vefifyData(rows, c, cloneName, expected, ConsistencyLevel.EVENTUAL);

      TableId tid = TableId.of(c.tableOperations().tableIdMap().get(cloneName));
      assertEquals(0, ManagerAssignmentIT.countTabletsWithLocation(c, tid));

      // merge the clone, so that delete table can run later on tablet with lots and lots of tablets
      t1 = System.currentTimeMillis();
      c.tableOperations().merge(cloneName, null, null);
      t2 = System.currentTimeMillis();
      log.info("Time to merge all tablets : {}ms", t2 - t1);

      vefifyData(rows, c, cloneName, expected, ConsistencyLevel.EVENTUAL);

      t1 = System.currentTimeMillis();
      c.tableOperations().delete(tableName);
      t2 = System.currentTimeMillis();
      log.info("Time to delete original table : {}ms", t2 - t1);

      t1 = System.currentTimeMillis();
      c.tableOperations().delete(cloneName);
      t2 = System.currentTimeMillis();
      log.info("Time to delete clone table : {}ms", t2 - t1);
    }
  }

  private void vefifyData(int[] rows, AccumuloClient c, String tableName,
      Map<String,String> expected, ConsistencyLevel level) throws Exception {
    // use a batch scanner so that many hosting request can be submitted at the same time
    long t1 = System.currentTimeMillis();
    try (var scanner = c.createBatchScanner(tableName)) {
      var ranges = IntStream.of(rows).mapToObj(row -> String.format("%010d", row)).map(Range::new)
          .collect(Collectors.toList());
      scanner.setRanges(ranges);
      scanner.setConsistencyLevel(level);
      Map<String,Map<String,String>> allCoords = new HashMap<>();
      scanner.forEach((k, v) -> {
        var row = k.getRowData().toString();
        var qual = k.getColumnQualifierData().toString();
        var val = v.toString();
        allCoords.computeIfAbsent(row, r -> new HashMap<>()).put(qual, val);
      });

      assertEquals(IntStream.of(rows).mapToObj(row -> String.format("%010d", row))
          .collect(Collectors.toSet()), allCoords.keySet());
      allCoords.values().forEach(coords -> assertEquals(expected, coords));
    }
    long t2 = System.currentTimeMillis();
    log.info("Time to verify {} rows was {}ms", rows.length, t2 - t1);
  }

  private void verifyRow(AccumuloClient c, String tableName, String row,
      Map<String,String> expected) throws Exception {
    try (var scanner = c.createScanner(tableName)) {
      scanner.setRange(new Range(row));
      Map<String,String> coords = scanner.stream().collect(Collectors
          .toMap(e -> e.getKey().getColumnQualifier().toString(), e -> e.getValue().toString()));
      assertEquals(expected, coords);
    }
  }

  private static void addSplits(AccumuloClient c, String tableName, SortedSet<Text> splits,
      Logger log) throws Exception {
    long t1 = System.currentTimeMillis();
    c.tableOperations().addSplits(tableName, splits);
    long t2 = System.currentTimeMillis();
    log.info("Added {} splits in {}ms", splits.size(), t2 - t1);
    splits.clear();
  }
}

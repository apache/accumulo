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
package org.apache.accumulo.manager;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;

public class MultipleManagerUtil {

  /**
   * Each Manager will be responsible for a range(s) of metadata tablets, but we don't want to split
   * up a table's metadata tablets between managers as it will throw off the tablet balancing. If
   * there are three managers, then we want to split up the metadata tablets roughly into thirds and
   * have each manager responsible for one third, for example.
   *
   * @param context server context
   * @param tables set of table ids
   * @param numManagers number of managers
   * @return list of num manager size, each element containing a set of tables for the manager to
   *         manage
   */
  public static List<Set<TableId>> getTablesForManagers(ServerContext context, Set<TableId> tables,
      int numManagers) {

    if (numManagers == 0) {
      throw new IllegalStateException("No managers, one or more expected");
    }

    if (numManagers == 1) {
      return List.of(tables);
    }

    SortedSet<Pair<TableId,Long>> tableTabletCounts = new TreeSet<>(new Comparator<>() {
      @Override
      public int compare(Pair<TableId,Long> table1, Pair<TableId,Long> table2) {
        // sort descending by number of tablets
        int result = table1.getSecond().compareTo(table2.getSecond());
        if (result == 0) {
          return table1.getFirst().compareTo(table2.getFirst());
        }
        return -1 * result;
      }
    });
    tables.forEach(tid -> {
      long count = context.getAmple().readTablets().forTable(tid).build().stream().count();
      tableTabletCounts.add(new Pair<>(tid, count));
    });
    List<Set<Pair<TableId,Long>>> buckets = new ArrayList<>(numManagers);
    IntStream.range(0, numManagers).forEach(i -> buckets.add(new HashSet<>()));

    for (Pair<TableId,Long> tableTabletCount : tableTabletCounts) {
      // Find the bucket with the lowest count and add this to it
      int lowestBucket = -1;
      int bucketIdx = 0;
      Long priorBucketCount = Long.MAX_VALUE;
      for (Set<Pair<TableId,Long>> bucket : buckets) {
        long bucketTabletCount = bucket.stream().mapToLong(Pair::getSecond).sum();
        if (bucketTabletCount < priorBucketCount) {
          lowestBucket = bucketIdx;
        }
        bucketIdx++;
        priorBucketCount = bucketTabletCount;
      }
      buckets.get(lowestBucket).add(tableTabletCount);
    }

    List<Set<TableId>> results = new ArrayList<>();
    buckets.stream()
        .forEach(b -> results.add(b.stream().map(Pair::getFirst).collect(Collectors.toSet())));
    return results;
  }

}

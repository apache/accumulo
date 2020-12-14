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
package org.apache.accumulo.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.util.Duration;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ActiveCompactionHelper {

  private static final Logger log = LoggerFactory.getLogger(ActiveCompactionHelper.class);

  private static String maxDecimal(double count) {
    if (count < 9.995)
      return String.format("%.2f", count);
    if (count < 99.95)
      return String.format("%.1f", count);
    return String.format("%.0f", count);
  }

  private static String shortenCount(long count) {
    if (count < 1_000)
      return count + "";
    if (count < 1_000_000)
      return maxDecimal(count / 1_000.0) + "K";
    if (count < 1_000_000_000)
      return maxDecimal(count / 1_000_000.0) + "M";
    return maxDecimal(count / 1_000_000_000.0) + "B";
  }

  private static String formatActiveCompactionLine(String tserver, ActiveCompaction ac)
      throws TableNotFoundException {
    String output = ac.getOutputFile();
    int index = output.indexOf("tables");
    if (index > 0) {
      output = output.substring(index + 6);
    }

    List<String> iterList = new ArrayList<>();
    Map<String,Map<String,String>> iterOpts = new HashMap<>();
    for (IteratorSetting is : ac.getIterators()) {
      iterList.add(is.getName() + "=" + is.getPriority() + "," + is.getIteratorClass());
      iterOpts.put(is.getName(), is.getOptions());
    }

    return String.format(
        "%21s | %9s | %5s | %6s | %5s | %5s | %15s | %-40s | %5s | %35s | %9s | %s", tserver,
        Duration.format(ac.getAge(), "", "-"), ac.getType(), ac.getReason(),
        shortenCount(ac.getEntriesRead()), shortenCount(ac.getEntriesWritten()), ac.getTable(),
        ac.getTablet(), ac.getInputFiles().size(), output, iterList, iterOpts);
  }

  private static List<String> activeCompactionsForServer(String tserver,
      InstanceOperations instanceOps) {
    List<String> compactions = new ArrayList<>();
    try {
      List<ActiveCompaction> acl = new ArrayList<>(instanceOps.getActiveCompactions(tserver));
      acl.sort((o1, o2) -> (int) (o2.getAge() - o1.getAge()));
      for (ActiveCompaction ac : acl) {
        compactions.add(formatActiveCompactionLine(tserver, ac));
      }
    } catch (Exception e) {
      log.debug("Failed to list active compactions for server {}", tserver, e);
      compactions.add(tserver + " ERROR " + e.getMessage());
    }
    return compactions;
  }

  public static Stream<String> stream(List<String> tservers, InstanceOperations instanceOps) {
    Stream<String> header = Stream.of(String.format(
        " %-21s| %-9s | %-5s | %-6s | %-5s | %-5s | %-15s | %-40s | %-5s | %-35s | %-9s | %s",
        "TABLET SERVER", "AGE", "TYPE", "REASON", "READ", "WROTE", "TABLE", "TABLET", "INPUT",
        "OUTPUT", "ITERATORS", "ITERATOR OPTIONS"));

    // use at least 4 threads (if needed), but no more than 256
    int numThreads = Math.max(4, Math.min(tservers.size() / 10, 256));
    var threadFactory = new NamingThreadFactory("shell-listcompactions");
    var executorService = Executors.newFixedThreadPool(numThreads, threadFactory);
    try {
      Stream<String> activeCompactionLines = tservers.stream()
          // submit each tserver to executor
          .map(tserver -> CompletableFuture
              .supplyAsync(() -> activeCompactionsForServer(tserver, instanceOps), executorService))
          // collect all the futures
          .collect(Collectors.collectingAndThen(Collectors.toList(),
              // then join the futures, and stream the results from each tserver in order
              futures -> futures.stream().map(CompletableFuture::join).flatMap(List::stream)));
      return Stream.concat(header, activeCompactionLines);
    } finally {
      executorService.shutdown();
    }

  }

}

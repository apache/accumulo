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
package org.apache.accumulo.shell.commands;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.util.DurationFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ActiveCompactionHelper {

  private static final Logger log = LoggerFactory.getLogger(ActiveCompactionHelper.class);

  private static String maxDecimal(double count) {
    if (count < 9.995) {
      return String.format("%.2f", count);
    }
    if (count < 99.95) {
      return String.format("%.1f", count);
    }
    return String.format("%.0f", count);
  }

  private static String shortenCount(long count) {
    if (count < 1_000) {
      return count + "";
    }
    if (count < 1_000_000) {
      return maxDecimal(count / 1_000.0) + "K";
    }
    if (count < 1_000_000_000) {
      return maxDecimal(count / 1_000_000.0) + "M";
    }
    return maxDecimal(count / 1_000_000_000.0) + "B";
  }

  private static String formatActiveCompactionLine(ActiveCompaction ac) {
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

    String hostSuffix;
    switch (ac.getHost().getType()) {
      case TSERVER:
        hostSuffix = "";
        break;
      case COMPACTOR:
        hostSuffix = " (ext)";
        break;
      default:
        hostSuffix = ac.getHost().getType().name();
        break;
    }

    String host = ac.getHost().getAddress() + ":" + ac.getHost().getPort() + hostSuffix;

    try {
      var dur = new DurationFormat(ac.getAge(), "");
      return String.format(
          "%21s | %9s | %5s | %6s | %5s | %5s | %15s | %-40s | %5s | %35s | %9s | %s", host, dur,
          ac.getType(), ac.getReason(), shortenCount(ac.getEntriesRead()),
          shortenCount(ac.getEntriesWritten()), ac.getTable(), ac.getTablet(),
          ac.getInputFiles().size(), output, iterList, iterOpts);
    } catch (TableNotFoundException e) {
      return "ERROR " + e.getMessage();
    }
  }

  public static Stream<String> appendHeader(Stream<String> stream) {
    Stream<String> header = Stream.of(String.format(
        " %-21s| %-9s | %-5s | %-6s | %-5s | %-5s | %-15s | %-40s | %-5s | %-35s | %-9s | %s",
        "SERVER", "AGE", "TYPE", "REASON", "READ", "WROTE", "TABLE", "TABLET", "INPUT", "OUTPUT",
        "ITERATORS", "ITERATOR OPTIONS"));
    return Stream.concat(header, stream);
  }

  public static Stream<String> activeCompactionsForServer(String tserver,
      InstanceOperations instanceOps) {
    List<String> compactions = new ArrayList<>();
    try {
      List<ActiveCompaction> acl = new ArrayList<>(instanceOps.getActiveCompactions(tserver));
      acl.sort((o1, o2) -> (int) (o2.getAge() - o1.getAge()));
      for (ActiveCompaction ac : acl) {
        compactions.add(formatActiveCompactionLine(ac));
      }
    } catch (Exception e) {
      log.debug("Failed to list active compactions for server {}", tserver, e);
      compactions.add(tserver + " ERROR " + e.getMessage());
    }
    return compactions.stream();
  }

  public static Stream<String> stream(InstanceOperations instanceOps) {
    List<ActiveCompaction> activeCompactions;
    try {
      activeCompactions = instanceOps.getActiveCompactions();
    } catch (AccumuloException | AccumuloSecurityException e) {
      return Stream.of("ERROR " + e.getMessage());
    }
    Comparator<ActiveCompaction> comparator = Comparator.comparing(ac -> ac.getHost().getAddress());
    comparator = comparator.thenComparing(ac -> ac.getHost().getPort())
        .thenComparing((o1, o2) -> (int) (o2.getAge() - o1.getAge()));

    activeCompactions.sort(comparator);

    return activeCompactions.stream().map(ac -> formatActiveCompactionLine(ac));
  }

}

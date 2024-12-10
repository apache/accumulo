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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.util.DurationFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

class ActiveCompactionHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ActiveCompactionHelper.class);
  private static final Comparator<ActiveCompaction> COMPACTION_AGE_DESCENDING =
      Comparator.comparingLong(ActiveCompaction::getAge).reversed();

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
    switch (ac.getServerId().getType()) {
      case TABLET_SERVER:
        hostSuffix = "";
        break;
      case COMPACTOR:
        hostSuffix = " (ext)";
        break;
      default:
        hostSuffix = ac.getServerId().getType().name();
        break;
    }

    String host = ac.getServerId().toHostPortString() + hostSuffix;

    try {
      var dur = new DurationFormat(ac.getAge(), "");
      return String.format(
          "%21s | %21s | %9s | %5s | %6s | %5s | %5s | %15s | %-40s | %5s | %35s | %9s | %s",
          ac.getServerId().getResourceGroup(), host, dur, ac.getType(), ac.getReason(),
          shortenCount(ac.getEntriesRead()), shortenCount(ac.getEntriesWritten()), ac.getTable(),
          ac.getTablet(), ac.getInputFiles().size(), output, iterList, iterOpts);
    } catch (TableNotFoundException e) {
      return "ERROR " + e.getMessage();
    }
  }

  public static Stream<String> appendHeader(Stream<String> stream) {
    Stream<String> header = Stream.of(String.format(
        " %-21s| %-21s| %-9s | %-5s | %-6s | %-5s | %-5s | %-15s | %-40s | %-5s | %-35s | %-9s | %s",
        "GROUP", "SERVER", "AGE", "TYPE", "REASON", "READ", "WROTE", "TABLE", "TABLET", "INPUT",
        "OUTPUT", "ITERATORS", "ITERATOR OPTIONS"));
    return Stream.concat(header, stream);
  }

  public static Stream<String> activeCompactionsForServer(String tserver,
      InstanceOperations instanceOps) {
    final HostAndPort hp = HostAndPort.fromString(tserver);
    ServerId server =
        instanceOps.getServer(ServerId.Type.COMPACTOR, null, hp.getHost(), hp.getPort());
    if (server == null) {
      server = instanceOps.getServer(ServerId.Type.TABLET_SERVER, null, hp.getHost(), hp.getPort());
    }
    if (server == null) {
      return Stream.of();
    } else {
      try {
        return instanceOps.getActiveCompactions(List.of(server)).stream()
            .sorted(COMPACTION_AGE_DESCENDING)
            .map(ActiveCompactionHelper::formatActiveCompactionLine);
      } catch (Exception e) {
        LOG.debug("Failed to list active compactions for server {}", tserver, e);
        return Stream.of(tserver + " ERROR " + e.getMessage());
      }
    }
  }

  public static Stream<String> activeCompactions(InstanceOperations instanceOps,
      Predicate<String> resourceGroupPredicate, BiPredicate<String,Integer> hostPortPredicate) {

    try {
      final Set<ServerId> compactionServers = new HashSet<>();
      compactionServers.addAll(instanceOps.getServers(ServerId.Type.COMPACTOR,
          resourceGroupPredicate, hostPortPredicate));
      compactionServers.addAll(instanceOps.getServers(ServerId.Type.TABLET_SERVER,
          resourceGroupPredicate, hostPortPredicate));

      return sortActiveCompactions(instanceOps.getActiveCompactions(compactionServers));
    } catch (AccumuloException | AccumuloSecurityException e) {
      LOG.debug("Failed to list active compactions with resource group and server predicates", e);
      return Stream.of("ERROR " + e.getMessage());
    }
  }

  public static Stream<String> activeCompactions(InstanceOperations instanceOps) {
    try {
      Set<ServerId> compactionServers = new HashSet<>();
      compactionServers.addAll(instanceOps.getServers(ServerId.Type.COMPACTOR));
      compactionServers.addAll(instanceOps.getServers(ServerId.Type.TABLET_SERVER));
      return sortActiveCompactions(instanceOps.getActiveCompactions(compactionServers));
    } catch (AccumuloException | AccumuloSecurityException e) {
      return Stream.of("ERROR " + e.getMessage());
    }
  }

  private static Stream<String> sortActiveCompactions(List<ActiveCompaction> activeCompactions) {
    Comparator<ActiveCompaction> comparator = Comparator
        .comparing((ActiveCompaction ac) -> ac.getServerId().getHost())
        .thenComparing(ac -> ac.getServerId().getPort()).thenComparing(COMPACTION_AGE_DESCENDING);
    return activeCompactions.stream().sorted(comparator)
        .map(ActiveCompactionHelper::formatActiveCompactionLine);
  }

}

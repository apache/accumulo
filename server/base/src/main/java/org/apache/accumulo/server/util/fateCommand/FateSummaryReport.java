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
package org.apache.accumulo.server.util.fateCommand;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.ReadOnlyTStore;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class FateSummaryReport {

  private final Map<String,Integer> statusCounts = new TreeMap<>();
  private final Map<String,Integer> cmdCounts = new TreeMap<>();
  private final Map<String,Integer> stepCounts = new TreeMap<>();
  private final Set<FateTxnDetails> fateDetails = new TreeSet<>();
  // epoch millis to avoid needing gson type adapter.
  private final long reportTime = Instant.now().toEpochMilli();

  private final Set<String> statusFilterNames = new TreeSet<>();

  private final static Gson gson = new GsonBuilder().setPrettyPrinting().create();

  // exclude from json output
  private final transient Map<String,String> idsToNameMap;

  public FateSummaryReport(Map<String,String> idsToNameMap,
      EnumSet<ReadOnlyTStore.TStatus> statusFilter) {
    this.idsToNameMap = idsToNameMap;
    if (statusFilter != null) {
      statusFilter.forEach(f -> this.statusFilterNames.add(f.name()));
    }
  }

  public void gatherTxnStatus(AdminUtil.TransactionStatus txnStatus) {
    var status = txnStatus.getStatus();
    if (status == null) {
      statusCounts.merge("?", 1, Integer::sum);
    } else {
      String name = txnStatus.getStatus().name();
      statusCounts.merge(name, 1, Integer::sum);
    }
    String top = txnStatus.getTop();
    stepCounts.merge(Objects.requireNonNullElse(top, "?"), 1, Integer::sum);
    String runningRepo = txnStatus.getTxName();
    cmdCounts.merge(Objects.requireNonNullElse(runningRepo, "?"), 1, Integer::sum);

    // filter status if provided.
    if (!statusFilterNames.isEmpty() && !statusFilterNames.contains(txnStatus.getStatus().name())) {
      return;
    }
    fateDetails.add(new FateTxnDetails(reportTime, txnStatus, idsToNameMap));
  }

  public Map<String,Integer> getStatusCounts() {
    return statusCounts;
  }

  public Map<String,Integer> getCmdCounts() {
    return cmdCounts;
  }

  public Map<String,Integer> getStepCounts() {
    return stepCounts;
  }

  public Set<FateTxnDetails> getFateDetails() {
    return fateDetails;
  }

  public long getReportTime() {
    return reportTime;
  }

  public Set<String> getStatusFilterNames() {
    return statusFilterNames;
  }

  public String toJson() {
    return gson.toJson(this);
  }

  public static FateSummaryReport fromJson(final String jsonString) {
    return gson.fromJson(jsonString, FateSummaryReport.class);
  }

  /**
   * Generate a summary report in a format suitable for pagination in fate commands that expects a
   * list of lines.
   *
   * @return formatted report lines.
   */
  public List<String> formatLines() {
    List<String> lines = new ArrayList<>();

    final DateTimeFormatter fmt =
        DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.from(ZoneOffset.UTC));

    // output report
    lines.add(String.format("Report Time: %s",
        fmt.format(Instant.ofEpochMilli(reportTime).truncatedTo(ChronoUnit.SECONDS))));

    lines.add("Status counts:");
    statusCounts.forEach((status, count) -> lines.add(String.format("  %s: %d", status, count)));

    lines.add("Command counts:");
    cmdCounts.forEach((cmd, count) -> lines.add(String.format("  %s: %d", cmd, count)));

    lines.add("Step counts:");
    stepCounts.forEach((step, count) -> lines.add(String.format("  %s: %d", step, count)));

    lines.add("\nFate transactions (oldest first):");
    lines.add("Status Filters: "
        + (statusFilterNames.isEmpty() ? "[NONE]" : statusFilterNames.toString()));

    lines.add(FateTxnDetails.TXN_HEADER);
    fateDetails.forEach(txnDetails -> lines.add(txnDetails.toString()));

    return lines;
  }
}

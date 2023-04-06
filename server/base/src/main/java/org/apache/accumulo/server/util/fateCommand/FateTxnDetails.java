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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.accumulo.core.fate.AdminUtil;

public class FateTxnDetails implements Comparable<FateTxnDetails> {
  final static String TXN_HEADER =
      "Running\ttxn_id\t\t\t\tStatus\t\tCommand\t\tStep (top)\t\tlocks held:(table id, name)\tlocks waiting:(table id, name)";

  private long running;
  private String status = "?";
  private String txName = "?";
  private String step = "?";
  private String txnId = "?";
  private List<String> locksHeld = List.of();
  private List<String> locksWaiting = List.of();

  /**
   * Create a detailed FaTE transaction that can be formatted for status reports.
   * <p>
   * Implementation note: Instance of this class are expected to be used for status reporting that
   * represent a snapshot at the time of measurement. This class is conservative in handling
   * possible vales - gathering FaTE information is done asynchronously and when the measurement is
   * captured it may not be complete.
   *
   * @param reportTime the Instant that the report snapshot was created
   * @param txnStatus the FaTE transaction status
   * @param idsToNameMap a map of namespace, table ids to names.
   */
  public FateTxnDetails(final long reportTime, final AdminUtil.TransactionStatus txnStatus,
      final Map<String,String> idsToNameMap) {

    // guard against invalid transaction
    if (txnStatus == null) {
      return;
    }

    // guard against partial / invalid info
    if (txnStatus.getTimeCreated() != 0) {
      running = reportTime - txnStatus.getTimeCreated();
    }
    if (txnStatus.getStatus() != null) {
      status = txnStatus.getStatus().name();
    }
    if (txnStatus.getTop() != null) {
      step = txnStatus.getTop();
    }
    if (txnStatus.getTxName() != null) {
      txName = txnStatus.getTxName();
    }
    if (txnStatus.getTxid() != null) {
      txnId = txnStatus.getTxid();
    }
    locksHeld = formatLockInfo(txnStatus.getHeldLocks(), idsToNameMap);
    locksWaiting = formatLockInfo(txnStatus.getWaitingLocks(), idsToNameMap);
  }

  private List<String> formatLockInfo(final List<String> lockInfo,
      final Map<String,String> idsToNameMap) {
    List<String> formattedLocks = new ArrayList<>();
    for (String lock : lockInfo) {
      String[] parts = lock.split(":");
      if (parts.length == 2) {
        String lockType = parts[0];
        String id = parts[1];
        formattedLocks.add(String.format("%s:(%s,%s)", lockType, id, idsToNameMap.get(id)));
      }
    }
    return formattedLocks;
  }

  public String getTxnId() {
    return txnId;
  }

  /**
   * Sort by running time in reverse (oldest txn first). txid is unique as used to break times and
   * so that compareTo remains consistent with hashCode and equals methods.
   *
   * @param other the FateTxnDetails to be compared.
   * @return -1, 0 or 1 if older, equal or newer than the other
   */
  @Override
  public int compareTo(FateTxnDetails other) {
    int v = Long.compare(other.running, this.running);
    if (v != 0) {
      return v;
    }
    return txnId.compareTo(other.txnId);
  }

  @Override
  public boolean equals(Object o) {

    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FateTxnDetails that = (FateTxnDetails) o;
    return running == that.running && txnId.equals(that.txnId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(running, txnId);
  }

  @Override
  public String toString() {
    Duration elapsed = Duration.ofMillis(running);
    String hms = String.format("%d:%02d:%02d", elapsed.toHours(), elapsed.toMinutesPart(),
        elapsed.toSecondsPart());

    return hms + "\t" + txnId + "\t" + status + "\t" + txName + "\t" + step + "\theld:"
        + locksHeld.toString() + "\twaiting:" + locksWaiting.toString();
  }

}

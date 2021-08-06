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
package org.apache.accumulo.core.client.admin;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * FATE transaction status, including lock information.
 */
public class TransactionStatus {

  private final long txid;
  private final String status;
  private final String debug;
  private final List<String> hlocks;
  private final List<String> wlocks;
  private final String top;
  private final long timeCreated;
  private final String stackInfo;

  public TransactionStatus(Long tid, String status, String debug, List<String> hlocks,
      List<String> wlocks, String top, Long timeCreated, String stackInfo) {

    this.txid = tid;
    this.status = status;
    this.debug = debug;
    this.hlocks = Collections.unmodifiableList(hlocks);
    this.wlocks = Collections.unmodifiableList(wlocks);
    this.top = top;
    this.timeCreated = timeCreated;
    this.stackInfo = stackInfo;

  }

  /**
   * @return This fate operations transaction id, formatted in the same way as FATE transactions are
   *         in the Accumulo logs.
   */
  public String getTxid() {
    return String.format("%016x", txid);
  }

  /**
   * @return This fate operations transaction id, in its original long form.
   */
  public long getTxidLong() {
    return txid;
  }

  // Changed this to string for public API, shouldn't cause issues elsewhere but need to
  // double-check
  public String getStatus() {
    return status;
  }

  /**
   * @return The debug info for the operation on the top of the stack for this Fate operation.
   */
  public String getDebug() {
    return debug;
  }

  /**
   * @return list of namespace and table ids locked
   */
  public List<String> getHeldLocks() {
    return hlocks;
  }

  /**
   * @return list of namespace and table ids locked
   */
  public List<String> getWaitingLocks() {
    return wlocks;
  }

  /**
   * @return The operation on the top of the stack for this Fate operation.
   */
  public String getTop() {
    return top;
  }

  /**
   * @return The timestamp of when the operation was created in ISO format wiht UTC timezone.
   */
  public String getTimeCreatedFormatted() {
    return timeCreated > 0 ? new Date(timeCreated).toInstant().atZone(ZoneOffset.UTC)
        .format(DateTimeFormatter.ISO_DATE_TIME) : "ERROR";
  }

  /**
   * @return The unformatted form of the timestamp.
   */
  public long getTimeCreated() {
    return timeCreated;
  }

  /**
   * @return The stackInfo for a transaction
   */
  public String getStackInfo() {
    return stackInfo;
  }
}

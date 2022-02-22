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
package org.apache.accumulo.shell.commands.fate;

import java.io.Serializable;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.fate.FateTransactionStatus;

/**
 * FATE transaction status, including lock information.
 */
public class FateCommandStatus {

  private final long txid;
  private final FateTransactionStatus status;
  private final Serializable debug;
  private final List<String> hlocks;
  private final List<String> wlocks;
  private final Object top;
  private final long timeCreated;

  FateCommandStatus(Long tid, FateTransactionStatus status, Serializable debug, List<String> hlocks,
      List<String> wlocks, Object top, Long timeCreated) {

    this.txid = tid;
    this.status = status;
    this.debug = debug;
    this.hlocks = Collections.unmodifiableList(hlocks);
    this.wlocks = Collections.unmodifiableList(wlocks);
    this.top = top;
    this.timeCreated = timeCreated;

  }

  /**
   * @return This fate operations transaction id, formatted in the same way as FATE transactions are
   *         in the Accumulo logs.
   */
  public String getTxid() {
    return String.format("%016x", txid);
  }

  public FateTransactionStatus getStatus() {
    return status;
  }

  /**
   * @return The debug info for the operation on the top of the stack for this Fate operation.
   */
  public Serializable getDebug() {
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
  public Object getTop() {
    return top;
  }

  /**
   * @return The timestamp of when the operation was created in ISO format with UTC timezone.
   */
  public String getTimeCreatedFormatted() {
    return timeCreated > 0 ? new Date(timeCreated).toInstant().atZone(ZoneOffset.UTC)
        .format(DateTimeFormatter.ISO_DATE_TIME) : "ERROR";
  }
}

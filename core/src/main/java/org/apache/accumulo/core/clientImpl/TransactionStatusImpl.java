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
package org.apache.accumulo.core.clientImpl;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.core.client.admin.TransactionStatus;

public class TransactionStatusImpl implements TransactionStatus {

  private final long txid;
  private final String status;
  private final String debug;
  private final List<String> hlocks;
  private final List<String> wlocks;
  private final String top;
  private final long timeCreated;
  private final String stackInfo;

  public TransactionStatusImpl(Long tid, String status, String debug, List<String> hlocks,
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

  @Override
  public String getTxid() {
    return String.format("%016x", txid);
  }

  @Override
  public long getTxidLong() {
    return txid;
  }

  @Override
  public String getStatus() {
    return status;
  }

  @Override
  public String getDebug() {
    return debug;
  }

  @Override
  public List<String> getHeldLocks() {
    return hlocks;
  }

  @Override
  public List<String> getWaitingLocks() {
    return wlocks;
  }

  @Override
  public String getTop() {
    return top;
  }

  @Override
  public String getTimeCreatedFormatted() {
    return timeCreated > 0 ? new Date(timeCreated).toInstant().atZone(ZoneOffset.UTC)
        .format(DateTimeFormatter.ISO_DATE_TIME) : "ERROR";
  }

  @Override
  public long getTimeCreated() {
    return timeCreated;
  }

  @Override
  public String getStackInfo() {
    return stackInfo;
  }
}

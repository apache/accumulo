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
package org.apache.accumulo.tserver.session;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.tserver.scan.ScanParameters;
import org.apache.accumulo.tserver.scan.ScanTask;
import org.apache.accumulo.tserver.tablet.ScanBatch;
import org.apache.accumulo.tserver.tablet.Scanner;

public class SingleScanSession extends ScanSession {
  public final KeyExtent extent;
  public final AtomicBoolean interruptFlag = new AtomicBoolean();
  public long entriesReturned = 0;
  public long batchCount = 0;
  public volatile ScanTask<ScanBatch> nextBatchTask;
  public Scanner scanner;
  public final long readaheadThreshold;

  public SingleScanSession(TCredentials credentials, KeyExtent extent, ScanParameters scanParams,
      long readaheadThreshold, Map<String,String> executionHints, TabletResolver tabletResolver) {
    super(credentials, scanParams, executionHints, tabletResolver);
    this.extent = extent;
    this.readaheadThreshold = readaheadThreshold;
  }

  @Override
  public Type getScanType() {
    return Type.SINGLE;
  }

  @Override
  public TableId getTableId() {
    return extent.tableId();
  }

  @Override
  public boolean cleanup() {
    final boolean ret;
    try {
      if (nextBatchTask != null) {
        nextBatchTask.cancel(true);
      }
    } finally {
      if (scanner != null) {
        ret = scanner.close();
      } else {
        ret = true;
      }
    }
    return ret;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.tserver.session;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.Stat;
import org.apache.accumulo.tserver.scan.ScanTask;
import org.apache.accumulo.tserver.tablet.ScanBatch;
import org.apache.accumulo.tserver.tablet.Scanner;

public class ScanSession extends Session {
  public final Stat nbTimes = new Stat();
  public final KeyExtent extent;
  public final Set<Column> columnSet;
  public final List<IterInfo> ssiList;
  public final Map<String,Map<String,String>> ssio;
  public final Authorizations auths;
  public final AtomicBoolean interruptFlag = new AtomicBoolean();
  public long entriesReturned = 0;
  public long batchCount = 0;
  public volatile ScanTask<ScanBatch> nextBatchTask;
  public Scanner scanner;
  public final long readaheadThreshold;
  public final long batchTimeOut;
  public final String context;

  public ScanSession(TCredentials credentials, KeyExtent extent, Set<Column> columnSet, List<IterInfo> ssiList, Map<String,Map<String,String>> ssio,
      Authorizations authorizations, long readaheadThreshold, long batchTimeOut, String context) {
    super(credentials);
    this.extent = extent;
    this.columnSet = columnSet;
    this.ssiList = ssiList;
    this.ssio = ssio;
    this.auths = authorizations;
    this.readaheadThreshold = readaheadThreshold;
    this.batchTimeOut = batchTimeOut;
    this.context = context;
  }

  @Override
  public boolean cleanup() {
    final boolean ret;
    try {
      if (nextBatchTask != null)
        nextBatchTask.cancel(true);
    } finally {
      if (scanner != null)
        ret = scanner.close();
      else
        ret = true;
    }
    return ret;
  }

}

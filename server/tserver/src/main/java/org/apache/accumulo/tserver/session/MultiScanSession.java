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

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.tserver.scan.ScanTask;

public class MultiScanSession extends ScanSession {
  public final KeyExtent threadPoolExtent;
  public final Map<KeyExtent,List<Range>> queries;
  public final SamplerConfiguration samplerConfig;
  public final long batchTimeOut;
  public final String context;

  // stats
  public int numRanges;
  public int numTablets;
  public int numEntries;
  public long totalLookupTime;

  public volatile ScanTask<MultiScanResult> lookupTask;

  public MultiScanSession(TCredentials credentials, KeyExtent threadPoolExtent,
      Map<KeyExtent,List<Range>> queries, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, Authorizations authorizations,
      SamplerConfiguration samplerConfig, long batchTimeOut, String context,
      Map<String,String> executionHints) {
    super(credentials, new HashSet<>(), ssiList, ssio, authorizations, executionHints);
    this.queries = queries;
    this.threadPoolExtent = threadPoolExtent;
    this.samplerConfig = samplerConfig;
    this.batchTimeOut = batchTimeOut;
    this.context = context;
  }

  @Override
  public Type getScanType() {
    return Type.MULTI;
  }

  @Override
  public TableId getTableId() {
    return threadPoolExtent.getTableId();
  }

  @Override
  public boolean cleanup() {
    if (lookupTask != null)
      lookupTask.cancel(true);
    // the cancellation should provide us the safety to return true here
    return true;
  }
}

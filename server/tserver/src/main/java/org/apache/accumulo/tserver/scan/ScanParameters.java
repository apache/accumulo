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
package org.apache.accumulo.tserver.scan;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.scan.ScanDispatch;

/**
 * Information needed to execute a scan inside a tablet
 */
public final class ScanParameters {

  private final Authorizations authorizations;
  private final Set<Column> columnSet;
  private final List<IterInfo> ssiList;
  private final Map<String,Map<String,String>> ssio;
  private final int maxEntries;
  private final boolean isolated;
  private final SamplerConfiguration samplerConfig;
  private final long batchTimeOut;
  private final String classLoaderContext;
  private volatile ScanDispatch dispatch;

  public ScanParameters(int maxEntries, Authorizations authorizations, Set<Column> columnSet,
      List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, boolean isolated,
      SamplerConfiguration samplerConfig, long batchTimeOut, String classLoaderContext) {
    this.maxEntries = maxEntries;
    this.authorizations = authorizations;
    this.columnSet = columnSet;
    this.ssiList = ssiList;
    this.ssio = ssio;
    this.isolated = isolated;
    this.samplerConfig = samplerConfig;
    this.batchTimeOut = batchTimeOut;
    this.classLoaderContext = classLoaderContext;
  }

  public Authorizations getAuthorizations() {
    return authorizations;
  }

  public Set<Column> getColumnSet() {
    return columnSet;
  }

  public List<IterInfo> getSsiList() {
    return ssiList;
  }

  public Map<String,Map<String,String>> getSsio() {
    return ssio;
  }

  public int getMaxEntries() {
    return maxEntries;
  }

  public boolean isIsolated() {
    return isolated;
  }

  public SamplerConfigurationImpl getSamplerConfigurationImpl() {
    if (samplerConfig == null) {
      return null;
    }
    return new SamplerConfigurationImpl(samplerConfig);
  }

  public long getBatchTimeOut() {
    return batchTimeOut;
  }

  public String getClassLoaderContext() {
    return classLoaderContext;
  }

  public void setScanDispatch(ScanDispatch dispatch) {
    this.dispatch = dispatch;
  }

  public ScanDispatch getScanDispatch() {
    return dispatch;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("[");
    buf.append("auths=").append(this.authorizations);
    buf.append(", batchTimeOut=").append(this.batchTimeOut);
    buf.append(", context=").append(this.classLoaderContext);
    buf.append(", columns=").append(this.columnSet);
    buf.append(", isolated=").append(this.isolated);
    buf.append(", maxEntries=").append(this.maxEntries);
    buf.append(", num=").append(this.maxEntries);
    buf.append(", samplerConfig=").append(this.samplerConfig);
    buf.append("]");
    return buf.toString();
  }
}

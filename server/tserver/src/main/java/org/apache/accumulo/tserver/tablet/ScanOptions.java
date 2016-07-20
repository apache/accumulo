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
package org.apache.accumulo.tserver.tablet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;

final class ScanOptions {

  private final Authorizations authorizations;
  private final byte[] defaultLabels;
  private final Set<Column> columnSet;
  private final List<IterInfo> ssiList;
  private final Map<String,Map<String,String>> ssio;
  private final AtomicBoolean interruptFlag;
  private final int num;
  private final boolean isolated;
  private SamplerConfiguration samplerConfig;
  private final long batchTimeOut;
  private String classLoaderContext;

  ScanOptions(int num, Authorizations authorizations, byte[] defaultLabels, Set<Column> columnSet, List<IterInfo> ssiList, Map<String,Map<String,String>> ssio,
      AtomicBoolean interruptFlag, boolean isolated, SamplerConfiguration samplerConfig, long batchTimeOut, String classLoaderContext) {
    this.num = num;
    this.authorizations = authorizations;
    this.defaultLabels = defaultLabels;
    this.columnSet = columnSet;
    this.ssiList = ssiList;
    this.ssio = ssio;
    this.interruptFlag = interruptFlag;
    this.isolated = isolated;
    this.samplerConfig = samplerConfig;
    this.batchTimeOut = batchTimeOut;
    this.classLoaderContext = classLoaderContext;
  }

  public Authorizations getAuthorizations() {
    return authorizations;
  }

  public byte[] getDefaultLabels() {
    return defaultLabels;
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

  public AtomicBoolean getInterruptFlag() {
    return interruptFlag;
  }

  public int getNum() {
    return num;
  }

  public boolean isIsolated() {
    return isolated;
  }

  public SamplerConfiguration getSamplerConfiguration() {
    return samplerConfig;
  }

  public SamplerConfigurationImpl getSamplerConfigurationImpl() {
    if (samplerConfig == null)
      return null;
    return new SamplerConfigurationImpl(samplerConfig);
  }

  public long getBatchTimeOut() {
    return batchTimeOut;
  }

  public String getClassLoaderContext() {
    return classLoaderContext;
  }

  public void setClassLoaderContext(String context) {
    this.classLoaderContext = context;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("[");
    buf.append("auths=").append(this.authorizations);
    buf.append(", batchTimeOut=").append(this.batchTimeOut);
    buf.append(", context=").append(this.classLoaderContext);
    buf.append(", columns=").append(this.columnSet);
    buf.append(", interruptFlag=").append(this.interruptFlag);
    buf.append(", isolated=").append(this.isolated);
    buf.append(", num=").append(this.num);
    buf.append(", samplerConfig=").append(this.samplerConfig);
    buf.append("]");
    return buf.toString();
  }
}

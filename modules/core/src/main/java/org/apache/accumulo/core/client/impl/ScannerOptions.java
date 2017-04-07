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
package org.apache.accumulo.core.client.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

public class ScannerOptions implements ScannerBase {

  protected List<IterInfo> serverSideIteratorList = Collections.emptyList();
  protected Map<String,Map<String,String>> serverSideIteratorOptions = Collections.emptyMap();

  protected SortedSet<Column> fetchedColumns = new TreeSet<>();

  protected long timeOut = Long.MAX_VALUE;

  protected long batchTimeOut = Long.MAX_VALUE;

  private String regexIterName = null;

  private SamplerConfiguration samplerConfig = null;

  protected String classLoaderContext = null;

  protected ScannerOptions() {}

  public ScannerOptions(ScannerOptions so) {
    setOptions(this, so);
  }

  @Override
  public synchronized void addScanIterator(IteratorSetting si) {
    checkArgument(si != null, "si is null");
    if (serverSideIteratorList.size() == 0)
      serverSideIteratorList = new ArrayList<>();

    for (IterInfo ii : serverSideIteratorList) {
      if (ii.iterName.equals(si.getName()))
        throw new IllegalArgumentException("Iterator name is already in use " + si.getName());
      if (ii.getPriority() == si.getPriority())
        throw new IllegalArgumentException("Iterator priority is already in use " + si.getPriority());
    }

    serverSideIteratorList.add(new IterInfo(si.getPriority(), si.getIteratorClass(), si.getName()));

    if (serverSideIteratorOptions.size() == 0)
      serverSideIteratorOptions = new HashMap<>();

    Map<String,String> opts = serverSideIteratorOptions.get(si.getName());

    if (opts == null) {
      opts = new HashMap<>();
      serverSideIteratorOptions.put(si.getName(), opts);
    }
    opts.putAll(si.getOptions());
  }

  @Override
  public synchronized void removeScanIterator(String iteratorName) {
    checkArgument(iteratorName != null, "iteratorName is null");
    // if no iterators are set, we don't have it, so it is already removed
    if (serverSideIteratorList.size() == 0)
      return;

    for (IterInfo ii : serverSideIteratorList) {
      if (ii.iterName.equals(iteratorName)) {
        serverSideIteratorList.remove(ii);
        break;
      }
    }

    serverSideIteratorOptions.remove(iteratorName);
  }

  @Override
  public synchronized void updateScanIteratorOption(String iteratorName, String key, String value) {
    checkArgument(iteratorName != null, "iteratorName is null");
    checkArgument(key != null, "key is null");
    checkArgument(value != null, "value is null");
    if (serverSideIteratorOptions.size() == 0)
      serverSideIteratorOptions = new HashMap<>();

    Map<String,String> opts = serverSideIteratorOptions.get(iteratorName);

    if (opts == null) {
      opts = new HashMap<>();
      serverSideIteratorOptions.put(iteratorName, opts);
    }
    opts.put(key, value);
  }

  @Override
  public synchronized void fetchColumnFamily(Text col) {
    checkArgument(col != null, "col is null");
    Column c = new Column(TextUtil.getBytes(col), null, null);
    fetchedColumns.add(c);
  }

  @Override
  public synchronized void fetchColumn(Text colFam, Text colQual) {
    checkArgument(colFam != null, "colFam is null");
    checkArgument(colQual != null, "colQual is null");
    Column c = new Column(TextUtil.getBytes(colFam), TextUtil.getBytes(colQual), null);
    fetchedColumns.add(c);
  }

  @Override
  public void fetchColumn(IteratorSetting.Column column) {
    checkArgument(column != null, "Column is null");
    fetchColumn(column.getColumnFamily(), column.getColumnQualifier());
  }

  @Override
  public synchronized void clearColumns() {
    fetchedColumns.clear();
  }

  public synchronized SortedSet<Column> getFetchedColumns() {
    return fetchedColumns;
  }

  @Override
  public synchronized void clearScanIterators() {
    serverSideIteratorList = Collections.emptyList();
    serverSideIteratorOptions = Collections.emptyMap();
    regexIterName = null;
  }

  protected static void setOptions(ScannerOptions dst, ScannerOptions src) {
    synchronized (dst) {
      synchronized (src) {
        dst.regexIterName = src.regexIterName;
        dst.fetchedColumns = new TreeSet<>(src.fetchedColumns);
        dst.serverSideIteratorList = new ArrayList<>(src.serverSideIteratorList);
        dst.classLoaderContext = src.classLoaderContext;

        dst.serverSideIteratorOptions = new HashMap<>();
        Set<Entry<String,Map<String,String>>> es = src.serverSideIteratorOptions.entrySet();
        for (Entry<String,Map<String,String>> entry : es)
          dst.serverSideIteratorOptions.put(entry.getKey(), new HashMap<>(entry.getValue()));

        dst.samplerConfig = src.samplerConfig;
        dst.batchTimeOut = src.batchTimeOut;
      }
    }
  }

  @Override
  public Iterator<Entry<Key,Value>> iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void setTimeout(long timeout, TimeUnit timeUnit) {
    if (timeOut < 0) {
      throw new IllegalArgumentException("TimeOut must be positive : " + timeOut);
    }

    if (timeout == 0)
      this.timeOut = Long.MAX_VALUE;
    else
      this.timeOut = timeUnit.toMillis(timeout);
  }

  @Override
  public synchronized long getTimeout(TimeUnit timeunit) {
    return timeunit.convert(timeOut, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    // Nothing needs to be closed
  }

  @Override
  public synchronized Authorizations getAuthorizations() {
    throw new UnsupportedOperationException("No authorizations to return");
  }

  @Override
  public synchronized void setSamplerConfiguration(SamplerConfiguration samplerConfig) {
    requireNonNull(samplerConfig);
    this.samplerConfig = samplerConfig;
  }

  @Override
  public synchronized SamplerConfiguration getSamplerConfiguration() {
    return samplerConfig;
  }

  @Override
  public synchronized void clearSamplerConfiguration() {
    this.samplerConfig = null;
  }

  @Override
  public void setBatchTimeout(long timeout, TimeUnit timeUnit) {
    if (timeOut < 0) {
      throw new IllegalArgumentException("Batch timeout must be positive : " + timeOut);
    }
    if (timeout == 0) {
      this.batchTimeOut = Long.MAX_VALUE;
    } else {
      this.batchTimeOut = timeUnit.toMillis(timeout);
    }
  }

  @Override
  public long getBatchTimeout(TimeUnit timeUnit) {
    return timeUnit.convert(batchTimeOut, TimeUnit.MILLISECONDS);
  }

  @Override
  public void setClassLoaderContext(String classLoaderContext) {
    requireNonNull(classLoaderContext, "classloader context name cannot be null");
    this.classLoaderContext = classLoaderContext;
  }

  @Override
  public void clearClassLoaderContext() {
    this.classLoaderContext = null;
  }

  @Override
  public String getClassLoaderContext() {
    return this.classLoaderContext;
  }

}

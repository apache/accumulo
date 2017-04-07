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
package org.apache.accumulo.core.client;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorAdapter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

/**
 * A scanner that instantiates iterators on the client side instead of on the tablet server. This can be useful for testing iterators or in cases where you
 * don't want iterators affecting the performance of tablet servers.<br>
 * <br>
 * Suggested usage:<br>
 * <code>Scanner scanner = new ClientSideIteratorScanner(connector.createScanner(tableName, authorizations));</code><br>
 * <br>
 * Iterators added to this scanner will be run in the client JVM. Separate scan iterators can be run on the server side and client side by adding iterators to
 * the source scanner (which will execute server side) and to the client side scanner (which will execute client side).
 */
public class ClientSideIteratorScanner extends ScannerOptions implements Scanner {
  private int size;

  private Range range;
  private boolean isolated = false;
  private long readaheadThreshold = Constants.SCANNER_DEFAULT_READAHEAD_THRESHOLD;
  private SamplerConfiguration iteratorSamplerConfig;

  private class ClientSideIteratorEnvironment implements IteratorEnvironment {

    private SamplerConfiguration samplerConfig;
    private boolean sampleEnabled;

    ClientSideIteratorEnvironment(boolean sampleEnabled, SamplerConfiguration samplerConfig) {
      this.sampleEnabled = sampleEnabled;
      this.samplerConfig = samplerConfig;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public AccumuloConfiguration getConfig() {
      throw new UnsupportedOperationException();
    }

    @Override
    public IteratorScope getIteratorScope() {
      return IteratorScope.scan;
    }

    @Override
    public boolean isFullMajorCompaction() {
      return false;
    }

    @Override
    public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Authorizations getAuthorizations() {
      return ClientSideIteratorScanner.this.getAuthorizations();
    }

    @Override
    public IteratorEnvironment cloneWithSamplingEnabled() {
      return new ClientSideIteratorEnvironment(true, samplerConfig);
    }

    @Override
    public boolean isSamplingEnabled() {
      return sampleEnabled;
    }

    @Override
    public SamplerConfiguration getSamplerConfiguration() {
      return samplerConfig;
    }
  }

  /**
   * A class that wraps a Scanner in a SortedKeyValueIterator so that other accumulo iterators can use it as a source.
   */
  private class ScannerTranslatorImpl implements SortedKeyValueIterator<Key,Value> {
    protected Scanner scanner;
    Iterator<Entry<Key,Value>> iter;
    Entry<Key,Value> top = null;
    private SamplerConfiguration samplerConfig;

    /**
     * Constructs an accumulo iterator from a scanner.
     *
     * @param scanner
     *          the scanner to iterate over
     */
    public ScannerTranslatorImpl(final Scanner scanner, SamplerConfiguration samplerConfig) {
      this.scanner = scanner;
      this.samplerConfig = samplerConfig;
    }

    @Override
    public void init(final SortedKeyValueIterator<Key,Value> source, final Map<String,String> options, final IteratorEnvironment env) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasTop() {
      return top != null;
    }

    @Override
    public void next() throws IOException {
      if (iter.hasNext())
        top = iter.next();
      else
        top = null;
    }

    @Override
    public void seek(final Range range, final Collection<ByteSequence> columnFamilies, final boolean inclusive) throws IOException {
      if (!inclusive && columnFamilies.size() > 0) {
        throw new IllegalArgumentException();
      }
      scanner.setRange(range);
      scanner.clearColumns();
      for (ByteSequence colf : columnFamilies) {
        scanner.fetchColumnFamily(new Text(colf.toArray()));
      }

      if (samplerConfig == null) {
        scanner.clearSamplerConfiguration();
      } else {
        scanner.setSamplerConfiguration(samplerConfig);
      }

      iter = scanner.iterator();
      next();
    }

    @Override
    public Key getTopKey() {
      return top.getKey();
    }

    @Override
    public Value getTopValue() {
      return top.getValue();
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(final IteratorEnvironment env) {
      return new ScannerTranslatorImpl(scanner, env.isSamplingEnabled() ? env.getSamplerConfiguration() : null);
    }
  }

  private ScannerTranslatorImpl smi;

  /**
   * Constructs a scanner that can execute client-side iterators.
   *
   * @param scanner
   *          the source scanner
   */
  public ClientSideIteratorScanner(final Scanner scanner) {
    smi = new ScannerTranslatorImpl(scanner, scanner.getSamplerConfiguration());
    this.range = scanner.getRange();
    this.size = scanner.getBatchSize();
    this.timeOut = scanner.getTimeout(TimeUnit.MILLISECONDS);
    this.batchTimeOut = scanner.getTimeout(TimeUnit.MILLISECONDS);
    this.readaheadThreshold = scanner.getReadaheadThreshold();
    SamplerConfiguration samplerConfig = scanner.getSamplerConfiguration();
    if (samplerConfig != null)
      setSamplerConfiguration(samplerConfig);
  }

  /**
   * Sets the source Scanner.
   */
  public void setSource(final Scanner scanner) {
    smi = new ScannerTranslatorImpl(scanner, scanner.getSamplerConfiguration());
  }

  @Override
  public Iterator<Entry<Key,Value>> iterator() {
    smi.scanner.setBatchSize(size);
    smi.scanner.setTimeout(timeOut, TimeUnit.MILLISECONDS);
    smi.scanner.setBatchTimeout(batchTimeOut, TimeUnit.MILLISECONDS);
    smi.scanner.setReadaheadThreshold(readaheadThreshold);
    if (isolated)
      smi.scanner.enableIsolation();
    else
      smi.scanner.disableIsolation();

    smi.samplerConfig = getSamplerConfiguration();

    final TreeMap<Integer,IterInfo> tm = new TreeMap<>();

    for (IterInfo iterInfo : serverSideIteratorList) {
      tm.put(iterInfo.getPriority(), iterInfo);
    }

    SortedKeyValueIterator<Key,Value> skvi;
    try {
      skvi = IteratorUtil.loadIterators(smi, tm.values(), serverSideIteratorOptions, new ClientSideIteratorEnvironment(getSamplerConfiguration() != null,
          getIteratorSamplerConfigurationInternal()), false, null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final Set<ByteSequence> colfs = new TreeSet<>();
    for (Column c : this.getFetchedColumns()) {
      colfs.add(new ArrayByteSequence(c.getColumnFamily()));
    }

    try {
      skvi.seek(range, colfs, true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new IteratorAdapter(skvi);
  }

  @Override
  public Authorizations getAuthorizations() {
    return smi.scanner.getAuthorizations();
  }

  @Deprecated
  @Override
  public void setTimeOut(int timeOut) {
    if (timeOut == Integer.MAX_VALUE)
      setTimeout(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    else
      setTimeout(timeOut, TimeUnit.SECONDS);
  }

  @Deprecated
  @Override
  public int getTimeOut() {
    long timeout = getTimeout(TimeUnit.SECONDS);
    if (timeout >= Integer.MAX_VALUE)
      return Integer.MAX_VALUE;
    return (int) timeout;
  }

  @Override
  public void setRange(final Range range) {
    this.range = range;
  }

  @Override
  public Range getRange() {
    return range;
  }

  @Override
  public void setBatchSize(final int size) {
    this.size = size;
  }

  @Override
  public int getBatchSize() {
    return size;
  }

  @Override
  public void enableIsolation() {
    this.isolated = true;
  }

  @Override
  public void disableIsolation() {
    this.isolated = false;
  }

  @Override
  public long getReadaheadThreshold() {
    return readaheadThreshold;
  }

  @Override
  public void setReadaheadThreshold(long batches) {
    if (0 > batches) {
      throw new IllegalArgumentException("Number of batches before read-ahead must be non-negative");
    }
    this.readaheadThreshold = batches;
  }

  private SamplerConfiguration getIteratorSamplerConfigurationInternal() {
    SamplerConfiguration scannerSamplerConfig = getSamplerConfiguration();
    if (scannerSamplerConfig != null) {
      if (iteratorSamplerConfig != null && !iteratorSamplerConfig.equals(scannerSamplerConfig)) {
        throw new IllegalStateException("Scanner and iterator sampler configuration differ");
      }

      return scannerSamplerConfig;
    }

    return iteratorSamplerConfig;
  }

  /**
   * This is provided for the case where no sampler configuration is set on the scanner, but there is a need to create iterator deep copies that have sampling
   * enabled. If sampler configuration is set on the scanner, then this method does not need to be called inorder to create deep copies with sampling.
   *
   * <p>
   * Setting this differently than the scanners sampler configuration may cause exceptions.
   *
   * @since 1.8.0
   */
  public void setIteratorSamplerConfiguration(SamplerConfiguration sc) {
    requireNonNull(sc);
    this.iteratorSamplerConfig = sc;
  }

  /**
   * Clear any iterator sampler configuration.
   *
   * @since 1.8.0
   */
  public void clearIteratorSamplerConfiguration() {
    this.iteratorSamplerConfig = null;
  }

  /**
   * @return currently set iterator sampler configuration.
   *
   * @since 1.8.0
   */

  public SamplerConfiguration getIteratorSamplerConfiguration() {
    return iteratorSamplerConfig;
  }
}

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
package org.apache.accumulo.tserver;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.EmptyIterator;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;
import org.apache.accumulo.core.iteratorsImpl.system.LocalityGroupIterator;
import org.apache.accumulo.core.iteratorsImpl.system.LocalityGroupIterator.LocalityGroup;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SourceSwitchingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SourceSwitchingIterator.DataSource;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.sample.impl.SamplerFactory;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.Partitioner;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.PreAllocatedArray;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryMap {
  private SimpleMap map = null;

  private static final Logger log = LoggerFactory.getLogger(InMemoryMap.class);

  private final ServerContext context;
  private volatile String memDumpFile = null;
  private final String memDumpDir;
  private final String mapType;
  private final TableId tableId;

  private Map<String,Set<ByteSequence>> lggroups;

  private static Pair<SamplerConfigurationImpl,Sampler> getSampler(AccumuloConfiguration config) {
    try {
      SamplerConfigurationImpl sampleConfig = SamplerConfigurationImpl.newSamplerConfig(config);
      if (sampleConfig == null) {
        return new Pair<>(null, null);
      }

      return new Pair<>(sampleConfig, SamplerFactory.newSampler(sampleConfig, config));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static final String TYPE_NATIVE_MAP_WRAPPER = "NativeMapWrapper";
  public static final String TYPE_DEFAULT_MAP = "DefaultMap";
  public static final String TYPE_LOCALITY_GROUP_MAP = "LocalityGroupMap";
  public static final String TYPE_LOCALITY_GROUP_MAP_NATIVE = "LocalityGroupMap with native";

  private AtomicReference<Pair<SamplerConfigurationImpl,Sampler>> samplerRef =
      new AtomicReference<>(null);

  private AccumuloConfiguration config;

  // defer creating sampler until first write. This was done because an empty sample map configured
  // with no sampler will not flush after a user changes sample
  // config.
  private Sampler getOrCreateSampler() {
    Pair<SamplerConfigurationImpl,Sampler> pair = samplerRef.get();
    if (pair == null) {
      pair = getSampler(config);
      if (!samplerRef.compareAndSet(null, pair)) {
        pair = samplerRef.get();
      }
    }

    return pair.getSecond();
  }

  public InMemoryMap(AccumuloConfiguration config, ServerContext context, TableId tableId) {

    boolean useNativeMap = config.getBoolean(Property.TSERV_NATIVEMAP_ENABLED);

    this.memDumpDir = config.get(Property.TSERV_MEMDUMP_DIR);
    this.lggroups = LocalityGroupUtil.getLocalityGroupsIgnoringErrors(config, tableId);

    this.config = config;
    this.context = context;
    this.tableId = tableId;

    SimpleMap allMap;
    SimpleMap sampleMap;

    if (lggroups.isEmpty()) {
      allMap = newMap(useNativeMap);
      sampleMap = newMap(useNativeMap);
      mapType = useNativeMap ? TYPE_NATIVE_MAP_WRAPPER : TYPE_DEFAULT_MAP;
    } else {
      allMap = new LocalityGroupMap(lggroups, useNativeMap);
      sampleMap = new LocalityGroupMap(lggroups, useNativeMap);
      mapType = useNativeMap ? TYPE_LOCALITY_GROUP_MAP_NATIVE : TYPE_LOCALITY_GROUP_MAP;
    }

    map = new SampleMap(allMap, sampleMap);
  }

  private static SimpleMap newMap(boolean useNativeMap) {
    if (useNativeMap) {
      try {
        return new NativeMapWrapper();
      } catch (Exception t) {
        log.error("Failed to create native map", t);
      }
    }

    return new DefaultMap();
  }

  /**
   * Description of the type of SimpleMap that is created.
   * <p>
   * If no locality groups are present, the SimpleMap is either TYPE_DEFAULT_MAP or
   * TYPE_NATIVE_MAP_WRAPPER. If there is one more locality groups, then the InMemoryMap has an
   * array for simple maps that either contain either TYPE_LOCALITY_GROUP_MAP which contains
   * DefaultMaps or TYPE_LOCALITY_GROUP_MAP_NATIVE which contains NativeMapWrappers.
   *
   * @return String that describes the Map type
   */
  public String getMapType() {
    return mapType;
  }

  private interface SimpleMap {

    int size();

    InterruptibleIterator skvIterator(SamplerConfigurationImpl samplerConfig);

    void delete();

    long getMemoryUsed();

    void mutate(List<Mutation> mutations, int kvCount);
  }

  private class SampleMap implements SimpleMap {

    private SimpleMap map;
    private SimpleMap sample;

    public SampleMap(SimpleMap map, SimpleMap sampleMap) {
      this.map = map;
      this.sample = sampleMap;
    }

    @Override
    public int size() {
      return map.size();
    }

    @Override
    public InterruptibleIterator skvIterator(SamplerConfigurationImpl samplerConfig) {
      if (samplerConfig == null) {
        return map.skvIterator(null);
      } else {
        Pair<SamplerConfigurationImpl,Sampler> samplerAndConf = samplerRef.get();
        if (samplerAndConf == null) {
          return EmptyIterator.EMPTY_ITERATOR;
        } else if (samplerAndConf.getFirst() != null
            && samplerAndConf.getFirst().equals(samplerConfig)) {
          return sample.skvIterator(null);
        } else {
          throw new SampleNotPresentException();
        }
      }
    }

    @Override
    public void delete() {
      map.delete();
      sample.delete();
    }

    @Override
    public long getMemoryUsed() {
      return map.getMemoryUsed() + sample.getMemoryUsed();
    }

    @Override
    public void mutate(List<Mutation> mutations, int kvCount) {
      map.mutate(mutations, kvCount);

      Sampler sampler = getOrCreateSampler();
      if (sampler != null) {
        List<Mutation> sampleMutations = null;

        for (Mutation m : mutations) {
          List<ColumnUpdate> colUpdates = m.getUpdates();
          List<ColumnUpdate> sampleColUpdates = null;
          for (ColumnUpdate cvp : colUpdates) {
            Key k = new Key(m.getRow(), cvp.getColumnFamily(), cvp.getColumnQualifier(),
                cvp.getColumnVisibility(), cvp.getTimestamp(), cvp.isDeleted(), false);
            if (sampler.accept(k)) {
              if (sampleColUpdates == null) {
                sampleColUpdates = new ArrayList<>();
              }
              sampleColUpdates.add(cvp);
            }
          }

          if (sampleColUpdates != null) {
            if (sampleMutations == null) {
              sampleMutations = new ArrayList<>();
            }

            sampleMutations
                .add(new LocalityGroupUtil.PartitionedMutation(m.getRow(), sampleColUpdates));
          }
        }

        if (sampleMutations != null) {
          sample.mutate(sampleMutations, kvCount);
        }
      }
    }
  }

  private static class LocalityGroupMap implements SimpleMap {

    private PreAllocatedArray<Map<ByteSequence,MutableLong>> groupFams;

    // the last map in the array is the default locality group
    private SimpleMap[] maps;
    private Partitioner partitioner;
    private PreAllocatedArray<List<Mutation>> partitioned;

    LocalityGroupMap(Map<String,Set<ByteSequence>> groups, boolean useNativeMap) {
      this.groupFams = new PreAllocatedArray<>(groups.size());
      this.maps = new SimpleMap[groups.size() + 1];
      this.partitioned = new PreAllocatedArray<>(groups.size() + 1);

      for (int i = 0; i < maps.length; i++) {
        maps[i] = newMap(useNativeMap);
      }

      int count = 0;
      for (Set<ByteSequence> cfset : groups.values()) {
        HashMap<ByteSequence,MutableLong> map = new HashMap<>();
        for (ByteSequence bs : cfset) {
          map.put(bs, new MutableLong(1));
        }
        this.groupFams.set(count++, map);
      }

      partitioner = new LocalityGroupUtil.Partitioner(this.groupFams);

      for (int i = 0; i < partitioned.length; i++) {
        partitioned.set(i, new ArrayList<>());
      }
    }

    @Override
    public int size() {
      int sum = 0;
      for (SimpleMap map : maps) {
        sum += map.size();
      }
      return sum;
    }

    @Override
    public InterruptibleIterator skvIterator(SamplerConfigurationImpl samplerConfig) {
      if (samplerConfig != null) {
        throw new SampleNotPresentException();
      }

      LocalityGroup[] groups = new LocalityGroup[maps.length];
      for (int i = 0; i < groups.length; i++) {
        if (i < groupFams.length) {
          groups[i] = new LocalityGroup(maps[i].skvIterator(null), groupFams.get(i), false);
        } else {
          groups[i] = new LocalityGroup(maps[i].skvIterator(null), null, true);
        }
      }

      return new LocalityGroupIterator(groups);
    }

    @Override
    public void delete() {
      for (SimpleMap map : maps) {
        map.delete();
      }
    }

    @Override
    public long getMemoryUsed() {
      long sum = 0;
      for (SimpleMap map : maps) {
        sum += map.getMemoryUsed();
      }
      return sum;
    }

    @Override
    public synchronized void mutate(List<Mutation> mutations, int kvCount) {
      // this method is synchronized because it reuses objects to avoid allocation,
      // currently, the method that calls this is synchronized so there is no
      // loss in parallelism.... synchronization was added here for future proofing

      try {
        partitioner.partition(mutations, partitioned);

        for (int i = 0; i < partitioned.length; i++) {
          if (!partitioned.get(i).isEmpty()) {
            maps[i].mutate(partitioned.get(i), kvCount);
            for (Mutation m : partitioned.get(i)) {
              kvCount += m.getUpdates().size();
            }
          }
        }
      } finally {
        // clear immediately so mutations can be garbage collected
        for (List<Mutation> list : partitioned) {
          list.clear();
        }
      }
    }

  }

  private static class DefaultMap implements SimpleMap {
    private ConcurrentSkipListMap<Key,Value> map =
        new ConcurrentSkipListMap<>(new MemKeyComparator());
    private AtomicLong bytesInMemory = new AtomicLong();
    private AtomicInteger size = new AtomicInteger();

    public void put(Key key, Value value) {
      // Always a MemKey, so account for the kvCount int
      bytesInMemory.addAndGet(key.getLength() + 4);
      bytesInMemory.addAndGet(value.getSize());
      if (map.put(key, value) == null) {
        size.incrementAndGet();
      }
    }

    @Override
    public int size() {
      return size.get();
    }

    @Override
    public InterruptibleIterator skvIterator(SamplerConfigurationImpl samplerConfig) {
      if (samplerConfig != null) {
        throw new SampleNotPresentException();
      }
      if (map == null) {
        throw new IllegalStateException();
      }

      return new SortedMapIterator(map);
    }

    @Override
    public synchronized void delete() {
      map = null;
    }

    public long getOverheadPerEntry() {
      // all of the java objects that are used to hold the
      // data and make it searchable have overhead... this
      // overhead is estimated using test.EstimateInMemMapOverhead
      // and is in bytes.. the estimates were obtained by running
      // java 6_16 in 64 bit server mode

      return 200;
    }

    @Override
    public void mutate(List<Mutation> mutations, int kvCount) {
      for (Mutation m : mutations) {
        for (ColumnUpdate cvp : m.getUpdates()) {
          Key newKey = new MemKey(m.getRow(), cvp.getColumnFamily(), cvp.getColumnQualifier(),
              cvp.getColumnVisibility(), cvp.getTimestamp(), cvp.isDeleted(), false, kvCount++);
          Value value = new Value(cvp.getValue());
          put(newKey, value);
        }
      }
    }

    @Override
    public long getMemoryUsed() {
      return bytesInMemory.get() + (size() * getOverheadPerEntry());
    }
  }

  private static class NativeMapWrapper implements SimpleMap {
    private NativeMap nativeMap;

    NativeMapWrapper() {
      nativeMap = new NativeMap();
    }

    @Override
    public int size() {
      return nativeMap.size();
    }

    @Override
    public InterruptibleIterator skvIterator(SamplerConfigurationImpl samplerConfig) {
      if (samplerConfig != null) {
        throw new SampleNotPresentException();
      }
      return (InterruptibleIterator) nativeMap.skvIterator();
    }

    @Override
    public void delete() {
      nativeMap.delete();
    }

    @Override
    public long getMemoryUsed() {
      return nativeMap.getMemoryUsed();
    }

    @Override
    public void mutate(List<Mutation> mutations, int kvCount) {
      nativeMap.mutate(mutations, kvCount);
    }
  }

  private AtomicInteger nextKVCount = new AtomicInteger(1);
  private AtomicInteger kvCount = new AtomicInteger(0);

  private Object writeSerializer = new Object();

  /**
   * Applies changes to a row in the InMemoryMap
   *
   */
  public void mutate(List<Mutation> mutations, int numKVs) {
    // Can not update mutationCount while writes that started before
    // are in progress, this would cause partial mutations to be seen.
    // Also, can not continue until mutation count is updated, because
    // a read may not see a successful write. Therefore writes must
    // wait for writes that started before to finish.
    //
    // using separate lock from this map, to allow read/write in parallel
    synchronized (writeSerializer) {
      int kv = nextKVCount.getAndAdd(numKVs);
      try {
        map.mutate(mutations, kv);
      } finally {
        kvCount.set(kv + numKVs - 1);
      }
    }
  }

  /**
   * Returns a long representing the size of the InMemoryMap
   *
   * @return bytesInMemory
   */
  public synchronized long estimatedSizeInBytes() {
    if (map == null) {
      return 0;
    }

    return map.getMemoryUsed();
  }

  public synchronized long getNumEntries() {
    if (map == null) {
      return 0;
    }
    return map.size();
  }

  private final Set<MemoryIterator> activeIters = Collections.synchronizedSet(new HashSet<>());

  class MemoryDataSource implements DataSource {

    private boolean switched = false;
    private InterruptibleIterator iter;
    private FileSKVIterator reader;
    private MemoryDataSource parent;
    private IteratorEnvironment env;
    private AtomicBoolean iflag;
    private SamplerConfigurationImpl iteratorSamplerConfig;

    private SamplerConfigurationImpl getSamplerConfig() {
      if (env != null) {
        if (env.isSamplingEnabled()) {
          return new SamplerConfigurationImpl(env.getSamplerConfiguration());
        } else {
          return null;
        }
      } else {
        return iteratorSamplerConfig;
      }
    }

    MemoryDataSource(SamplerConfigurationImpl samplerConfig) {
      this(null, false, null, null, samplerConfig);
    }

    public MemoryDataSource(MemoryDataSource parent, boolean switched, IteratorEnvironment env,
        AtomicBoolean iflag, SamplerConfigurationImpl samplerConfig) {
      this.parent = parent;
      this.switched = switched;
      this.env = env;
      this.iflag = iflag;
      this.iteratorSamplerConfig = samplerConfig;
    }

    @Override
    public boolean isCurrent() {
      if (switched) {
        return true;
      } else {
        return memDumpFile == null;
      }
    }

    @Override
    public DataSource getNewDataSource() {
      if (switched) {
        throw new IllegalStateException();
      }

      if (!isCurrent()) {
        switched = true;
        iter = null;
        try {
          // ensure files are referenced even if iterator was never seeked before
          iterator();
        } catch (IOException e) {
          throw new RuntimeException();
        }
      }

      return this;
    }

    private synchronized FileSKVIterator getReader() throws IOException {
      if (reader == null) {
        Configuration conf = context.getHadoopConf();
        FileSystem fs = FileSystem.getLocal(conf);

        TableConfiguration tableConf = context.getTableConfiguration(tableId);
        reader = new RFileOperations().newReaderBuilder()
            .forFile(memDumpFile, fs, conf, tableConf.getCryptoService())
            .withTableConfiguration(tableConf).seekToBeginning().build();
        if (iflag != null) {
          reader.setInterruptFlag(iflag);
        }

        if (getSamplerConfig() != null) {
          reader = reader.getSample(getSamplerConfig());
        }
      }

      return reader;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> iterator() throws IOException {
      if (iter == null) {
        if (!switched) {
          iter = map.skvIterator(getSamplerConfig());
          if (iflag != null) {
            iter.setInterruptFlag(iflag);
          }
        } else {
          if (parent == null) {
            iter = new MemKeyConversionIterator(getReader());
          } else {
            synchronized (parent) {
              // synchronize deep copy operation on parent, this prevents multiple threads from deep
              // copying the rfile shared from parent its possible that the
              // thread deleting an InMemoryMap and scan thread could be switching different deep
              // copies
              iter = new MemKeyConversionIterator(parent.getReader().deepCopy(env));
            }
          }
        }
      }

      return iter;
    }

    @Override
    public DataSource getDeepCopyDataSource(IteratorEnvironment env) {
      return new MemoryDataSource(parent == null ? this : parent, switched, env, iflag,
          iteratorSamplerConfig);
    }

    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      this.iflag = flag;
    }

  }

  public class MemoryIterator extends WrappingIterator implements InterruptibleIterator {

    private AtomicBoolean closed;
    private SourceSwitchingIterator ssi;
    private MemoryDataSource mds;

    private MemoryIterator(InterruptibleIterator source) {
      this(source, new AtomicBoolean(false));
    }

    private MemoryIterator(SortedKeyValueIterator<Key,Value> source, AtomicBoolean closed) {
      setSource(source);
      this.closed = closed;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      return new MemoryIterator(getSource().deepCopy(env), closed);
    }

    public void close() {

      synchronized (this) {
        if (closed.compareAndSet(false, true)) {
          try {
            if (mds.reader != null) {
              mds.reader.close();
            }
          } catch (IOException e) {
            log.warn("{}", e.getMessage(), e);
          }
        }
      }

      // remove outside of sync to avoid deadlock
      activeIters.remove(this);
    }

    private synchronized boolean switchNow() throws IOException {
      if (closed.get()) {
        return false;
      }

      ssi.switchNow();
      return true;
    }

    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      ((InterruptibleIterator) getSource()).setInterruptFlag(flag);
    }

    private void setSSI(SourceSwitchingIterator ssi) {
      this.ssi = ssi;
    }

    public void setMDS(MemoryDataSource mds) {
      this.mds = mds;
    }

  }

  public synchronized MemoryIterator skvIterator(SamplerConfigurationImpl iteratorSamplerConfig) {
    if (map == null) {
      throw new NullPointerException();
    }

    if (deleted) {
      throw new IllegalStateException("Can not obtain iterator after map deleted");
    }

    int mc = kvCount.get();
    MemoryDataSource mds = new MemoryDataSource(iteratorSamplerConfig);
    SourceSwitchingIterator ssi = new SourceSwitchingIterator(mds);
    MemoryIterator mi = new MemoryIterator(new PartialMutationSkippingIterator(ssi, mc));
    mi.setSSI(ssi);
    mi.setMDS(mds);
    activeIters.add(mi);
    return mi;
  }

  public SortedKeyValueIterator<Key,Value> compactionIterator() {

    if (nextKVCount.get() - 1 != kvCount.get()) {
      throw new IllegalStateException("Memory map in unexpected state : nextKVCount = "
          + nextKVCount.get() + " kvCount = " + kvCount.get());
    }

    return map.skvIterator(null);
  }

  private boolean deleted = false;

  public void delete(long waitTime) {

    synchronized (this) {
      if (deleted) {
        throw new IllegalStateException("Double delete");
      }

      deleted = true;
    }

    long t1 = System.currentTimeMillis();

    while (!activeIters.isEmpty() && System.currentTimeMillis() - t1 < waitTime) {
      sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
    }

    if (!activeIters.isEmpty()) {
      // dump memmap exactly as is to a tmp file on disk, and switch scans to that temp file
      try {
        Configuration conf = context.getHadoopConf();
        FileSystem fs = FileSystem.getLocal(conf);

        String tmpFile = memDumpDir + "/memDump" + UUID.randomUUID() + "." + RFile.EXTENSION;

        Configuration newConf = new Configuration(conf);
        newConf.setInt("io.seqfile.compress.blocksize", 100000);

        AccumuloConfiguration aconf = context.getConfiguration();

        if (getOrCreateSampler() != null) {
          aconf = createSampleConfig(aconf);
        }

        TableConfiguration tableConf = context.getTableConfiguration(tableId);
        FileSKVWriter out = new RFileOperations().newWriterBuilder()
            .forFile(tmpFile, fs, newConf, tableConf.getCryptoService())
            .withTableConfiguration(aconf).build();

        InterruptibleIterator iter = map.skvIterator(null);

        HashSet<ByteSequence> allfams = new HashSet<>();

        for (Entry<String,Set<ByteSequence>> entry : lggroups.entrySet()) {
          allfams.addAll(entry.getValue());
          out.startNewLocalityGroup(entry.getKey(), entry.getValue());
          iter.seek(new Range(), entry.getValue(), true);
          dumpLocalityGroup(out, iter);
        }

        out.startDefaultLocalityGroup();
        iter.seek(new Range(), allfams, false);

        dumpLocalityGroup(out, iter);

        out.close();

        log.debug("Created mem dump file {}", tmpFile);

        memDumpFile = tmpFile;

        synchronized (activeIters) {
          for (MemoryIterator mi : activeIters) {
            mi.switchNow();
          }
        }

        // rely on unix behavior that file will be deleted when last
        // reader closes it
        fs.delete(new Path(memDumpFile), true);

      } catch (IOException ioe) {
        log.error("Failed to create mem dump file", ioe);

        while (!activeIters.isEmpty()) {
          sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
      }

    }

    SimpleMap tmpMap = map;

    synchronized (this) {
      map = null;
    }

    tmpMap.delete();
  }

  private AccumuloConfiguration createSampleConfig(AccumuloConfiguration siteConf) {
    ConfigurationCopy confCopy = new ConfigurationCopy(siteConf.stream()
        .filter(input -> !input.getKey().startsWith(Property.TABLE_SAMPLER.getKey())));

    for (Entry<String,String> entry : samplerRef.get().getFirst().toTablePropertiesMap()
        .entrySet()) {
      confCopy.set(entry.getKey(), entry.getValue());
    }

    siteConf = confCopy;
    return siteConf;
  }

  private void dumpLocalityGroup(FileSKVWriter out, InterruptibleIterator iter) throws IOException {
    while (iter.hasTop() && !activeIters.isEmpty()) {
      // RFile does not support MemKey, so we move the kv count into the value only for the RFile.
      // There is no need to change the MemKey to a normal key because the kvCount info gets lost
      // when it is written
      out.append(iter.getTopKey(),
          MemValue.encode(iter.getTopValue(), ((MemKey) iter.getTopKey()).getKVCount()));
      iter.next();
    }
  }
}

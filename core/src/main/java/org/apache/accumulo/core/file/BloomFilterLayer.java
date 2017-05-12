/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.core.file;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.bloomfilter.DynamicBloomFilter;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.keyfunctor.KeyFunctor;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.fate.util.LoggingRunnable;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that sits on top of different accumulo file formats and provides bloom filter functionality.
 *
 */
public class BloomFilterLayer {
  private static final Logger LOG = LoggerFactory.getLogger(BloomFilterLayer.class);
  public static final String BLOOM_FILE_NAME = "acu_bloom";
  public static final int HASH_COUNT = 5;

  private static ExecutorService loadThreadPool = null;

  private static synchronized ExecutorService getLoadThreadPool(int maxLoadThreads) {
    if (loadThreadPool != null) {
      return loadThreadPool;
    }

    if (maxLoadThreads > 0) {
      BlockingQueue<Runnable> q = new LinkedBlockingQueue<>();
      loadThreadPool = new ThreadPoolExecutor(0, maxLoadThreads, 60, TimeUnit.SECONDS, q, new NamingThreadFactory("bloom-loader"));
    }

    return loadThreadPool;
  }

  public static class Writer implements FileSKVWriter {
    private DynamicBloomFilter bloomFilter;
    private int numKeys;
    private int vectorSize;

    private FileSKVWriter writer;
    private KeyFunctor transformer = null;
    private boolean closed = false;
    private long length = -1;

    Writer(FileSKVWriter writer, AccumuloConfiguration acuconf, boolean useAccumuloStart) {
      this.writer = writer;
      initBloomFilter(acuconf, useAccumuloStart);
    }

    private synchronized void initBloomFilter(AccumuloConfiguration acuconf, boolean useAccumuloStart) {

      numKeys = acuconf.getCount(Property.TABLE_BLOOM_SIZE);
      // vector size should be <code>-kn / (ln(1 - c^(1/k)))</code> bits for
      // single key, where <code> is the number of hash functions,
      // <code>n</code> is the number of keys and <code>c</code> is the desired
      // max. error rate.
      // Our desired error rate is by default 0.005, i.e. 0.5%
      double errorRate = acuconf.getFraction(Property.TABLE_BLOOM_ERRORRATE);
      vectorSize = (int) Math.ceil(-HASH_COUNT * numKeys / Math.log(1.0 - Math.pow(errorRate, 1.0 / HASH_COUNT)));
      bloomFilter = new DynamicBloomFilter(vectorSize, HASH_COUNT, Hash.parseHashType(acuconf.get(Property.TABLE_BLOOM_HASHTYPE)), numKeys);

      /**
       * load KeyFunctor
       */
      try {
        String context = acuconf.get(Property.TABLE_CLASSPATH);
        String classname = acuconf.get(Property.TABLE_BLOOM_KEY_FUNCTOR);
        Class<? extends KeyFunctor> clazz;
        if (!useAccumuloStart)
          clazz = Writer.class.getClassLoader().loadClass(classname).asSubclass(KeyFunctor.class);
        else if (context != null && !context.equals(""))
          clazz = AccumuloVFSClassLoader.getContextManager().loadClass(context, classname, KeyFunctor.class);
        else
          clazz = AccumuloVFSClassLoader.loadClass(classname, KeyFunctor.class);

        transformer = clazz.newInstance();

      } catch (Exception e) {
        LOG.error("Failed to find KeyFunctor: " + acuconf.get(Property.TABLE_BLOOM_KEY_FUNCTOR), e);
        throw new IllegalArgumentException("Failed to find KeyFunctor: " + acuconf.get(Property.TABLE_BLOOM_KEY_FUNCTOR));

      }

    }

    @Override
    public synchronized void append(org.apache.accumulo.core.data.Key key, Value val) throws IOException {
      writer.append(key, val);
      Key bloomKey = transformer.transform(key);
      if (bloomKey.getBytes().length > 0)
        bloomFilter.add(bloomKey);
    }

    @Override
    public synchronized void close() throws IOException {

      if (closed)
        return;

      DataOutputStream out = writer.createMetaStore(BLOOM_FILE_NAME);
      out.writeUTF(transformer.getClass().getName());
      bloomFilter.write(out);
      out.flush();
      out.close();
      writer.close();
      length = writer.getLength();
      closed = true;
    }

    @Override
    public DataOutputStream createMetaStore(String name) throws IOException {
      return writer.createMetaStore(name);
    }

    @Override
    public void startDefaultLocalityGroup() throws IOException {
      writer.startDefaultLocalityGroup();

    }

    @Override
    public void startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies) throws IOException {
      writer.startNewLocalityGroup(name, columnFamilies);
    }

    @Override
    public boolean supportsLocalityGroups() {
      return writer.supportsLocalityGroups();
    }

    @Override
    public long getLength() throws IOException {
      if (closed) {
        return length;
      }
      return writer.getLength();
    }
  }

  static class BloomFilterLoader {

    private volatile DynamicBloomFilter bloomFilter;
    private int loadRequest = 0;
    private int loadThreshold = 1;
    private int maxLoadThreads;
    private Runnable loadTask;
    private volatile KeyFunctor transformer = null;
    private volatile boolean closed = false;

    BloomFilterLoader(final FileSKVIterator reader, AccumuloConfiguration acuconf) {

      maxLoadThreads = acuconf.getCount(Property.TSERV_BLOOM_LOAD_MAXCONCURRENT);

      loadThreshold = acuconf.getCount(Property.TABLE_BLOOM_LOAD_THRESHOLD);

      final String context = acuconf.get(Property.TABLE_CLASSPATH);

      loadTask = new Runnable() {
        @Override
        public void run() {

          // no need to load the bloom filter if the map file is closed
          if (closed)
            return;
          String ClassName = null;
          DataInputStream in = null;

          try {
            in = reader.getMetaStore(BLOOM_FILE_NAME);
            DynamicBloomFilter tmpBloomFilter = new DynamicBloomFilter();

            // check for closed again after open but before reading the bloom filter in
            if (closed)
              return;

            /**
             * Load classname for keyFunctor
             */
            ClassName = in.readUTF();

            Class<? extends KeyFunctor> clazz;
            if (context != null && !context.equals(""))
              clazz = AccumuloVFSClassLoader.getContextManager().loadClass(context, ClassName, KeyFunctor.class);
            else
              clazz = AccumuloVFSClassLoader.loadClass(ClassName, KeyFunctor.class);
            transformer = clazz.newInstance();

            /**
             * read in bloom filter
             */

            tmpBloomFilter.readFields(in);
            // only set the bloom filter after it is fully constructed
            bloomFilter = tmpBloomFilter;
          } catch (NoSuchMetaStoreException nsme) {
            // file does not have a bloom filter, ignore it
          } catch (IOException ioe) {
            if (!closed)
              LOG.warn("Can't open BloomFilter", ioe);
            else
              LOG.debug("Can't open BloomFilter, file closed : {}", ioe.getMessage());

            bloomFilter = null;
          } catch (ClassNotFoundException e) {
            LOG.error("Failed to find KeyFunctor in config: " + ClassName, e);
            bloomFilter = null;
          } catch (InstantiationException e) {
            LOG.error("Could not instantiate KeyFunctor: " + ClassName, e);
            bloomFilter = null;
          } catch (IllegalAccessException e) {
            LOG.error("Illegal acess exception", e);
            bloomFilter = null;
          } catch (RuntimeException rte) {
            if (!closed)
              throw rte;
            else
              LOG.debug("Can't open BloomFilter, RTE after closed ", rte);
          } finally {
            if (in != null) {
              try {
                in.close();
              } catch (IOException e) {
                LOG.warn("Failed to close ", e);
              }
            }
          }
        }
      };

      initiateLoad(maxLoadThreads);

    }

    private synchronized void initiateLoad(int maxLoadThreads) {
      // ensure only one thread initiates loading of bloom filter by
      // only taking action when loadTask != null
      if (loadTask != null && loadRequest >= loadThreshold) {
        try {
          ExecutorService ltp = getLoadThreadPool(maxLoadThreads);
          if (ltp == null) {
            // load the bloom filter in the foreground
            loadTask.run();
          } else {
            // load the bloom filter in the background
            ltp.execute(new LoggingRunnable(LOG, loadTask));
          }
        } finally {
          // set load task to null so no one else can initiate the load
          loadTask = null;
        }
      }

      loadRequest++;
    }

    /**
     * Checks if this {@link RFile} contains keys from this range. The membership test is performed using a Bloom filter, so the result has always non-zero
     * probability of false positives.
     *
     * @param range
     *          range of keys to check
     * @return false iff key doesn't exist, true if key probably exists.
     */
    boolean probablyHasKey(Range range) throws IOException {
      if (bloomFilter == null) {
        initiateLoad(maxLoadThreads);
        if (bloomFilter == null)
          return true;
      }

      Key bloomKey = transformer.transform(range);

      if (bloomKey == null || bloomKey.getBytes().length == 0)
        return true;

      return bloomFilter.membershipTest(bloomKey);
    }

    public void close() {
      this.closed = true;
    }
  }

  public static class Reader implements FileSKVIterator {

    private BloomFilterLoader bfl;
    private FileSKVIterator reader;

    public Reader(FileSKVIterator reader, AccumuloConfiguration acuconf) {
      this.reader = reader;
      bfl = new BloomFilterLoader(reader, acuconf);
    }

    private Reader(FileSKVIterator src, BloomFilterLoader bfl) {
      this.reader = src;
      this.bfl = bfl;
    }

    private boolean checkSuper = true;

    @Override
    public boolean hasTop() {
      return checkSuper ? reader.hasTop() : false;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

      if (!bfl.probablyHasKey(range)) {
        checkSuper = false;
      } else {
        reader.seek(range, columnFamilies, inclusive);
        checkSuper = true;
      }
    }

    @Override
    public synchronized void close() throws IOException {
      bfl.close();
      reader.close();
    }

    @Override
    public org.apache.accumulo.core.data.Key getFirstKey() throws IOException {
      return reader.getFirstKey();
    }

    @Override
    public org.apache.accumulo.core.data.Key getLastKey() throws IOException {
      return reader.getLastKey();
    }

    @Override
    public SortedKeyValueIterator<org.apache.accumulo.core.data.Key,Value> deepCopy(IteratorEnvironment env) {
      return new BloomFilterLayer.Reader((FileSKVIterator) reader.deepCopy(env), bfl);
    }

    @Override
    public org.apache.accumulo.core.data.Key getTopKey() {
      return reader.getTopKey();
    }

    @Override
    public Value getTopValue() {
      return reader.getTopValue();
    }

    @Override
    public void init(SortedKeyValueIterator<org.apache.accumulo.core.data.Key,Value> source, Map<String,String> options, IteratorEnvironment env)
        throws IOException {
      throw new UnsupportedOperationException();

    }

    @Override
    public void next() throws IOException {
      reader.next();
    }

    @Override
    public DataInputStream getMetaStore(String name) throws IOException {
      return reader.getMetaStore(name);
    }

    @Override
    public void closeDeepCopies() throws IOException {
      reader.closeDeepCopies();
    }

    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      reader.setInterruptFlag(flag);
    }

    @Override
    public FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig) {
      return new BloomFilterLayer.Reader(reader.getSample(sampleConfig), bfl);
    }

  }

  public static void main(String[] args) throws IOException {
    PrintStream out = System.out;

    Random r = new Random();

    HashSet<Integer> valsSet = new HashSet<>();

    for (int i = 0; i < 100000; i++) {
      valsSet.add(r.nextInt(Integer.MAX_VALUE));
    }

    ArrayList<Integer> vals = new ArrayList<>(valsSet);
    Collections.sort(vals);

    ConfigurationCopy acuconf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    acuconf.set(Property.TABLE_BLOOM_ENABLED, "true");
    acuconf.set(Property.TABLE_BLOOM_KEY_FUNCTOR, "accumulo.core.file.keyfunctor.ColumnFamilyFunctor");
    acuconf.set(Property.TABLE_FILE_TYPE, RFile.EXTENSION);
    acuconf.set(Property.TABLE_BLOOM_LOAD_THRESHOLD, "1");
    acuconf.set(Property.TSERV_BLOOM_LOAD_MAXCONCURRENT, "1");

    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);

    String suffix = FileOperations.getNewFileExtension(acuconf);
    String fname = "/tmp/test." + suffix;
    FileSKVWriter bmfw = FileOperations.getInstance().newWriterBuilder().forFile(fname, fs, conf).withTableConfiguration(acuconf).build();

    long t1 = System.currentTimeMillis();

    bmfw.startDefaultLocalityGroup();

    for (Integer i : vals) {
      String fi = String.format("%010d", i);
      bmfw.append(new org.apache.accumulo.core.data.Key(new Text("r" + fi), new Text("cf1")), new Value(("v" + fi).getBytes(UTF_8)));
      bmfw.append(new org.apache.accumulo.core.data.Key(new Text("r" + fi), new Text("cf2")), new Value(("v" + fi).getBytes(UTF_8)));
    }

    long t2 = System.currentTimeMillis();

    out.printf("write rate %6.2f%n", vals.size() / ((t2 - t1) / 1000.0));

    bmfw.close();

    t1 = System.currentTimeMillis();
    FileSKVIterator bmfr = FileOperations.getInstance().newReaderBuilder().forFile(fname, fs, conf).withTableConfiguration(acuconf).build();
    t2 = System.currentTimeMillis();
    out.println("Opened " + fname + " in " + (t2 - t1));

    t1 = System.currentTimeMillis();

    int hits = 0;
    for (int i = 0; i < 5000; i++) {
      int row = r.nextInt(Integer.MAX_VALUE);
      String fi = String.format("%010d", row);
      // bmfr.seek(new Range(new Text("r"+fi)));
      org.apache.accumulo.core.data.Key k1 = new org.apache.accumulo.core.data.Key(new Text("r" + fi), new Text("cf1"));
      bmfr.seek(new Range(k1, true, k1.followingKey(PartialKey.ROW_COLFAM), false), new ArrayList<ByteSequence>(), false);
      if (valsSet.contains(row)) {
        hits++;
        if (!bmfr.hasTop()) {
          out.println("ERROR " + row);
        }
      }
    }

    t2 = System.currentTimeMillis();

    out.printf("random lookup rate : %6.2f%n", 5000 / ((t2 - t1) / 1000.0));
    out.println("hits = " + hits);

    int count = 0;

    t1 = System.currentTimeMillis();

    for (Integer row : valsSet) {
      String fi = String.format("%010d", row);
      // bmfr.seek(new Range(new Text("r"+fi)));

      org.apache.accumulo.core.data.Key k1 = new org.apache.accumulo.core.data.Key(new Text("r" + fi), new Text("cf1"));
      bmfr.seek(new Range(k1, true, k1.followingKey(PartialKey.ROW_COLFAM), false), new ArrayList<ByteSequence>(), false);

      if (!bmfr.hasTop()) {
        out.println("ERROR 2 " + row);
      }

      count++;

      if (count >= 500) {
        break;
      }
    }

    t2 = System.currentTimeMillis();

    out.printf("existant lookup rate %6.2f%n", 500 / ((t2 - t1) / 1000.0));
    out.println("expected hits 500.  Receive hits: " + count);
    bmfr.close();
  }
}

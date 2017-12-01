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
package org.apache.accumulo.core.file.rfile;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.NoSuchMetaStoreException;
import org.apache.accumulo.core.file.blockfile.ABlockReader;
import org.apache.accumulo.core.file.blockfile.ABlockWriter;
import org.apache.accumulo.core.file.blockfile.BlockFileReader;
import org.apache.accumulo.core.file.blockfile.BlockFileWriter;
import org.apache.accumulo.core.file.rfile.BlockIndex.BlockIndexEntry;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.IndexEntry;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.Reader.IndexIterator;
import org.apache.accumulo.core.file.rfile.RelativeKey.SkippR;
import org.apache.accumulo.core.file.rfile.bcfile.MetaBlockDoesNotExist;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.HeapIterator;
import org.apache.accumulo.core.iterators.system.InterruptibleIterator;
import org.apache.accumulo.core.iterators.system.LocalityGroupIterator;
import org.apache.accumulo.core.iterators.system.LocalityGroupIterator.LocalityGroup;
import org.apache.accumulo.core.iterators.system.LocalityGroupIterator.LocalityGroupContext;
import org.apache.accumulo.core.iterators.system.LocalityGroupIterator.LocalityGroupSeekCache;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.MutableByteSequence;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class RFile {

  public static final String EXTENSION = "rf";

  private static final Logger log = LoggerFactory.getLogger(RFile.class);

  private RFile() {}

  private static final int RINDEX_MAGIC = 0x20637474;

  static final int RINDEX_VER_8 = 8; // Added sample storage. There is a sample locality group for each locality group. Sample are built using a Sampler and
                                     // sampler configuration. The Sampler and its configuration are stored in RFile. Persisting the method of producing the
                                     // sample allows a user of RFile to determine if the sample is useful.
                                     //
                                     // Selected smaller keys for index by doing two things. First internal stats were used to look for keys that were below
                                     // average in size for the index. Also keys that were statistically large were excluded from the index. Second shorter keys
                                     // (that may not exist in data) were generated for the index.
  static final int RINDEX_VER_7 = 7; // Added support for prefix encoding and encryption. Before this change only exact matches within a key field were deduped
                                     // for consecutive keys. After this change, if consecutive key fields have the same prefix then the prefix is only stored
                                     // once.
  static final int RINDEX_VER_6 = 6; // Added support for multilevel indexes. Before this the index was one list with an entry for each data block. For large
                                     // files, a large index needed to be read into memory before any seek could be done. After this change the index is a fat
                                     // tree, and opening a large rfile is much faster. Like the previous version of Rfile, each index node in the tree is kept
                                     // in memory serialized and used in its serialized form.
  // static final int RINDEX_VER_5 = 5; // unreleased
  static final int RINDEX_VER_4 = 4; // Added support for seeking using serialized indexes. After this change index is no longer deserialized when rfile opened.
                                     // Entire serialized index is read into memory as single byte array. For seeks, serialized index is used to find blocks
                                     // (the binary search deserializes the specific entries its needs). This resulted in less memory usage (no object overhead)
                                     // and faster open times for RFiles.
  static final int RINDEX_VER_3 = 3; // Initial released version of RFile. R is for relative encoding. A keys is encoded relative to the previous key. The
                                     // initial version deduped key fields that were the same for consecutive keys. For sorted data this is a common occurrence.
                                     // This version supports locality groups. Each locality group has an index pointing to set of data blocks. Each data block
                                     // contains relatively encoded keys and values.

  // Buffer sample data so that many sample data blocks are stored contiguously.
  private static int sampleBufferSize = 10000000;

  @VisibleForTesting
  public static void setSampleBufferSize(int bufferSize) {
    sampleBufferSize = bufferSize;
  }

  private static class LocalityGroupMetadata implements Writable {

    private int startBlock = -1;
    private Key firstKey;
    private Map<ByteSequence,MutableLong> columnFamilies;

    private boolean isDefaultLG = false;
    private String name;
    private Set<ByteSequence> previousColumnFamilies;

    private MultiLevelIndex.BufferedWriter indexWriter;
    private MultiLevelIndex.Reader indexReader;
    private int version;

    public LocalityGroupMetadata(int version, BlockFileReader br) {
      columnFamilies = new HashMap<>();
      indexReader = new MultiLevelIndex.Reader(br, version);
      this.version = version;
    }

    public LocalityGroupMetadata(Set<ByteSequence> pcf, int indexBlockSize, BlockFileWriter bfw) {
      isDefaultLG = true;
      columnFamilies = new HashMap<>();
      previousColumnFamilies = pcf;

      indexWriter = new MultiLevelIndex.BufferedWriter(new MultiLevelIndex.Writer(bfw, indexBlockSize));
    }

    public LocalityGroupMetadata(String name, Set<ByteSequence> cfset, int indexBlockSize, BlockFileWriter bfw) {
      this.name = name;
      isDefaultLG = false;
      columnFamilies = new HashMap<>();
      for (ByteSequence cf : cfset) {
        columnFamilies.put(cf, new MutableLong(0));
      }

      indexWriter = new MultiLevelIndex.BufferedWriter(new MultiLevelIndex.Writer(bfw, indexBlockSize));
    }

    private Key getFirstKey() {
      return firstKey;
    }

    private void setFirstKey(Key key) {
      if (firstKey != null)
        throw new IllegalStateException();
      this.firstKey = new Key(key);
    }

    public void updateColumnCount(Key key) {

      if (isDefaultLG && columnFamilies == null) {
        if (previousColumnFamilies.size() > 0) {
          // only do this check when there are previous column families
          ByteSequence cf = key.getColumnFamilyData();
          if (previousColumnFamilies.contains(cf)) {
            throw new IllegalArgumentException("Added column family \"" + cf + "\" to default locality group that was in previous locality group");
          }
        }

        // no longer keeping track of column families, so return
        return;
      }

      ByteSequence cf = key.getColumnFamilyData();
      MutableLong count = columnFamilies.get(cf);

      if (count == null) {
        if (!isDefaultLG) {
          throw new IllegalArgumentException("invalid column family : " + cf);
        }

        if (previousColumnFamilies.contains(cf)) {
          throw new IllegalArgumentException("Added column family \"" + cf + "\" to default locality group that was in previous locality group");
        }

        if (columnFamilies.size() > Writer.MAX_CF_IN_DLG) {
          // stop keeping track, there are too many
          columnFamilies = null;
          return;
        }
        count = new MutableLong(0);
        columnFamilies.put(new ArrayByteSequence(cf.getBackingArray(), cf.offset(), cf.length()), count);

      }

      count.increment();

    }

    @Override
    public void readFields(DataInput in) throws IOException {

      isDefaultLG = in.readBoolean();
      if (!isDefaultLG) {
        name = in.readUTF();
      }

      if (version == RINDEX_VER_3 || version == RINDEX_VER_4 || version == RINDEX_VER_6 || version == RINDEX_VER_7) {
        startBlock = in.readInt();
      }

      int size = in.readInt();

      if (size == -1) {
        if (!isDefaultLG)
          throw new IllegalStateException("Non default LG " + name + " does not have column families");

        columnFamilies = null;
      } else {
        if (columnFamilies == null)
          columnFamilies = new HashMap<>();
        else
          columnFamilies.clear();

        for (int i = 0; i < size; i++) {
          int len = in.readInt();
          byte cf[] = new byte[len];
          in.readFully(cf);
          long count = in.readLong();

          columnFamilies.put(new ArrayByteSequence(cf), new MutableLong(count));
        }
      }

      if (in.readBoolean()) {
        firstKey = new Key();
        firstKey.readFields(in);
      } else {
        firstKey = null;
      }

      indexReader.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {

      out.writeBoolean(isDefaultLG);
      if (!isDefaultLG) {
        out.writeUTF(name);
      }

      if (isDefaultLG && columnFamilies == null) {
        // only expect null when default LG, otherwise let a NPE occur
        out.writeInt(-1);
      } else {
        out.writeInt(columnFamilies.size());

        for (Entry<ByteSequence,MutableLong> entry : columnFamilies.entrySet()) {
          out.writeInt(entry.getKey().length());
          out.write(entry.getKey().getBackingArray(), entry.getKey().offset(), entry.getKey().length());
          out.writeLong(entry.getValue().longValue());
        }
      }

      out.writeBoolean(firstKey != null);
      if (firstKey != null)
        firstKey.write(out);

      indexWriter.close(out);
    }

    public void printInfo(boolean isSample, boolean includeIndexDetails) throws IOException {
      PrintStream out = System.out;
      out.printf("%-24s : %s\n", (isSample ? "Sample " : "") + "Locality group ", (isDefaultLG ? "<DEFAULT>" : name));
      if (version == RINDEX_VER_3 || version == RINDEX_VER_4 || version == RINDEX_VER_6 || version == RINDEX_VER_7) {
        out.printf("\t%-22s : %d\n", "Start block", startBlock);
      }
      out.printf("\t%-22s : %,d\n", "Num   blocks", indexReader.size());
      TreeMap<Integer,Long> sizesByLevel = new TreeMap<>();
      TreeMap<Integer,Long> countsByLevel = new TreeMap<>();
      indexReader.getIndexInfo(sizesByLevel, countsByLevel);
      for (Entry<Integer,Long> entry : sizesByLevel.descendingMap().entrySet()) {
        out.printf("\t%-22s : %,d bytes  %,d blocks\n", "Index level " + entry.getKey(), entry.getValue(), countsByLevel.get(entry.getKey()));
      }
      out.printf("\t%-22s : %s\n", "First key", firstKey);

      Key lastKey = null;
      if (indexReader.size() > 0) {
        lastKey = indexReader.getLastKey();
      }

      out.printf("\t%-22s : %s\n", "Last key", lastKey);

      long numKeys = 0;
      IndexIterator countIter = indexReader.lookup(new Key());
      while (countIter.hasNext()) {
        IndexEntry indexEntry = countIter.next();
        numKeys += indexEntry.getNumEntries();
      }

      out.printf("\t%-22s : %,d\n", "Num entries", numKeys);
      out.printf("\t%-22s : %s\n", "Column families", (isDefaultLG && columnFamilies == null ? "<UNKNOWN>" : columnFamilies.keySet()));

      if (includeIndexDetails) {
        out.printf("\t%-22s :\nIndex Entries", lastKey);
        String prefix = String.format("\t   ");
        indexReader.printIndex(prefix, out);
      }
    }

  }

  private static class SampleEntry {
    Key key;
    Value val;

    SampleEntry(Key key, Value val) {
      this.key = new Key(key);
      this.val = new Value(val);
    }
  }

  private static class SampleLocalityGroupWriter {

    private Sampler sampler;

    private List<SampleEntry> entries = new ArrayList<>();
    private long dataSize = 0;

    private LocalityGroupWriter lgr;

    public SampleLocalityGroupWriter(LocalityGroupWriter lgr, Sampler sampler) {
      this.lgr = lgr;
      this.sampler = sampler;
    }

    public void append(Key key, Value value) throws IOException {
      if (sampler.accept(key)) {
        entries.add(new SampleEntry(key, value));
        dataSize += key.getSize() + value.getSize();
      }
    }

    public void close() throws IOException {
      for (SampleEntry se : entries) {
        lgr.append(se.key, se.val);
      }

      lgr.close();
    }

    public void flushIfNeeded() throws IOException {
      if (dataSize > sampleBufferSize) {
        // the reason to write out all but one key is so that closeBlock() can always eventually be called with true
        List<SampleEntry> subList = entries.subList(0, entries.size() - 1);

        if (subList.size() > 0) {
          for (SampleEntry se : subList) {
            lgr.append(se.key, se.val);
          }

          lgr.closeBlock(subList.get(subList.size() - 1).key, false);

          subList.clear();
          dataSize = 0;
        }
      }
    }
  }

  private static class LocalityGroupWriter {

    private BlockFileWriter fileWriter;
    private ABlockWriter blockWriter;

    // private BlockAppender blockAppender;
    private final long blockSize;
    private final long maxBlockSize;
    private int entries = 0;

    private LocalityGroupMetadata currentLocalityGroup = null;

    private Key lastKeyInBlock = null;

    private Key prevKey = new Key();

    private SampleLocalityGroupWriter sample;

    // Use windowed stats to fix ACCUMULO-4669
    private RollingStats keyLenStats = new RollingStats(2017);
    private double averageKeySize = 0;

    LocalityGroupWriter(BlockFileWriter fileWriter, long blockSize, long maxBlockSize, LocalityGroupMetadata currentLocalityGroup,
        SampleLocalityGroupWriter sample) {
      this.fileWriter = fileWriter;
      this.blockSize = blockSize;
      this.maxBlockSize = maxBlockSize;
      this.currentLocalityGroup = currentLocalityGroup;
      this.sample = sample;
    }

    private boolean isGiantKey(Key k) {
      double mean = keyLenStats.getMean();
      double stddev = keyLenStats.getStandardDeviation();
      return k.getSize() > mean + Math.max(9 * mean, 4 * stddev);
    }

    public void append(Key key, Value value) throws IOException {

      if (key.compareTo(prevKey) < 0) {
        throw new IllegalArgumentException("Keys appended out-of-order.  New key " + key + ", previous key " + prevKey);
      }

      currentLocalityGroup.updateColumnCount(key);

      if (currentLocalityGroup.getFirstKey() == null) {
        currentLocalityGroup.setFirstKey(key);
      }

      if (sample != null) {
        sample.append(key, value);
      }

      if (blockWriter == null) {
        blockWriter = fileWriter.prepareDataBlock();
      } else if (blockWriter.getRawSize() > blockSize) {

        // Look for a key thats short to put in the index, defining short as average or below.
        if (averageKeySize == 0) {
          // use the same average for the search for a below average key for a block
          averageKeySize = keyLenStats.getMean();
        }

        // Possibly produce a shorter key that does not exist in data. Even if a key can be shortened, it may not be below average.
        Key closeKey = KeyShortener.shorten(prevKey, key);

        if ((closeKey.getSize() <= averageKeySize || blockWriter.getRawSize() > maxBlockSize) && !isGiantKey(closeKey)) {
          closeBlock(closeKey, false);
          blockWriter = fileWriter.prepareDataBlock();
          // set average to zero so its recomputed for the next block
          averageKeySize = 0;
          // To constrain the growth of data blocks, we limit our worst case scenarios to closing
          // blocks if they reach the maximum configurable block size of Integer.MAX_VALUE.
          // 128 bytes added for metadata overhead
        } else if (((long) key.getSize() + (long) value.getSize() + blockWriter.getRawSize() + 128L) >= Integer.MAX_VALUE) {
          closeBlock(closeKey, false);
          blockWriter = fileWriter.prepareDataBlock();
          averageKeySize = 0;

        }
      }

      RelativeKey rk = new RelativeKey(lastKeyInBlock, key);

      rk.write(blockWriter);
      value.write(blockWriter);
      entries++;

      keyLenStats.addValue(key.getSize());

      prevKey = new Key(key);
      lastKeyInBlock = prevKey;

    }

    private void closeBlock(Key key, boolean lastBlock) throws IOException {
      blockWriter.close();

      if (lastBlock)
        currentLocalityGroup.indexWriter.addLast(key, entries, blockWriter.getStartPos(), blockWriter.getCompressedSize(), blockWriter.getRawSize());
      else
        currentLocalityGroup.indexWriter.add(key, entries, blockWriter.getStartPos(), blockWriter.getCompressedSize(), blockWriter.getRawSize());

      if (sample != null)
        sample.flushIfNeeded();

      blockWriter = null;
      lastKeyInBlock = null;
      entries = 0;
    }

    public void close() throws IOException {
      if (blockWriter != null) {
        closeBlock(lastKeyInBlock, true);
      }

      if (sample != null) {
        sample.close();
      }
    }
  }

  public static class Writer implements FileSKVWriter {

    public static final int MAX_CF_IN_DLG = 1000;
    private static final double MAX_BLOCK_MULTIPLIER = 1.1;

    private BlockFileWriter fileWriter;

    // private BlockAppender blockAppender;
    private final long blockSize;
    private final long maxBlockSize;
    private final int indexBlockSize;

    private ArrayList<LocalityGroupMetadata> localityGroups = new ArrayList<>();
    private ArrayList<LocalityGroupMetadata> sampleGroups = new ArrayList<>();
    private LocalityGroupMetadata currentLocalityGroup = null;
    private LocalityGroupMetadata sampleLocalityGroup = null;

    private boolean dataClosed = false;
    private boolean closed = false;
    private boolean startedDefaultLocalityGroup = false;

    private HashSet<ByteSequence> previousColumnFamilies;
    private long length = -1;

    private LocalityGroupWriter lgWriter;

    private SamplerConfigurationImpl samplerConfig;
    private Sampler sampler;

    public Writer(BlockFileWriter bfw, int blockSize) throws IOException {
      this(bfw, blockSize, (int) DefaultConfiguration.getInstance().getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX), null, null);
    }

    public Writer(BlockFileWriter bfw, int blockSize, int indexBlockSize, SamplerConfigurationImpl samplerConfig, Sampler sampler) throws IOException {
      this.blockSize = blockSize;
      this.maxBlockSize = (long) (blockSize * MAX_BLOCK_MULTIPLIER);
      this.indexBlockSize = indexBlockSize;
      this.fileWriter = bfw;
      previousColumnFamilies = new HashSet<>();
      this.samplerConfig = samplerConfig;
      this.sampler = sampler;
    }

    @Override
    public synchronized void close() throws IOException {

      if (closed) {
        return;
      }

      closeData();

      ABlockWriter mba = fileWriter.prepareMetaBlock("RFile.index");

      mba.writeInt(RINDEX_MAGIC);
      mba.writeInt(RINDEX_VER_8);

      if (currentLocalityGroup != null) {
        localityGroups.add(currentLocalityGroup);
        sampleGroups.add(sampleLocalityGroup);
      }

      mba.writeInt(localityGroups.size());

      for (LocalityGroupMetadata lc : localityGroups) {
        lc.write(mba);
      }

      if (samplerConfig == null) {
        mba.writeBoolean(false);
      } else {
        mba.writeBoolean(true);

        for (LocalityGroupMetadata lc : sampleGroups) {
          lc.write(mba);
        }

        samplerConfig.write(mba);
      }

      mba.close();
      fileWriter.close();
      length = fileWriter.getLength();

      closed = true;
    }

    private void closeData() throws IOException {

      if (dataClosed) {
        return;
      }

      dataClosed = true;

      if (lgWriter != null) {
        lgWriter.close();
      }
    }

    @Override
    public void append(Key key, Value value) throws IOException {

      if (dataClosed) {
        throw new IllegalStateException("Cannont append, data closed");
      }

      lgWriter.append(key, value);
    }

    @Override
    public DataOutputStream createMetaStore(String name) throws IOException {
      closeData();

      return (DataOutputStream) fileWriter.prepareMetaBlock(name);
    }

    private void _startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies) throws IOException {
      if (dataClosed) {
        throw new IllegalStateException("data closed");
      }

      if (startedDefaultLocalityGroup) {
        throw new IllegalStateException("Can not start anymore new locality groups after default locality group started");
      }

      if (lgWriter != null) {
        lgWriter.close();
      }

      if (currentLocalityGroup != null) {
        localityGroups.add(currentLocalityGroup);
        sampleGroups.add(sampleLocalityGroup);
      }

      if (columnFamilies == null) {
        startedDefaultLocalityGroup = true;
        currentLocalityGroup = new LocalityGroupMetadata(previousColumnFamilies, indexBlockSize, fileWriter);
        sampleLocalityGroup = new LocalityGroupMetadata(previousColumnFamilies, indexBlockSize, fileWriter);
      } else {
        if (!Collections.disjoint(columnFamilies, previousColumnFamilies)) {
          HashSet<ByteSequence> overlap = new HashSet<>(columnFamilies);
          overlap.retainAll(previousColumnFamilies);
          throw new IllegalArgumentException("Column families over lap with previous locality group : " + overlap);
        }
        currentLocalityGroup = new LocalityGroupMetadata(name, columnFamilies, indexBlockSize, fileWriter);
        sampleLocalityGroup = new LocalityGroupMetadata(name, columnFamilies, indexBlockSize, fileWriter);
        previousColumnFamilies.addAll(columnFamilies);
      }

      SampleLocalityGroupWriter sampleWriter = null;
      if (sampler != null) {
        sampleWriter = new SampleLocalityGroupWriter(new LocalityGroupWriter(fileWriter, blockSize, maxBlockSize, sampleLocalityGroup, null), sampler);
      }
      lgWriter = new LocalityGroupWriter(fileWriter, blockSize, maxBlockSize, currentLocalityGroup, sampleWriter);
    }

    @Override
    public void startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies) throws IOException {
      if (columnFamilies == null)
        throw new NullPointerException();

      _startNewLocalityGroup(name, columnFamilies);
    }

    @Override
    public void startDefaultLocalityGroup() throws IOException {
      _startNewLocalityGroup(null, null);
    }

    @Override
    public boolean supportsLocalityGroups() {
      return true;
    }

    @Override
    public long getLength() throws IOException {
      if (!closed) {
        return fileWriter.getLength();
      }
      return length;
    }
  }

  private static class LocalityGroupReader extends LocalityGroup implements FileSKVIterator {

    private BlockFileReader reader;
    private MultiLevelIndex.Reader index;
    private int blockCount;
    private Key firstKey;
    private int startBlock;
    private boolean closed = false;
    private int version;
    private boolean checkRange = true;

    private LocalityGroupReader(BlockFileReader reader, LocalityGroupMetadata lgm, int version) throws IOException {
      super(lgm.columnFamilies, lgm.isDefaultLG);
      this.firstKey = lgm.firstKey;
      this.index = lgm.indexReader;
      this.startBlock = lgm.startBlock;
      blockCount = index.size();
      this.version = version;

      this.reader = reader;

    }

    public LocalityGroupReader(LocalityGroupReader lgr) {
      super(lgr.columnFamilies, lgr.isDefaultLocalityGroup);
      this.firstKey = lgr.firstKey;
      this.index = lgr.index;
      this.startBlock = lgr.startBlock;
      this.blockCount = lgr.blockCount;
      this.reader = lgr.reader;
      this.version = lgr.version;
    }

    Iterator<IndexEntry> getIndex() throws IOException {
      return index.lookup(new Key());
    }

    @Override
    public void close() throws IOException {
      closed = true;
      hasTop = false;
      if (currBlock != null)
        currBlock.close();

    }

    private IndexIterator iiter;
    private int entriesLeft;
    private ABlockReader currBlock;
    private RelativeKey rk;
    private Value val;
    private Key prevKey = null;
    private Range range = null;
    private boolean hasTop = false;
    private AtomicBoolean interruptFlag;

    @Override
    public Key getTopKey() {
      return rk.getKey();
    }

    @Override
    public Value getTopValue() {
      return val;
    }

    @Override
    public boolean hasTop() {
      return hasTop;
    }

    @Override
    public void next() throws IOException {
      try {
        _next();
      } catch (IOException ioe) {
        reset();
        throw ioe;
      }
    }

    private void _next() throws IOException {

      if (!hasTop)
        throw new IllegalStateException();

      if (entriesLeft == 0) {
        currBlock.close();
        if (metricsGatherer != null)
          metricsGatherer.startBlock();

        if (iiter.hasNext()) {
          IndexEntry indexEntry = iiter.next();
          entriesLeft = indexEntry.getNumEntries();
          currBlock = getDataBlock(indexEntry);

          checkRange = range.afterEndKey(indexEntry.getKey());
          if (!checkRange)
            hasTop = true;

        } else {
          rk = null;
          val = null;
          hasTop = false;
          return;
        }
      }

      prevKey = rk.getKey();
      rk.readFields(currBlock);
      val.readFields(currBlock);

      if (metricsGatherer != null)
        metricsGatherer.addMetric(rk.getKey(), val);

      entriesLeft--;
      if (checkRange)
        hasTop = !range.afterEndKey(rk.getKey());
    }

    private ABlockReader getDataBlock(IndexEntry indexEntry) throws IOException {
      if (interruptFlag != null && interruptFlag.get())
        throw new IterationInterruptedException();

      if (version == RINDEX_VER_3 || version == RINDEX_VER_4)
        return reader.getDataBlock(startBlock + iiter.previousIndex());
      else
        return reader.getDataBlock(indexEntry.getOffset(), indexEntry.getCompressedSize(), indexEntry.getRawSize());

    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

      if (closed)
        throw new IllegalStateException("Locality group reader closed");

      if (columnFamilies.size() != 0 || inclusive)
        throw new IllegalArgumentException("I do not know how to filter column families");

      if (interruptFlag != null && interruptFlag.get()) {
        throw new IterationInterruptedException();
      }

      try {
        _seek(range);
      } catch (IOException ioe) {
        reset();
        throw ioe;
      }
    }

    private void reset() {
      rk = null;
      hasTop = false;
      if (currBlock != null) {
        try {
          try {
            currBlock.close();
          } catch (IOException e) {
            log.warn("Failed to close block reader", e);
          }
        } finally {
          currBlock = null;
        }
      }
    }

    private void _seek(Range range) throws IOException {

      this.range = range;
      this.checkRange = true;

      if (blockCount == 0) {
        // its an empty file
        rk = null;
        return;
      }

      Key startKey = range.getStartKey();
      if (startKey == null)
        startKey = new Key();

      boolean reseek = true;

      if (range.afterEndKey(firstKey)) {
        // range is before first key in rfile, so there is nothing to do
        reset();
        reseek = false;
      }

      if (rk != null) {
        if (range.beforeStartKey(prevKey) && range.afterEndKey(getTopKey())) {
          // range is between the two keys in the file where the last range seeked to stopped, so there is
          // nothing to do
          reseek = false;
        }

        if (startKey.compareTo(getTopKey()) <= 0 && startKey.compareTo(prevKey) > 0) {
          // current location in file can satisfy this request, no need to seek
          reseek = false;
        }

        if (entriesLeft > 0 && startKey.compareTo(getTopKey()) >= 0 && startKey.compareTo(iiter.peekPrevious().getKey()) <= 0) {
          // start key is within the unconsumed portion of the current block

          // this code intentionally does not use the index associated with a cached block
          // because if only forward seeks are being done, then there is no benefit to building
          // and index for the block... could consider using the index if it exist but not
          // causing the build of an index... doing this could slow down some use cases and
          // and speed up others.

          MutableByteSequence valbs = new MutableByteSequence(new byte[64], 0, 0);
          SkippR skippr = RelativeKey.fastSkip(currBlock, startKey, valbs, prevKey, getTopKey(), entriesLeft);
          if (skippr.skipped > 0) {
            entriesLeft -= skippr.skipped;
            val = new Value(valbs.toArray());
            prevKey = skippr.prevKey;
            rk = skippr.rk;
          }

          reseek = false;
        }

        if (entriesLeft == 0 && startKey.compareTo(getTopKey()) > 0 && startKey.compareTo(iiter.peekPrevious().getKey()) <= 0) {
          // In the empty space at the end of a block. This can occur when keys are shortened in the index creating index entries that do not exist in the
          // block. These shortened index entires fall between the last key in a block and first key in the next block, but may not exist in the data.
          // Just proceed to the next block.
          reseek = false;
        }

        if (iiter.previousIndex() == 0 && getTopKey().equals(firstKey) && startKey.compareTo(firstKey) <= 0) {
          // seeking before the beginning of the file, and already positioned at the first key in the file
          // so there is nothing to do
          reseek = false;
        }
      }

      if (reseek) {
        iiter = index.lookup(startKey);

        reset();

        if (!iiter.hasNext()) {
          // past the last key
        } else {

          // if the index contains the same key multiple times, then go to the
          // earliest index entry containing the key
          while (iiter.hasPrevious() && iiter.peekPrevious().getKey().equals(iiter.peek().getKey())) {
            iiter.previous();
          }

          if (iiter.hasPrevious())
            prevKey = new Key(iiter.peekPrevious().getKey()); // initially prevKey is the last key of the prev block
          else
            prevKey = new Key(); // first block in the file, so set prev key to minimal key

          IndexEntry indexEntry = iiter.next();
          entriesLeft = indexEntry.getNumEntries();
          currBlock = getDataBlock(indexEntry);

          checkRange = range.afterEndKey(indexEntry.getKey());
          if (!checkRange)
            hasTop = true;

          MutableByteSequence valbs = new MutableByteSequence(new byte[64], 0, 0);

          Key currKey = null;

          if (currBlock.isIndexable()) {
            BlockIndex blockIndex = BlockIndex.getIndex(currBlock, indexEntry);
            if (blockIndex != null) {
              BlockIndexEntry bie = blockIndex.seekBlock(startKey, currBlock);
              if (bie != null) {
                // we are seeked to the current position of the key in the index
                // need to prime the read process and read this key from the block
                RelativeKey tmpRk = new RelativeKey();
                tmpRk.setPrevKey(bie.getPrevKey());
                tmpRk.readFields(currBlock);
                val = new Value();

                val.readFields(currBlock);
                valbs = new MutableByteSequence(val.get(), 0, val.getSize());

                // just consumed one key from the input stream, so subtract one from entries left
                entriesLeft = bie.getEntriesLeft() - 1;
                prevKey = new Key(bie.getPrevKey());
                currKey = tmpRk.getKey();
              }
            }
          }

          SkippR skippr = RelativeKey.fastSkip(currBlock, startKey, valbs, prevKey, currKey, entriesLeft);
          prevKey = skippr.prevKey;
          entriesLeft -= skippr.skipped;
          val = new Value(valbs.toArray());
          // set rk when everything above is successful, if exception
          // occurs rk will not be set
          rk = skippr.rk;
        }
      }

      hasTop = rk != null && !range.afterEndKey(rk.getKey());

      while (hasTop() && range.beforeStartKey(getTopKey())) {
        next();
      }

      if (metricsGatherer != null) {
        metricsGatherer.startLocalityGroup(rk.getKey().getColumnFamily());
        metricsGatherer.addMetric(rk.getKey(), val);
      }
    }

    @Override
    public Key getFirstKey() throws IOException {
      return firstKey;
    }

    @Override
    public Key getLastKey() throws IOException {
      if (index.size() == 0)
        return null;
      return index.getLastKey();
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void closeDeepCopies() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public DataInputStream getMetaStore(String name) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      this.interruptFlag = flag;
    }

    @Override
    public InterruptibleIterator getIterator() {
      return this;
    }

    private MetricsGatherer<?> metricsGatherer;

    public void registerMetrics(MetricsGatherer<?> vmg) {
      metricsGatherer = vmg;
    }

    @Override
    public FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig) {
      throw new UnsupportedOperationException();
    }
  }

  public static class Reader extends HeapIterator implements FileSKVIterator {

    private final BlockFileReader reader;

    private final ArrayList<LocalityGroupMetadata> localityGroups = new ArrayList<>();
    private final ArrayList<LocalityGroupMetadata> sampleGroups = new ArrayList<>();

    private final LocalityGroupReader currentReaders[];
    private final LocalityGroupReader readers[];
    private final LocalityGroupReader sampleReaders[];
    private final LocalityGroupContext lgContext;
    private LocalityGroupSeekCache lgCache;

    private List<Reader> deepCopies;
    private boolean deepCopy = false;

    private AtomicBoolean interruptFlag;

    private SamplerConfigurationImpl samplerConfig = null;

    private int rfileVersion;

    public Reader(BlockFileReader rdr) throws IOException {
      this.reader = rdr;

      ABlockReader mb = reader.getMetaBlock("RFile.index");
      try {
        int magic = mb.readInt();
        int ver = mb.readInt();
        rfileVersion = ver;

        if (magic != RINDEX_MAGIC)
          throw new IOException("Did not see expected magic number, saw " + magic);
        if (ver != RINDEX_VER_8 && ver != RINDEX_VER_7 && ver != RINDEX_VER_6 && ver != RINDEX_VER_4 && ver != RINDEX_VER_3)
          throw new IOException("Did not see expected version, saw " + ver);

        int size = mb.readInt();
        currentReaders = new LocalityGroupReader[size];

        deepCopies = new LinkedList<>();

        for (int i = 0; i < size; i++) {
          LocalityGroupMetadata lgm = new LocalityGroupMetadata(ver, rdr);
          lgm.readFields(mb);
          localityGroups.add(lgm);

          currentReaders[i] = new LocalityGroupReader(reader, lgm, ver);
        }

        readers = currentReaders;

        if (ver == RINDEX_VER_8 && mb.readBoolean()) {
          sampleReaders = new LocalityGroupReader[size];

          for (int i = 0; i < size; i++) {
            LocalityGroupMetadata lgm = new LocalityGroupMetadata(ver, rdr);
            lgm.readFields(mb);
            sampleGroups.add(lgm);

            sampleReaders[i] = new LocalityGroupReader(reader, lgm, ver);
          }

          samplerConfig = new SamplerConfigurationImpl(mb);
        } else {
          sampleReaders = null;
          samplerConfig = null;
        }

      } finally {
        mb.close();
      }

      lgContext = new LocalityGroupContext(currentReaders);

      createHeap(currentReaders.length);
    }

    private Reader(Reader r, LocalityGroupReader sampleReaders[]) {
      super(sampleReaders.length);
      this.reader = r.reader;
      this.currentReaders = new LocalityGroupReader[sampleReaders.length];
      this.deepCopies = r.deepCopies;
      this.deepCopy = false;
      this.readers = r.readers;
      this.sampleReaders = r.sampleReaders;
      this.samplerConfig = r.samplerConfig;
      this.rfileVersion = r.rfileVersion;
      for (int i = 0; i < sampleReaders.length; i++) {
        this.currentReaders[i] = sampleReaders[i];
        this.currentReaders[i].setInterruptFlag(r.interruptFlag);
      }
      this.lgContext = new LocalityGroupContext(currentReaders);
    }

    private Reader(Reader r, boolean useSample) {
      super(r.currentReaders.length);
      this.reader = r.reader;
      this.currentReaders = new LocalityGroupReader[r.currentReaders.length];
      this.deepCopies = r.deepCopies;
      this.deepCopy = true;
      this.samplerConfig = r.samplerConfig;
      this.rfileVersion = r.rfileVersion;
      this.readers = r.readers;
      this.sampleReaders = r.sampleReaders;

      for (int i = 0; i < r.readers.length; i++) {
        if (useSample) {
          this.currentReaders[i] = new LocalityGroupReader(r.sampleReaders[i]);
          this.currentReaders[i].setInterruptFlag(r.interruptFlag);
        } else {
          this.currentReaders[i] = new LocalityGroupReader(r.readers[i]);
          this.currentReaders[i].setInterruptFlag(r.interruptFlag);
        }

      }
      this.lgContext = new LocalityGroupContext(currentReaders);
    }

    private void closeLocalityGroupReaders() {
      for (LocalityGroupReader lgr : currentReaders) {
        try {
          lgr.close();
        } catch (IOException e) {
          log.warn("Errored out attempting to close LocalityGroupReader.", e);
        }
      }
    }

    @Override
    public void closeDeepCopies() {
      if (deepCopy)
        throw new RuntimeException("Calling closeDeepCopies on a deep copy is not supported");

      for (Reader deepCopy : deepCopies)
        deepCopy.closeLocalityGroupReaders();

      deepCopies.clear();
    }

    @Override
    public void close() throws IOException {
      if (deepCopy)
        throw new RuntimeException("Calling close on a deep copy is not supported");

      closeDeepCopies();
      closeLocalityGroupReaders();

      if (sampleReaders != null) {
        for (LocalityGroupReader lgr : sampleReaders) {
          try {
            lgr.close();
          } catch (IOException e) {
            log.warn("Errored out attempting to close LocalityGroupReader.", e);
          }
        }
      }

      try {
        reader.close();
      } finally {
        /**
         * input Stream is passed to CachableBlockFile and closed there
         */
      }
    }

    @Override
    public Key getFirstKey() throws IOException {
      if (currentReaders.length == 0) {
        return null;
      }

      Key minKey = null;

      for (int i = 0; i < currentReaders.length; i++) {
        if (minKey == null) {
          minKey = currentReaders[i].getFirstKey();
        } else {
          Key firstKey = currentReaders[i].getFirstKey();
          if (firstKey != null && firstKey.compareTo(minKey) < 0)
            minKey = firstKey;
        }
      }

      return minKey;
    }

    @Override
    public Key getLastKey() throws IOException {
      if (currentReaders.length == 0) {
        return null;
      }

      Key maxKey = null;

      for (int i = 0; i < currentReaders.length; i++) {
        if (maxKey == null) {
          maxKey = currentReaders[i].getLastKey();
        } else {
          Key lastKey = currentReaders[i].getLastKey();
          if (lastKey != null && lastKey.compareTo(maxKey) > 0)
            maxKey = lastKey;
        }
      }

      return maxKey;
    }

    @Override
    public DataInputStream getMetaStore(String name) throws IOException, NoSuchMetaStoreException {
      try {
        return this.reader.getMetaBlock(name).getStream();
      } catch (MetaBlockDoesNotExist e) {
        throw new NoSuchMetaStoreException("name = " + name, e);
      }
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      if (env != null && env.isSamplingEnabled()) {
        SamplerConfiguration sc = env.getSamplerConfiguration();
        if (sc == null) {
          throw new SampleNotPresentException();
        }

        if (this.samplerConfig != null && this.samplerConfig.equals(new SamplerConfigurationImpl(sc))) {
          Reader copy = new Reader(this, true);
          copy.setInterruptFlagInternal(interruptFlag);
          deepCopies.add(copy);
          return copy;
        } else {
          throw new SampleNotPresentException();
        }
      } else {
        Reader copy = new Reader(this, false);
        copy.setInterruptFlagInternal(interruptFlag);
        deepCopies.add(copy);
        return copy;
      }
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      throw new UnsupportedOperationException();
    }

    /**
     * @return map of locality group names to column families. The default locality group will have {@code null} for a name. RFile will only track up to
     *         {@value Writer#MAX_CF_IN_DLG} families for the default locality group. After this it will stop tracking. For the case where the default group has
     *         more thn {@value Writer#MAX_CF_IN_DLG} families an empty list of families is returned.
     * @see LocalityGroupUtil#seek(FileSKVIterator, Range, String, Map)
     */
    public Map<String,ArrayList<ByteSequence>> getLocalityGroupCF() {
      Map<String,ArrayList<ByteSequence>> cf = new HashMap<>();

      for (LocalityGroupMetadata lcg : localityGroups) {
        ArrayList<ByteSequence> setCF;

        if (lcg.columnFamilies == null) {
          Preconditions.checkState(lcg.isDefaultLG, " Group %s has null families. Only expect default locality group to have null families.", lcg.name);
          setCF = new ArrayList<>();
        } else {
          setCF = new ArrayList<>(lcg.columnFamilies.keySet());
        }

        cf.put(lcg.name, setCF);
      }

      return cf;
    }

    /**
     * Method that registers the given MetricsGatherer. You can only register one as it will clobber any previously set. The MetricsGatherer should be
     * registered before iterating through the LocalityGroups.
     *
     * @param vmg
     *          MetricsGatherer to be registered with the LocalityGroupReaders
     */
    public void registerMetrics(MetricsGatherer<?> vmg) {
      vmg.init(getLocalityGroupCF());
      for (LocalityGroupReader lgr : currentReaders) {
        lgr.registerMetrics(vmg);
      }

      if (sampleReaders != null) {
        for (LocalityGroupReader lgr : sampleReaders) {
          lgr.registerMetrics(vmg);
        }
      }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
      lgCache = LocalityGroupIterator.seek(this, lgContext, range, columnFamilies, inclusive, lgCache);
    }

    int getNumLocalityGroupsSeeked() {
      return (lgCache == null ? 0 : lgCache.getNumLGSeeked());
    }

    public FileSKVIterator getIndex() throws IOException {

      ArrayList<Iterator<IndexEntry>> indexes = new ArrayList<>();

      for (LocalityGroupReader lgr : currentReaders) {
        indexes.add(lgr.getIndex());
      }

      return new MultiIndexIterator(this, indexes);
    }

    @Override
    public FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig) {
      requireNonNull(sampleConfig);

      if (this.samplerConfig != null && this.samplerConfig.equals(sampleConfig)) {
        Reader copy = new Reader(this, sampleReaders);
        copy.setInterruptFlagInternal(interruptFlag);
        return copy;
      }

      return null;
    }

    // only visible for printinfo
    FileSKVIterator getSample() {
      if (samplerConfig == null)
        return null;
      return getSample(this.samplerConfig);
    }

    public void printInfo() throws IOException {
      printInfo(false);
    }

    public void printInfo(boolean includeIndexDetails) throws IOException {

      System.out.printf("%-24s : %d\n", "RFile Version", rfileVersion);
      System.out.println();

      for (LocalityGroupMetadata lgm : localityGroups) {
        lgm.printInfo(false, includeIndexDetails);
      }

      if (sampleGroups.size() > 0) {

        System.out.println();
        System.out.printf("%-24s :\n", "Sample Configuration");
        System.out.printf("\t%-22s : %s\n", "Sampler class ", samplerConfig.getClassName());
        System.out.printf("\t%-22s : %s\n", "Sampler options ", samplerConfig.getOptions());
        System.out.println();

        for (LocalityGroupMetadata lgm : sampleGroups) {
          lgm.printInfo(true, includeIndexDetails);
        }
      }
    }

    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      if (deepCopy)
        throw new RuntimeException("Calling setInterruptFlag on a deep copy is not supported");

      if (deepCopies.size() != 0)
        throw new RuntimeException("Setting interrupt flag after calling deep copy not supported");

      setInterruptFlagInternal(flag);
    }

    private void setInterruptFlagInternal(AtomicBoolean flag) {
      this.interruptFlag = flag;
      for (LocalityGroupReader lgr : currentReaders) {
        lgr.setInterruptFlag(interruptFlag);
      }
    }
  }
}

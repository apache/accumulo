/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Tests iterator class hierarchy bug. See https://github.com/apache/accumulo/issues/2341
 */
public class IteratorMincClassCastBugIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // this bug only shows up when not using native maps
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, "false");
    cfg.setNumTservers(1);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = getUniqueNames(1)[0];

      NewTableConfiguration ntc = new NewTableConfiguration();
      Map<String,Set<Text>> groups = new HashMap<>();
      groups.put("g1", Set.of(new Text("~chunk")));
      groups.put("g2", Set.of(new Text("refs")));
      ntc.setLocalityGroups(groups);

      IteratorSetting iteratorSetting = new IteratorSetting(20, ChunkCombiner.class);
      ntc.attachIterator(iteratorSetting, EnumSet.of(IteratorUtil.IteratorScope.minc));

      c.tableOperations().create(tableName, ntc);

      int chunkSize = 64 * 1024;
      ColumnVisibility visibility = new ColumnVisibility();

      List<URL> files = new ArrayList<>();
      files.add(getClass().getClassLoader().getResource("testfile1.md"));

      try (BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig())) {
        FileDataIngest fdi = new FileDataIngest(chunkSize, visibility);
        for (URL filename : files) {
          fdi.insertFileData(filename, bw);
        }
      }

      c.tableOperations().flush(tableName, null, null, true);
    }
  }

  /**
   * Copied from 2.0 examples. Takes a list of files and archives them into Accumulo keyed on hashes
   * of the files.
   */
  @SuppressFBWarnings(value = "WEAK_MESSAGE_DIGEST_MD5", justification = "For testing only")
  public static class FileDataIngest {
    public static final Text CHUNK_CF = new Text("~chunk");
    public static final Text REFS_CF = new Text("refs");
    public static final String REFS_ORIG_FILE = "name";
    public static final String REFS_FILE_EXT = "filext";
    public static final ByteSequence CHUNK_CF_BS =
        new ArrayByteSequence(CHUNK_CF.getBytes(), 0, CHUNK_CF.getLength());
    public static final ByteSequence REFS_CF_BS =
        new ArrayByteSequence(REFS_CF.getBytes(), 0, REFS_CF.getLength());

    int chunkSize;
    byte[] chunkSizeBytes;
    byte[] buf;
    MessageDigest md5digest;
    ColumnVisibility cv;

    public FileDataIngest(int chunkSize, ColumnVisibility colvis) {
      this.chunkSize = chunkSize;
      chunkSizeBytes = intToBytes(chunkSize);
      buf = new byte[chunkSize];
      try {
        md5digest = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
      cv = colvis;
    }

    public String insertFileData(URL fileURL, BatchWriter bw)
        throws MutationsRejectedException, IOException {
      String filename = fileURL.getFile();
      if (chunkSize == 0)
        return "";
      md5digest.reset();
      String uid = hexString(md5digest.digest(filename.getBytes()));

      // read through file once, calculating hashes
      md5digest.reset();
      InputStream fis = null;
      int numRead = 0;
      try {
        fis = new FileInputStream(filename);
        numRead = fis.read(buf);
        while (numRead >= 0) {
          if (numRead > 0) {
            md5digest.update(buf, 0, numRead);
          }
          numRead = fis.read(buf);
        }
      } finally {
        if (fis != null) {
          fis.close();
        }
      }

      String hash = hexString(md5digest.digest());
      Text row = new Text(hash);

      // write info to accumulo
      Mutation m = new Mutation(row);
      m.put(REFS_CF, buildNullSepText(uid, REFS_ORIG_FILE), cv, new Value(filename.getBytes()));
      String fext = getExt(filename);
      if (fext != null)
        m.put(REFS_CF, buildNullSepText(uid, REFS_FILE_EXT), cv, new Value(fext.getBytes()));
      bw.addMutation(m);

      // read through file again, writing chunks to accumulo
      int chunkCount = 0;
      try {
        fis = new FileInputStream(filename);
        numRead = fis.read(buf);
        while (numRead >= 0) {
          while (numRead < buf.length) {
            int moreRead = fis.read(buf, numRead, buf.length - numRead);
            if (moreRead > 0)
              numRead += moreRead;
            else if (moreRead < 0)
              break;
          }
          m = new Mutation(row);
          Text chunkCQ = new Text(chunkSizeBytes);
          chunkCQ.append(intToBytes(chunkCount), 0, 4);
          m.put(CHUNK_CF, chunkCQ, cv, new Value(buf, 0, numRead));
          bw.addMutation(m);
          if (chunkCount == Integer.MAX_VALUE)
            throw new RuntimeException(
                "too many chunks for file " + filename + ", try raising chunk size");
          chunkCount++;
          numRead = fis.read(buf);
        }
      } finally {
        if (fis != null) {
          fis.close();
        }
      }
      m = new Mutation(row);
      Text chunkCQ = new Text(chunkSizeBytes);
      chunkCQ.append(intToBytes(chunkCount), 0, 4);
      m.put(new Text(CHUNK_CF), chunkCQ, cv, new Value(new byte[0]));
      bw.addMutation(m);
      return hash;
    }

    public static byte[] intToBytes(int l) {
      byte[] b = new byte[4];
      b[0] = (byte) (l >>> 24);
      b[1] = (byte) (l >>> 16);
      b[2] = (byte) (l >>> 8);
      b[3] = (byte) (l >>> 0);
      return b;
    }

    private static String getExt(String filename) {
      if (filename.indexOf(".") == -1)
        return null;
      return filename.substring(filename.lastIndexOf(".") + 1);
    }

    public String hexString(byte[] bytes) {
      StringBuilder sb = new StringBuilder();
      for (byte b : bytes) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    }
  }

  /**
   * This iterator dedupes chunks and sets their visibilities to the combined visibility of the refs
   * columns. For example, it would combine
   *
   * <pre>
   *    row1 refs uid1\0a A&amp;B V0
   *    row1 refs uid2\0b C&amp;D V0
   *    row1 ~chunk 0 A&amp;B V1
   *    row1 ~chunk 0 C&amp;D V1
   *    row1 ~chunk 0 E&amp;F V1
   *    row1 ~chunk 0 G&amp;H V1
   * </pre>
   *
   * into the following
   *
   * <pre>
   *    row1 refs uid1\0a A&amp;B V0
   *    row1 refs uid2\0b C&amp;D V0
   *    row1 ~chunk 0 (A&amp;B)|(C&amp;D) V1
   * </pre>
   *
   * {@link VisibilityCombiner} is used to combie the visibilities.
   */
  public static class ChunkCombiner implements SortedKeyValueIterator<Key,Value> {
    private static final Logger log = LoggerFactory.getLogger(ChunkCombiner.class);

    private SortedKeyValueIterator<Key,Value> source;
    private SortedKeyValueIterator<Key,Value> refsSource;
    private static final Collection<ByteSequence> refsColf =
        Collections.singleton(FileDataIngest.REFS_CF_BS);
    private Map<Text,byte[]> lastRowVC = Collections.emptyMap();

    private Key topKey = null;
    private Value topValue = null;

    public ChunkCombiner() {}

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      this.source = source;
      this.refsSource = source.deepCopy(env);
      log.info("DUDE call init. deepCopy on {}", source.getClass());
    }

    @Override
    public boolean hasTop() {
      return topKey != null;
    }

    @Override
    public void next() throws IOException {
      findTop();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
        throws IOException {
      source.seek(range, columnFamilies, inclusive);
      findTop();
    }

    private void findTop() throws IOException {
      do {
        topKey = null;
        topValue = null;
      } while (source.hasTop() && _findTop() == null);
    }

    private byte[] _findTop() throws IOException {
      long maxTS;

      topKey = new Key(source.getTopKey());
      topValue = new Value(source.getTopValue());
      source.next();

      if (!topKey.getColumnFamilyData().equals(FileDataIngest.CHUNK_CF_BS))
        return topKey.getColumnVisibility().getBytes();

      maxTS = topKey.getTimestamp();

      while (source.hasTop() && source.getTopKey().equals(topKey, PartialKey.ROW_COLFAM_COLQUAL)) {
        if (source.getTopKey().getTimestamp() > maxTS)
          maxTS = source.getTopKey().getTimestamp();

        if (!topValue.equals(source.getTopValue()))
          throw new RuntimeException("values not equals " + topKey + " " + source.getTopKey()
              + " : " + diffInfo(topValue, source.getTopValue()));

        source.next();
      }

      byte[] vis = getVisFromRefs();
      if (vis != null) {
        topKey = new Key(topKey.getRowData().toArray(), topKey.getColumnFamilyData().toArray(),
            topKey.getColumnQualifierData().toArray(), vis, maxTS);
      }
      return vis;
    }

    private byte[] getVisFromRefs() throws IOException {
      Text row = topKey.getRow();
      if (lastRowVC.containsKey(row))
        return lastRowVC.get(row);
      Range range = new Range(row);
      refsSource.seek(range, refsColf, true);
      VisibilityCombiner vc = null;
      while (refsSource.hasTop()) {
        if (vc == null)
          vc = new VisibilityCombiner();
        vc.add(refsSource.getTopKey().getColumnVisibilityData());
        refsSource.next();
      }
      if (vc == null) {
        lastRowVC = Collections.singletonMap(row, null);
        return null;
      }
      lastRowVC = Collections.singletonMap(row, vc.get());
      return vc.get();
    }

    private String diffInfo(Value v1, Value v2) {
      if (v1.getSize() != v2.getSize()) {
        return "val len not equal " + v1.getSize() + "!=" + v2.getSize();
      }

      byte[] vb1 = v1.get();
      byte[] vb2 = v2.get();

      for (int i = 0; i < vb1.length; i++) {
        if (vb1[i] != vb2[i]) {
          return String.format("first diff at offset %,d 0x%02x != 0x%02x", i, 0xff & vb1[i],
              0xff & vb2[i]);
        }
      }

      return null;
    }

    @Override
    public Key getTopKey() {
      return topKey;
    }

    @Override
    public Value getTopValue() {
      return topValue;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      ChunkCombiner cc = new ChunkCombiner();
      try {
        cc.init(source.deepCopy(env), null, env);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
      return cc;
    }
  }

  /**
   * Copied from 2.0 Examples. A utility for merging visibilities into the form
   * {@code (VIS1)|(VIS2)|...|(VISN)}. Used by the ChunkCombiner.
   */
  public static class VisibilityCombiner {

    private TreeSet<String> visibilities = new TreeSet<>();

    void add(ByteSequence cv) {
      if (cv.length() == 0)
        return;

      int depth = 0;
      int offset = 0;

      for (int i = 0; i < cv.length(); i++) {
        switch (cv.byteAt(i)) {
          case '(':
            depth++;
            break;
          case ')':
            depth--;
            if (depth < 0)
              throw new IllegalArgumentException("Invalid vis " + cv);
            break;
          case '|':
            if (depth == 0) {
              insert(cv.subSequence(offset, i));
              offset = i + 1;
            }

            break;
        }
      }

      insert(cv.subSequence(offset, cv.length()));

      if (depth != 0)
        throw new IllegalArgumentException("Invalid vis " + cv);

    }

    private void insert(ByteSequence cv) {
      for (int i = 0; i < cv.length(); i++) {

      }

      String cvs = cv.toString();

      if (cvs.charAt(0) != '(')
        cvs = "(" + cvs + ")";
      else {
        int depth = 0;
        int depthZeroCloses = 0;
        for (int i = 0; i < cv.length(); i++) {
          switch (cv.byteAt(i)) {
            case '(':
              depth++;
              break;
            case ')':
              depth--;
              if (depth == 0)
                depthZeroCloses++;
              break;
          }
        }

        if (depthZeroCloses > 1)
          cvs = "(" + cvs + ")";
      }

      visibilities.add(cvs);
    }

    byte[] get() {
      StringBuilder sb = new StringBuilder();
      String sep = "";
      for (String cvs : visibilities) {
        sb.append(sep);
        sep = "|";
        sb.append(cvs);
      }

      return sb.toString().getBytes();
    }
  }

  public static final byte[] nullbyte = new byte[] {0};

  /**
   * Join some number of strings using a null byte separator into a text object.
   *
   * @param s
   *          strings
   * @return a text object containing the strings separated by null bytes
   */
  public static Text buildNullSepText(String... s) {
    Text t = new Text(s[0]);
    for (int i = 1; i < s.length; i++) {
      t.append(nullbyte, 0, 1);
      t.append(s[i].getBytes(), 0, s[i].length());
    }
    return t;
  }

}

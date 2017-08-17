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
package org.apache.accumulo.tserver.compaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.NoSuchMetaStoreException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class DefaultCompactionStrategyTest {

  private static Pair<Key,Key> keys(String firstString, String secondString) {
    Key first = null;
    if (firstString != null)
      first = new Key(new Text(firstString));
    Key second = null;
    if (secondString != null)
      second = new Key(new Text(secondString));
    return new Pair<>(first, second);
  }

  static final Map<String,Pair<Key,Key>> fakeFiles = new HashMap<>();

  static {
    fakeFiles.put("file1", keys("b", "m"));
    fakeFiles.put("file2", keys("n", "z"));
    fakeFiles.put("file3", keys("a", "y"));
    fakeFiles.put("file4", keys(null, null));
  }

  // Mock FileSKVIterator, which will provide first/last keys above
  private static class TestFileSKVIterator implements FileSKVIterator {
    private String filename;

    TestFileSKVIterator(String filename) {
      this.filename = filename;
    }

    @Override
    public void setInterruptFlag(AtomicBoolean flag) {}

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {}

    @Override
    public boolean hasTop() {
      return false;
    }

    @Override
    public void next() throws IOException {}

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {}

    @Override
    public Key getTopKey() {
      return null;
    }

    @Override
    public Value getTopValue() {
      return null;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      return null;
    }

    @Override
    public Key getFirstKey() throws IOException {
      Pair<Key,Key> pair = fakeFiles.get(filename);
      if (pair == null)
        return null;
      return pair.getFirst();
    }

    @Override
    public Key getLastKey() throws IOException {
      Pair<Key,Key> pair = fakeFiles.get(filename);
      if (pair == null)
        return null;
      return pair.getSecond();
    }

    @Override
    public DataInputStream getMetaStore(String name) throws IOException, NoSuchMetaStoreException {
      return null;
    }

    @Override
    public void closeDeepCopies() throws IOException {}

    @Override
    public void close() throws IOException {}

    @Override
    public FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig) {
      return null;
    }

  }

  static final DefaultConfiguration dfault = DefaultConfiguration.getInstance();

  private static class TestCompactionRequest extends MajorCompactionRequest {
    @Override
    public FileSKVIterator openReader(FileRef ref) throws IOException {
      return new TestFileSKVIterator(ref.toString());
    }

    TestCompactionRequest(KeyExtent extent, MajorCompactionReason reason, Map<FileRef,DataFileValue> files) {
      super(extent, reason, dfault);
      setFiles(files);
    }

  }

  private MajorCompactionRequest createRequest(MajorCompactionReason reason, Object... objs) throws IOException {
    return createRequest(new KeyExtent(Table.ID.of("0"), null, null), reason, objs);
  }

  private MajorCompactionRequest createRequest(KeyExtent extent, MajorCompactionReason reason, Object... objs) throws IOException {
    Map<FileRef,DataFileValue> files = new HashMap<>();
    for (int i = 0; i < objs.length; i += 2) {
      files.put(new FileRef("hdfs://nn1/accumulo/tables/5/t-0001/" + (String) objs[i]), new DataFileValue(((Number) objs[i + 1]).longValue(), 0));
    }
    return new TestCompactionRequest(extent, reason, files);
  }

  private static Set<String> asSet(String... strings) {
    return asSet(Arrays.asList(strings));
  }

  private static Set<String> asStringSet(Collection<FileRef> refs) {
    HashSet<String> result = new HashSet<>();
    for (FileRef ref : refs) {
      result.add(ref.path().toString());
    }
    return result;
  }

  private static Set<String> asSet(Collection<String> strings) {
    HashSet<String> result = new HashSet<>();
    for (String string : strings)
      result.add("hdfs://nn1/accumulo/tables/5/t-0001/" + string);
    return result;
  }

  @Test
  public void testGetCompactionPlan() throws Exception {
    DefaultCompactionStrategy s = new DefaultCompactionStrategy();

    // do nothing
    MajorCompactionRequest request = createRequest(MajorCompactionReason.IDLE, "file1", 10, "file2", 10);
    s.gatherInformation(request);
    CompactionPlan plan = s.getCompactionPlan(request);
    assertTrue(plan.inputFiles.isEmpty());

    // do everything
    request = createRequest(MajorCompactionReason.IDLE, "file1", 10, "file2", 10, "file3", 10);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(3, plan.inputFiles.size());

    // do everything
    request = createRequest(MajorCompactionReason.USER, "file1", 10, "file2", 10);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(2, plan.inputFiles.size());

    // partial
    request = createRequest(MajorCompactionReason.NORMAL, "file0", 100, "file1", 10, "file2", 10, "file3", 10);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(3, plan.inputFiles.size());
    assertEquals(asStringSet(plan.inputFiles), asSet("file1,file2,file3".split(",")));

  }
}

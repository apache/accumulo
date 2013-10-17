package org.apache.accumulo.server.tabletserver.compaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.NoSuchMetaStoreException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
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
    return new Pair<Key, Key>(first, second);
  }
  
  static final Map<String, Pair<Key, Key>> fakeFiles = new HashMap<String, Pair<Key, Key>>();
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
    public void setInterruptFlag(AtomicBoolean flag) {
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    }

    @Override
    public boolean hasTop() {
      return false;
    }

    @Override
    public void next() throws IOException {
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    }

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
    public void closeDeepCopies() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }
    
  }

  static final DefaultConfiguration dfault = AccumuloConfiguration.getDefaultConfiguration();
  private static class TestCompactionRequest extends MajorCompactionRequest {
    @Override
    FileSKVIterator openReader(FileRef ref) throws IOException {
      return new TestFileSKVIterator(ref.toString());
    }

    TestCompactionRequest(KeyExtent extent, 
        MajorCompactionReason reason, 
        Map<FileRef, DataFileValue> files) {
        super(extent, reason, null, dfault);
        setFiles(files);
    }
    
  }
  
  
  private MajorCompactionRequest createRequest(MajorCompactionReason reason, Object... objs) throws IOException {
    return createRequest(new KeyExtent(new Text("0"), null, null), reason, objs);
  }
  
  private MajorCompactionRequest createRequest(KeyExtent extent, MajorCompactionReason reason, Object... objs) throws IOException {
    Map<FileRef, DataFileValue> files = new HashMap<FileRef, DataFileValue>();
    for (int i = 0; i < objs.length; i += 2) {
      files.put(new FileRef((String)objs[i]), new DataFileValue(((Number)objs[i+1]).longValue(), 0));
    }
    return new TestCompactionRequest(extent, reason, files);
  }
  
  private static Set<String> asSet(String ... strings) {
    return asSet(Arrays.asList(strings));
  }
  
  private static Set<String> asStringSet(Collection<FileRef> refs) {
    HashSet<String> result = new HashSet<String>();
    for (FileRef ref : refs) {
      result.add(ref.path().toString());
    }
    return result;
  }
  
  private static Set<String> asSet(Collection<String> strings) {
    HashSet<String> result = new HashSet<String>();
    result.addAll(strings);
    return result;
  }
  
  @Test
  public void testGetCompactionPlan() throws Exception {
    DefaultCompactionStrategy s = new DefaultCompactionStrategy();
    
    // do nothing
    MajorCompactionRequest request = createRequest(MajorCompactionReason.IDLE, "file1", 10, "file2", 10);
    s.gatherInformation(request);
    CompactionPlan plan = s.getCompactionPlan(request);
    assertTrue(plan.passes.isEmpty());
    
    // do everything
    request = createRequest(MajorCompactionReason.IDLE, "file1", 10, "file2", 10, "file3", 10);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(1, plan.passes.size());
    CompactionPass pass = plan.passes.get(0);
    assertEquals(3, pass.inputFiles.size());
    assertEquals(1, pass.outputFiles);
    assertFalse(plan.propogateDeletes);
    
    // do everything
    request = createRequest(MajorCompactionReason.USER, "file1", 10, "file2", 10);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(1, plan.passes.size());
    pass = plan.passes.get(0);
    assertEquals(2, pass.inputFiles.size());
    assertEquals(1, pass.outputFiles);
    assertFalse(plan.propogateDeletes);
    
    // partial
    request = createRequest(MajorCompactionReason.NORMAL, "file0", 100, "file1", 10, "file2", 10, "file3", 10);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(1, plan.passes.size());
    pass = plan.passes.get(0);
    assertEquals(3, pass.inputFiles.size());
    assertEquals(1, pass.outputFiles);
    assertEquals(asStringSet(pass.inputFiles), asSet("file1,file2,file3".split(",")));
    assertTrue(plan.propogateDeletes);
    
    // chop tests
    // everything overlaps default tablet
    request = createRequest(MajorCompactionReason.NORMAL, "file1", 10, "file2", 10, "file3", 10);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(1, plan.passes.size());
    pass = plan.passes.get(0);
    assertEquals(3, pass.inputFiles.size());
    assertEquals(1, pass.outputFiles);
    assertEquals(asStringSet(pass.inputFiles), asSet("file1,file2,file3".split(",")));
    assertFalse(plan.propogateDeletes);
    
    // Partial overlap
    KeyExtent extent = new KeyExtent(new Text("0"), new Text("n"), new Text("a"));
    request = createRequest(extent, MajorCompactionReason.CHOP, "file1", 10, "file2", 10, "file3", 10);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(1, plan.passes.size());
    pass = plan.passes.get(0);
    assertEquals(2, pass.inputFiles.size());
    assertEquals(1, pass.outputFiles);
    assertEquals(asStringSet(pass.inputFiles), asSet("file2,file3".split(",")));
    assertTrue(plan.propogateDeletes);

    // empty file
    request = createRequest(extent, MajorCompactionReason.CHOP, "file1", 10, "file4", 10);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(1, plan.passes.size());
    pass = plan.passes.get(0);
    assertEquals(1, pass.inputFiles.size());
    assertEquals(1, pass.outputFiles);
    assertEquals(asStringSet(pass.inputFiles), asSet("file4".split(",")));
    assertTrue(plan.propogateDeletes);
    
    // file without first/last keys
    request = createRequest(extent, MajorCompactionReason.CHOP, "file1", 10, "file5", 10);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(1, plan.passes.size());
    pass = plan.passes.get(0);
    assertEquals(1, pass.inputFiles.size());
    assertEquals(1, pass.outputFiles);
    assertEquals(asStringSet(pass.inputFiles), asSet("file5".split(",")));
    assertTrue(plan.propogateDeletes);
  }
}

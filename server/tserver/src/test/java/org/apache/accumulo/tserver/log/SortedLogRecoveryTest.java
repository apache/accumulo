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
package org.apache.accumulo.tserver.log;

import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_FINISH;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_START;
import static org.apache.accumulo.tserver.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.tserver.logger.LogEvents.MUTATION;
import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class SortedLogRecoveryTest {

  static final KeyExtent extent = new KeyExtent(TableId.of("table"), null, null);
  static final Text cf = new Text("cf");
  static final Text cq = new Text("cq");
  static final Value value = new Value("value".getBytes());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  static class KeyValue implements Comparable<KeyValue> {
    public final LogFileKey key;
    public final LogFileValue value;

    KeyValue() {
      key = new LogFileKey();
      value = new LogFileValue();
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key) + Objects.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
      return this == obj
          || (obj != null && obj instanceof KeyValue && 0 == compareTo((KeyValue) obj));
    }

    @Override
    public int compareTo(KeyValue o) {
      return key.compareTo(o.key);
    }
  }

  private static KeyValue createKeyValue(LogEvents type, long seq, int tid,
      Object fileExtentMutation) {
    KeyValue result = new KeyValue();
    result.key.event = type;
    result.key.seq = seq;
    result.key.tabletId = tid;
    switch (type) {
      case OPEN:
        result.key.tserverSession = (String) fileExtentMutation;
        break;
      case COMPACTION_FINISH:
        break;
      case COMPACTION_START:
        result.key.filename = (String) fileExtentMutation;
        break;
      case DEFINE_TABLET:
        result.key.tablet = (KeyExtent) fileExtentMutation;
        break;
      case MUTATION:
        result.value.mutations = Arrays.asList((Mutation) fileExtentMutation);
        break;
      case MANY_MUTATIONS:
        result.value.mutations = Arrays.asList((Mutation[]) fileExtentMutation);
    }
    return result;
  }

  private static class CaptureMutations implements MutationReceiver {
    public ArrayList<Mutation> result = new ArrayList<>();

    @Override
    public void receive(Mutation m) {
      // make a copy of Mutation:
      result.add(m);
    }
  }

  private static List<Mutation> recover(Map<String,KeyValue[]> logs, KeyExtent extent)
      throws IOException {
    return recover(logs, new HashSet<>(), extent);
  }

  private static List<Mutation> recover(Map<String,KeyValue[]> logs, Set<String> files,
      KeyExtent extent) throws IOException {
    TemporaryFolder root =
        new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));
    root.create();
    final String workdir = root.getRoot().getAbsolutePath() + "/workdir";
    VolumeManager fs = VolumeManagerImpl.getLocal(workdir);
    final Path workdirPath = new Path("file://" + workdir);
    fs.deleteRecursively(workdirPath);
    ArrayList<Path> dirs = new ArrayList<>();
    try {
      for (Entry<String,KeyValue[]> entry : logs.entrySet()) {
        String path = workdir + "/" + entry.getKey();
        FileSystem ns = fs.getVolumeByPath(new Path(path)).getFileSystem();
        @SuppressWarnings("deprecation")
        Writer map = new MapFile.Writer(ns.getConf(), ns, path + "/log1", LogFileKey.class,
            LogFileValue.class);
        for (KeyValue lfe : entry.getValue()) {
          map.append(lfe.key, lfe.value);
        }
        map.close();
        ns.create(SortedLogState.getFinishedMarkerPath(path)).close();
        dirs.add(new Path(path));
      }
      // Recover
      SortedLogRecovery recovery = new SortedLogRecovery(fs);
      CaptureMutations capture = new CaptureMutations();
      recovery.recover(extent, dirs, files, capture);
      return capture.result;
    } finally {
      root.delete();
    }
  }

  @Test
  public void testCompactionCrossesLogs() throws IOException {
    Mutation ignored = new ServerMutation(new Text("ignored"));
    ignored.put(cf, cq, value);
    Mutation m = new ServerMutation(new Text("row1"));
    m.put(cf, cq, value);
    Mutation m2 = new ServerMutation(new Text("row2"));
    m2.put(cf, cq, value);
    KeyValue[] entries =
        {createKeyValue(OPEN, 0, 1, "2"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 3, 1, "/t1/f1"),
            createKeyValue(MUTATION, 2, 1, ignored), createKeyValue(MUTATION, 4, 1, ignored),};
    KeyValue[] entries2 =
        {createKeyValue(OPEN, 0, 1, "2"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 4, 1, "/t1/f1"), createKeyValue(MUTATION, 7, 1, m),};
    KeyValue[] entries3 =
        {createKeyValue(OPEN, 0, 2, "23"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 5, 1, "/t1/f2"),
            createKeyValue(COMPACTION_FINISH, 6, 1, null), createKeyValue(MUTATION, 3, 1, ignored),
            createKeyValue(MUTATION, 4, 1, ignored),};
    KeyValue[] entries4 = {createKeyValue(OPEN, 0, 3, "69"),
        createKeyValue(DEFINE_TABLET, 1, 1, extent), createKeyValue(MUTATION, 2, 1, ignored),
        createKeyValue(MUTATION, 3, 1, ignored), createKeyValue(MUTATION, 4, 1, ignored),};
    KeyValue[] entries5 =
        {createKeyValue(OPEN, 0, 4, "70"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 3, 1, "/t1/f3"),
            createKeyValue(MUTATION, 2, 1, ignored), createKeyValue(MUTATION, 6, 1, m2),};

    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);
    logs.put("entries2", entries2);
    logs.put("entries3", entries3);
    logs.put("entries4", entries4);
    logs.put("entries5", entries5);

    // Recover
    List<Mutation> mutations = recover(logs, extent);

    // Verify recovered data
    assertEquals(2, mutations.size());
    assertTrue(mutations.contains(m));
    assertTrue(mutations.contains(m2));
  }

  @Test
  public void testCompactionCrossesLogs5() throws IOException {
    // Create a test log
    Mutation ignored = new ServerMutation(new Text("ignored"));
    ignored.put(cf, cq, value);
    Mutation m = new ServerMutation(new Text("row1"));
    m.put(cf, cq, value);
    Mutation m2 = new ServerMutation(new Text("row2"));
    m2.put(cf, cq, value);
    Mutation m3 = new ServerMutation(new Text("row3"));
    m3.put(cf, cq, value);
    Mutation m4 = new ServerMutation(new Text("row4"));
    m4.put(cf, cq, value);
    KeyValue[] entries =
        {createKeyValue(OPEN, 0, -1, "1"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 3, 1, "/t1/f1"),
            createKeyValue(MUTATION, 2, 1, ignored), createKeyValue(MUTATION, 4, 1, ignored),};
    KeyValue[] entries2 = {createKeyValue(OPEN, 5, -1, "2"),
        createKeyValue(DEFINE_TABLET, 6, 1, extent), createKeyValue(MUTATION, 7, 1, ignored),};
    // createKeyValue(COMPACTION_FINISH, 14, 1, null),
    KeyValue[] entries3 =
        {createKeyValue(OPEN, 8, -1, "3"), createKeyValue(DEFINE_TABLET, 9, 1, extent),
            createKeyValue(COMPACTION_FINISH, 10, 1, "/t1/f1"),
            createKeyValue(COMPACTION_START, 12, 1, "/t1/f2"),
            createKeyValue(COMPACTION_FINISH, 13, 1, "/t1/f2"),
            // createKeyValue(COMPACTION_FINISH, 14, 1, null),
            createKeyValue(MUTATION, 11, 1, ignored), createKeyValue(MUTATION, 15, 1, m),
            createKeyValue(MUTATION, 16, 1, m2),};
    KeyValue[] entries4 =
        {createKeyValue(OPEN, 17, -1, "4"), createKeyValue(DEFINE_TABLET, 18, 1, extent),
            createKeyValue(COMPACTION_START, 20, 1, "/t1/f3"), createKeyValue(MUTATION, 19, 1, m3),
            createKeyValue(MUTATION, 21, 1, m4),};
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);
    logs.put("entries2", entries2);
    logs.put("entries3", entries3);
    logs.put("entries4", entries4);
    // Recover
    List<Mutation> mutations = recover(logs, extent);
    // Verify recovered data
    assertEquals(4, mutations.size());
    assertEquals(m, mutations.get(0));
    assertEquals(m2, mutations.get(1));
    assertEquals(m3, mutations.get(2));
    assertEquals(m4, mutations.get(3));
  }

  @Test
  public void testCompactionCrossesLogs6() throws IOException {
    // Create a test log
    Mutation ignored = new ServerMutation(new Text("ignored"));
    ignored.put(cf, cq, value);
    Mutation m = new ServerMutation(new Text("row1"));
    m.put(cf, cq, value);
    Mutation m2 = new ServerMutation(new Text("row2"));
    m2.put(cf, cq, value);
    Mutation m3 = new ServerMutation(new Text("row3"));
    m3.put(cf, cq, value);
    Mutation m4 = new ServerMutation(new Text("row4"));
    m4.put(cf, cq, value);
    Mutation m5 = new ServerMutation(new Text("row5"));
    m5.put(cf, cq, value);
    KeyValue[] entries =
        {createKeyValue(OPEN, 0, 1, "2"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(MUTATION, 1, 1, ignored), createKeyValue(MUTATION, 3, 1, m),};
    KeyValue[] entries2 =
        {createKeyValue(OPEN, 0, 1, "2"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 2, 1, "/t1/f1"),
            createKeyValue(COMPACTION_FINISH, 3, 1, "/t1/f1"), createKeyValue(MUTATION, 3, 1, m2),};

    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);
    logs.put("entries2", entries2);

    // Recover
    List<Mutation> mutations = recover(logs, extent);

    // Verify recovered data
    assertEquals(2, mutations.size());
    assertEquals(m, mutations.get(0));
    assertEquals(m2, mutations.get(1));
  }

  @Test
  public void testEmpty() throws IOException {
    // Create a test log
    KeyValue[] entries =
        {createKeyValue(OPEN, 0, -1, "1"), createKeyValue(DEFINE_TABLET, 1, 1, extent),};
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("testlog", entries);
    // Recover
    List<Mutation> mutations = recover(logs, extent);
    // Verify recovered data
    assertEquals(0, mutations.size());

  }

  @Test
  public void testMissingDefinition() {
    // Create a test log
    KeyValue[] entries = {createKeyValue(OPEN, 0, -1, "1"),};
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("testlog", entries);
    // Recover
    try {
      recover(logs, extent);
      fail("tablet should not have been found");
    } catch (Throwable t) {}
  }

  @Test
  public void testSimple() throws IOException {
    // Create a test log
    Mutation m = new ServerMutation(new Text("row1"));
    m.put(cf, cq, value);
    KeyValue[] entries = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 1, 1, extent), createKeyValue(MUTATION, 2, 1, m),};
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("testlog", entries);
    // Recover
    List<Mutation> mutations = recover(logs, extent);
    // Verify recovered data
    assertEquals(1, mutations.size());
    assertEquals(m, mutations.get(0));
  }

  @Test
  public void testSkipSuccessfulCompaction() throws IOException {
    // Create a test log
    Mutation ignored = new ServerMutation(new Text("ignored"));
    ignored.put(cf, cq, value);
    Mutation m = new ServerMutation(new Text("row1"));
    m.put(cf, cq, value);
    KeyValue[] entries =
        {createKeyValue(OPEN, 0, -1, "1"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 3, 1, "/t1/f1"),
            createKeyValue(COMPACTION_FINISH, 4, 1, null), createKeyValue(MUTATION, 2, 1, ignored),
            createKeyValue(MUTATION, 5, 1, m),};
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("testlog", entries);
    // Recover
    List<Mutation> mutations = recover(logs, extent);
    // Verify recovered data
    assertEquals(1, mutations.size());
    assertEquals(m, mutations.get(0));
  }

  @Test
  public void testSkipSuccessfulCompactionAcrossFiles() throws IOException {
    // Create a test log
    Mutation ignored = new ServerMutation(new Text("ignored"));
    ignored.put(cf, cq, value);
    Mutation m = new ServerMutation(new Text("row1"));
    m.put(cf, cq, value);
    KeyValue[] entries = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 1, 1, extent),
        createKeyValue(COMPACTION_START, 3, 1, "/t1/f1"), createKeyValue(MUTATION, 2, 1, ignored),};
    KeyValue[] entries2 =
        {createKeyValue(OPEN, 4, -1, "1"), createKeyValue(DEFINE_TABLET, 5, 1, extent),
            createKeyValue(COMPACTION_FINISH, 6, 1, null), createKeyValue(MUTATION, 7, 1, m),};
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);
    logs.put("entries2", entries2);
    // Recover
    List<Mutation> mutations = recover(logs, extent);
    // Verify recovered data
    assertEquals(1, mutations.size());
    assertEquals(m, mutations.get(0));
  }

  @Test
  public void testGetMutationsAfterCompactionStart() throws IOException {
    // Create a test log
    Mutation ignored = new ServerMutation(new Text("ignored"));
    ignored.put(cf, cq, value);
    Mutation m = new ServerMutation(new Text("row1"));
    m.put(cf, cq, value);
    Mutation m2 = new ServerMutation(new Text("row2"));
    m2.put(cf, cq, new Value("123".getBytes()));
    KeyValue[] entries =
        {createKeyValue(OPEN, 0, -1, "1"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 3, 1, "/t1/f1"),
            createKeyValue(MUTATION, 2, 1, ignored), createKeyValue(MUTATION, 4, 1, m),};
    KeyValue[] entries2 =
        {createKeyValue(OPEN, 5, -1, "1"), createKeyValue(DEFINE_TABLET, 4, 1, extent),
            createKeyValue(COMPACTION_FINISH, 4, 1, null), createKeyValue(MUTATION, 8, 1, m2),};
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);
    logs.put("entries2", entries2);
    // Recover
    List<Mutation> mutations = recover(logs, extent);
    // Verify recovered data
    assertEquals(2, mutations.size());
    assertEquals(m, mutations.get(0));
    assertEquals(m2, mutations.get(1));
  }

  @Test
  public void testDoubleFinish() throws IOException {
    // Create a test log
    Mutation ignored = new ServerMutation(new Text("ignored"));
    ignored.put(cf, cq, value);
    Mutation m = new ServerMutation(new Text("row1"));
    m.put(cf, cq, value);
    Mutation m2 = new ServerMutation(new Text("row2"));
    m2.put(cf, cq, new Value("123".getBytes()));
    KeyValue[] entries = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 1, 1, extent), createKeyValue(COMPACTION_FINISH, 2, 1, null),
        createKeyValue(COMPACTION_START, 4, 1, "/t1/f1"),
        createKeyValue(COMPACTION_FINISH, 5, 1, null), createKeyValue(MUTATION, 3, 1, ignored),
        createKeyValue(MUTATION, 5, 1, m), createKeyValue(MUTATION, 5, 1, m2),};
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);
    // Recover
    List<Mutation> mutations = recover(logs, extent);
    // Verify recovered data
    assertEquals(2, mutations.size());
    assertEquals(m, mutations.get(0));
    assertEquals(m2, mutations.get(1));
  }

  @Test
  public void testCompactionCrossesLogs2() throws IOException {
    // Create a test log
    Mutation ignored = new ServerMutation(new Text("ignored"));
    ignored.put(cf, cq, value);
    Mutation m = new ServerMutation(new Text("row1"));
    m.put(cf, cq, value);
    Mutation m2 = new ServerMutation(new Text("row2"));
    m2.put(cf, cq, value);
    Mutation m3 = new ServerMutation(new Text("row3"));
    m3.put(cf, cq, value);
    KeyValue[] entries =
        {createKeyValue(OPEN, 0, -1, "1"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 3, 1, "/t1/f1"),
            createKeyValue(MUTATION, 2, 1, ignored), createKeyValue(MUTATION, 4, 1, m),};
    KeyValue[] entries2 = {createKeyValue(OPEN, 5, -1, "1"),
        createKeyValue(DEFINE_TABLET, 4, 1, extent), createKeyValue(MUTATION, 4, 1, m2),};
    KeyValue[] entries3 =
        {createKeyValue(OPEN, 8, -1, "1"), createKeyValue(DEFINE_TABLET, 4, 1, extent),
            createKeyValue(COMPACTION_FINISH, 4, 1, null), createKeyValue(MUTATION, 4, 1, m3),};
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);
    logs.put("entries2", entries2);
    logs.put("entries3", entries3);
    // Recover
    List<Mutation> mutations = recover(logs, extent);
    // Verify recovered data
    assertEquals(3, mutations.size());
    assertTrue(mutations.contains(m));
    assertTrue(mutations.contains(m2));
    assertTrue(mutations.contains(m3));
  }

  @Test
  public void testBug1() throws IOException {
    // this unit test reproduces a bug that occurred, nothing should recover
    Mutation m1 = new ServerMutation(new Text("row1"));
    m1.put(cf, cq, value);
    Mutation m2 = new ServerMutation(new Text("row2"));
    m2.put(cf, cq, value);
    KeyValue[] entries =
        {createKeyValue(OPEN, 0, -1, "1"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 30, 1, "/t1/f1"),
            createKeyValue(COMPACTION_FINISH, 32, 1, "/t1/f1"), createKeyValue(MUTATION, 29, 1, m1),
            createKeyValue(MUTATION, 30, 1, m2),};
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("testlog", entries);
    // Recover
    List<Mutation> mutations = recover(logs, extent);
    // Verify recovered data
    assertEquals(0, mutations.size());
  }

  @Test
  public void testBug2() throws IOException {
    // Create a test log
    Mutation ignored = new ServerMutation(new Text("ignored"));
    ignored.put(cf, cq, value);
    Mutation m = new ServerMutation(new Text("row1"));
    m.put(cf, cq, value);
    Mutation m2 = new ServerMutation(new Text("row2"));
    m2.put(cf, cq, value);
    Mutation m3 = new ServerMutation(new Text("row3"));
    m3.put(cf, cq, value);
    KeyValue[] entries =
        {createKeyValue(OPEN, 0, -1, "1"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 3, 1, "/t1/f1"),
            createKeyValue(COMPACTION_FINISH, 4, 1, null), createKeyValue(MUTATION, 4, 1, m),};
    KeyValue[] entries2 =
        {createKeyValue(OPEN, 5, -1, "1"), createKeyValue(DEFINE_TABLET, 6, 1, extent),
            createKeyValue(COMPACTION_START, 8, 1, "/t1/f1"), createKeyValue(MUTATION, 7, 1, m2),
            createKeyValue(MUTATION, 9, 1, m3),};
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);
    logs.put("entries2", entries2);
    // Recover
    List<Mutation> mutations = recover(logs, extent);
    // Verify recovered data
    assertEquals(3, mutations.size());
    assertEquals(m, mutations.get(0));
    assertEquals(m2, mutations.get(1));
    assertEquals(m3, mutations.get(2));
  }

  @Test
  public void testCompactionCrossesLogs4() throws IOException {
    // Create a test log
    Mutation ignored = new ServerMutation(new Text("ignored"));
    ignored.put(cf, cq, value);
    Mutation m = new ServerMutation(new Text("row1"));
    m.put(cf, cq, value);
    Mutation m2 = new ServerMutation(new Text("row2"));
    m2.put(cf, cq, value);
    Mutation m3 = new ServerMutation(new Text("row3"));
    m3.put(cf, cq, value);
    Mutation m4 = new ServerMutation(new Text("row4"));
    m4.put(cf, cq, value);
    Mutation m5 = new ServerMutation(new Text("row5"));
    m5.put(cf, cq, value);
    Mutation m6 = new ServerMutation(new Text("row6"));
    m6.put(cf, cq, value);
    // createKeyValue(COMPACTION_FINISH, 5, 1, null),
    KeyValue[] entries =
        {createKeyValue(OPEN, 0, -1, "1"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 4, 1, "/t1/f1"),
            // createKeyValue(COMPACTION_FINISH, 5, 1, null),
            createKeyValue(MUTATION, 2, 1, m), createKeyValue(MUTATION, 3, 1, m2),};
    KeyValue[] entries2 =
        {createKeyValue(OPEN, 5, -1, "2"), createKeyValue(DEFINE_TABLET, 6, 1, extent),
            createKeyValue(MUTATION, 7, 1, m3), createKeyValue(MUTATION, 8, 1, m4),};
    // createKeyValue(COMPACTION_FINISH, 11, 1, null),
    // createKeyValue(COMPACTION_FINISH, 14, 1, null),
    // createKeyValue(COMPACTION_START, 15, 1, "somefile"),
    // createKeyValue(COMPACTION_FINISH, 17, 1, null),
    // createKeyValue(COMPACTION_START, 18, 1, "somefile"),
    // createKeyValue(COMPACTION_FINISH, 19, 1, null),
    KeyValue[] entries3 =
        {createKeyValue(OPEN, 9, -1, "3"), createKeyValue(DEFINE_TABLET, 10, 1, extent),
            // createKeyValue(COMPACTION_FINISH, 11, 1, null),
            createKeyValue(COMPACTION_START, 12, 1, "/t1/f1"),
            // createKeyValue(COMPACTION_FINISH, 14, 1, null),
            // createKeyValue(COMPACTION_START, 15, 1, "somefile"),
            // createKeyValue(COMPACTION_FINISH, 17, 1, null),
            // createKeyValue(COMPACTION_START, 18, 1, "somefile"),
            // createKeyValue(COMPACTION_FINISH, 19, 1, null),
            createKeyValue(MUTATION, 9, 1, m5), createKeyValue(MUTATION, 20, 1, m6),};
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);
    logs.put("entries2", entries2);
    logs.put("entries3", entries3);
    // Recover

    List<Mutation> mutations = recover(logs, extent);

    // Verify recovered data
    assertEquals(6, mutations.size());
    assertEquals(m, mutations.get(0));
    assertEquals(m2, mutations.get(1));
    assertEquals(m3, mutations.get(2));
    assertEquals(m4, mutations.get(3));
    assertEquals(m5, mutations.get(4));
    assertEquals(m6, mutations.get(5));
  }

  @Test
  public void testLookingForBug3() throws IOException {
    Mutation ignored = new ServerMutation(new Text("ignored"));
    ignored.put(cf, cq, value);
    Mutation m = new ServerMutation(new Text("row1"));
    m.put(cf, cq, value);
    Mutation m2 = new ServerMutation(new Text("row2"));
    m2.put(cf, cq, value);
    Mutation m3 = new ServerMutation(new Text("row3"));
    m3.put(cf, cq, value);
    Mutation m4 = new ServerMutation(new Text("row4"));
    m4.put(cf, cq, value);
    Mutation m5 = new ServerMutation(new Text("row5"));
    m5.put(cf, cq, value);
    KeyValue[] entries =
        {createKeyValue(OPEN, 0, -1, "1"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 2, 1, "/t1/f1"),
            createKeyValue(COMPACTION_FINISH, 3, 1, null), createKeyValue(MUTATION, 1, 1, ignored),
            createKeyValue(MUTATION, 3, 1, m), createKeyValue(MUTATION, 3, 1, m2),
            createKeyValue(MUTATION, 3, 1, m3),};
    KeyValue[] entries2 =
        {createKeyValue(OPEN, 0, -1, "2"), createKeyValue(DEFINE_TABLET, 1, 1, extent),
            createKeyValue(COMPACTION_START, 2, 1, "/t1/f12"), createKeyValue(MUTATION, 3, 1, m4),
            createKeyValue(MUTATION, 3, 1, m5),};
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);
    logs.put("entries2", entries2);
    // Recover
    List<Mutation> mutations = recover(logs, extent);
    // Verify recovered data
    assertEquals(5, mutations.size());
    assertTrue(mutations.contains(m));
    assertTrue(mutations.contains(m2));
    assertTrue(mutations.contains(m3));
    assertTrue(mutations.contains(m4));
    assertTrue(mutations.contains(m5));
  }

  @Test
  public void testMultipleTabletDefinition() throws Exception {
    // test for a tablet defined multiple times in a log file
    // there was a bug where the oldest tablet id was used instead
    // of the newest

    Mutation ignored = new ServerMutation(new Text("row1"));
    ignored.put("foo", "bar", "v1");
    Mutation m = new ServerMutation(new Text("row1"));
    m.put("foo", "bar", "v1");

    KeyValue[] entries = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 1, 1, extent), createKeyValue(DEFINE_TABLET, 1, 2, extent),
        createKeyValue(MUTATION, 2, 2, ignored), createKeyValue(COMPACTION_START, 5, 2, "/t1/f1"),
        createKeyValue(MUTATION, 6, 2, m), createKeyValue(COMPACTION_FINISH, 6, 2, null),};

    Arrays.sort(entries);

    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);

    List<Mutation> mutations = recover(logs, extent);

    assertEquals(1, mutations.size());
    assertEquals(m, mutations.get(0));
  }

  @Test
  public void testNoFinish0() throws Exception {
    // its possible that a minor compaction finishes successfully, but the process dies before
    // writing the compaction event

    Mutation ignored = new ServerMutation(new Text("row1"));
    ignored.put("foo", "bar", "v1");

    KeyValue[] entries = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 1, 2, extent), createKeyValue(MUTATION, 2, 2, ignored),
        createKeyValue(COMPACTION_START, 3, 2, "/t/f1")};

    Arrays.sort(entries);
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);

    List<Mutation> mutations = recover(logs, Collections.singleton("/t/f1"), extent);

    assertEquals(0, mutations.size());
  }

  @Test
  public void testNoFinish1() throws Exception {
    // its possible that a minor compaction finishes successfully, but the process dies before
    // writing the compaction event

    Mutation ignored = new ServerMutation(new Text("row1"));
    ignored.put("foo", "bar", "v1");
    Mutation m = new ServerMutation(new Text("row1"));
    m.put("foo", "bar", "v2");

    KeyValue[] entries = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 1, 2, extent), createKeyValue(MUTATION, 2, 2, ignored),
        createKeyValue(COMPACTION_START, 3, 2, "/t/f1"), createKeyValue(MUTATION, 4, 2, m),};

    Arrays.sort(entries);
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);

    List<Mutation> mutations = recover(logs, Collections.singleton("/t/f1"), extent);

    assertEquals(1, mutations.size());
    assertEquals(m, mutations.get(0));
  }

  @Test
  public void testLeaveAndComeBack() throws IOException {
    /**
     * This test recreates the situation in bug #449 (Github issues).
     */
    Mutation m1 = new ServerMutation(new Text("r1"));
    m1.put("f1", "q1", "v1");

    Mutation m2 = new ServerMutation(new Text("r2"));
    m2.put("f1", "q1", "v2");

    KeyValue[] entries1 = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 100, 10, extent), createKeyValue(MUTATION, 100, 10, m1),
        createKeyValue(COMPACTION_START, 101, 10, "/t/f1"),
        createKeyValue(COMPACTION_FINISH, 102, 10, null)};

    KeyValue[] entries2 = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 1, 20, extent), createKeyValue(MUTATION, 1, 20, m2)};

    Arrays.sort(entries1);
    Arrays.sort(entries2);
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries1", entries1);
    logs.put("entries2", entries2);

    List<Mutation> mutations = recover(logs, extent);

    assertEquals(1, mutations.size());
    assertEquals(m2, mutations.get(0));
  }

  @Test
  public void testMultipleTablets() throws IOException {
    KeyExtent e1 = new KeyExtent(TableId.of("1"), new Text("m"), null);
    KeyExtent e2 = new KeyExtent(TableId.of("1"), null, new Text("m"));

    Mutation m1 = new ServerMutation(new Text("b"));
    m1.put("f1", "q1", "v1");

    Mutation m2 = new ServerMutation(new Text("b"));
    m2.put("f1", "q2", "v2");

    Mutation m3 = new ServerMutation(new Text("s"));
    m3.put("f1", "q1", "v3");

    Mutation m4 = new ServerMutation(new Text("s"));
    m4.put("f1", "q2", "v4");

    KeyValue[] entries1 =
        {createKeyValue(OPEN, 0, -1, "1"), createKeyValue(DEFINE_TABLET, 7, 10, e1),
            createKeyValue(DEFINE_TABLET, 5, 11, e2), createKeyValue(MUTATION, 8, 10, m1),
            createKeyValue(COMPACTION_START, 9, 10, "/t/f1"), createKeyValue(MUTATION, 10, 10, m2),
            createKeyValue(COMPACTION_FINISH, 10, 10, null), createKeyValue(MUTATION, 6, 11, m3),
            createKeyValue(COMPACTION_START, 7, 11, "/t/f2"), createKeyValue(MUTATION, 8, 11, m4)};

    Arrays.sort(entries1);

    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries1", entries1);

    List<Mutation> mutations1 = recover(logs, e1);
    assertEquals(1, mutations1.size());
    assertEquals(m2, mutations1.get(0));

    List<Mutation> mutations2 = recover(logs, e2);
    assertEquals(2, mutations2.size());
    assertEquals(m3, mutations2.get(0));
    assertEquals(m4, mutations2.get(1));

    KeyValue[] entries2 = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 9, 11, e2), createKeyValue(COMPACTION_FINISH, 8, 11, null)};
    Arrays.sort(entries2);
    logs.put("entries2", entries2);

    mutations2 = recover(logs, e2);
    assertEquals(1, mutations2.size());
    assertEquals(m4, mutations2.get(0));
  }

  private void runPathTest(boolean startMatches, String compactionStartFile, String... tabletFiles)
      throws IOException {
    Mutation m1 = new ServerMutation(new Text("row1"));
    m1.put("foo", "bar", "v1");
    Mutation m2 = new ServerMutation(new Text("row1"));
    m2.put("foo", "bar", "v2");

    KeyValue[] entries = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 1, 2, extent), createKeyValue(MUTATION, 2, 2, m1),
        createKeyValue(COMPACTION_START, 3, 2, compactionStartFile),
        createKeyValue(MUTATION, 4, 2, m2),};

    Arrays.sort(entries);
    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries", entries);

    HashSet<String> filesSet = new HashSet<>();
    filesSet.addAll(Arrays.asList(tabletFiles));
    List<Mutation> mutations = recover(logs, filesSet, extent);

    if (!startMatches) {
      assertEquals(2, mutations.size());
      assertEquals(m1, mutations.get(0));
      assertEquals(m2, mutations.get(1));
    } else {
      assertEquals(1, mutations.size());
      assertEquals(m2, mutations.get(0));
    }
  }

  @Test
  public void testPaths() throws IOException {
    // test having different paths for the same file. This can happen as a result of upgrade or user
    // changing configuration
    runPathTest(false, "/t1/f1", "/t1/f0");
    runPathTest(true, "/t1/f1", "/t1/f0", "/t1/f1");

    String[] aliases = {"/t1/f1", "hdfs://nn1/accumulo/tables/8/t1/f1",
        "hdfs://1.2.3.4/accumulo/tables/8/t1/f1", "hdfs://1.2.3.4//accumulo//tables//8//t1//f1"};
    String[] others = {"/t1/f0", "hdfs://nn1/accumulo/tables/8/t1/f2",
        "hdfs://1.2.3.4//accumulo//tables//8//t1//f3", "hdfs://nn1/accumulo/tables/8/t1/t1",
        "hdfs://nn1/accumulo/tables/8/f1/f1"};

    for (String alias1 : aliases) {
      for (String alias2 : aliases) {
        runPathTest(true, alias1, alias2);
        for (String other : others) {
          runPathTest(true, alias1, other, alias2);
          runPathTest(true, alias1, alias2, other);
        }
      }
    }

    for (String alias1 : aliases) {
      for (String other : others) {
        runPathTest(false, alias1, other);
      }
    }
  }

  @Test
  public void testOnlyCompactionFinishEvent() throws IOException {
    Mutation m1 = new ServerMutation(new Text("r1"));
    m1.put("f1", "q1", "v1");

    // The presence of only a compaction finish event indicates the write ahead logs are incomplete
    // in some way. This should cause an exception.

    KeyValue[] entries1 = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 100, 10, extent),
        createKeyValue(COMPACTION_FINISH, 102, 10, null), createKeyValue(MUTATION, 102, 10, m1)};

    Arrays.sort(entries1);

    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries1", entries1);

    List<Mutation> mutations = recover(logs, extent);
    assertEquals(1, mutations.size());
    assertEquals(m1, mutations.get(0));
  }

  @Test
  public void testConsecutiveCompactionFinishEvents() throws IOException {
    Mutation m1 = new ServerMutation(new Text("r1"));
    m1.put("f1", "q1", "v1");

    Mutation m2 = new ServerMutation(new Text("r2"));
    m2.put("f1", "q1", "v2");

    // Consecutive compaction finish events indicate the write ahead logs are incomplete in some
    // way. This should cause an exception.

    KeyValue[] entries1 = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 100, 10, extent), createKeyValue(MUTATION, 100, 10, m1),
        createKeyValue(COMPACTION_START, 102, 10, "/t/f1"),
        createKeyValue(COMPACTION_FINISH, 103, 10, null),
        createKeyValue(COMPACTION_FINISH, 109, 10, null), createKeyValue(MUTATION, 105, 10, m2)};

    Arrays.sort(entries1);

    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries1", entries1);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("consecutive " + LogEvents.COMPACTION_FINISH.name());
    recover(logs, extent);
  }

  @Test
  public void testDuplicateCompactionFinishEvents() throws IOException {
    Mutation m1 = new ServerMutation(new Text("r1"));
    m1.put("f1", "q1", "v1");

    Mutation m2 = new ServerMutation(new Text("r2"));
    m2.put("f1", "q1", "v2");

    // Duplicate consecutive compaction finish events should not cause an exception.

    KeyValue[] entries1 = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 100, 10, extent), createKeyValue(MUTATION, 100, 10, m1),
        createKeyValue(COMPACTION_START, 102, 10, "/t/f1"),
        createKeyValue(COMPACTION_FINISH, 103, 10, null),
        createKeyValue(COMPACTION_FINISH, 103, 10, null), createKeyValue(MUTATION, 103, 10, m2)};

    Arrays.sort(entries1);

    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries1", entries1);

    List<Mutation> mutations1 = recover(logs, extent);
    assertEquals(1, mutations1.size());
    assertEquals(m2, mutations1.get(0));
  }

  @Test
  public void testEmptyLogFiles() throws IOException {
    Mutation m1 = new ServerMutation(new Text("r1"));
    m1.put("f1", "q1", "v1");

    Mutation m2 = new ServerMutation(new Text("r2"));
    m2.put("f1", "q1", "v2");

    KeyValue[] entries1 = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 100, 10, extent), createKeyValue(MUTATION, 100, 10, m1)};

    KeyValue[] entries2 = {createKeyValue(OPEN, 0, -1, "1")};

    KeyValue[] entries3 =
        {createKeyValue(OPEN, 0, -1, "1"), createKeyValue(DEFINE_TABLET, 105, 10, extent),
            createKeyValue(COMPACTION_START, 107, 10, "/t/f1")};

    KeyValue[] entries4 = {};

    KeyValue[] entries5 =
        {createKeyValue(OPEN, 0, -1, "1"), createKeyValue(DEFINE_TABLET, 107, 10, extent),
            createKeyValue(COMPACTION_FINISH, 111, 10, null)};

    KeyValue[] entries6 = {createKeyValue(OPEN, 0, -1, "1"),
        createKeyValue(DEFINE_TABLET, 122, 10, extent), createKeyValue(MUTATION, 123, 10, m2)};

    Arrays.sort(entries1);
    Arrays.sort(entries2);
    Arrays.sort(entries3);
    Arrays.sort(entries4);
    Arrays.sort(entries5);
    Arrays.sort(entries6);

    Map<String,KeyValue[]> logs = new TreeMap<>();

    logs.put("entries1", entries1);

    List<Mutation> mutations = recover(logs, extent);
    assertEquals(1, mutations.size());
    assertEquals(m1, mutations.get(0));

    logs.put("entries2", entries2);

    mutations = recover(logs, extent);
    assertEquals(1, mutations.size());
    assertEquals(m1, mutations.get(0));

    logs.put("entries3", entries3);

    mutations = recover(logs, extent);
    assertEquals(1, mutations.size());
    assertEquals(m1, mutations.get(0));

    logs.put("entries4", entries4);

    mutations = recover(logs, extent);
    assertEquals(1, mutations.size());
    assertEquals(m1, mutations.get(0));

    logs.put("entries5", entries5);

    mutations = recover(logs, extent);
    assertEquals(0, mutations.size());

    logs.put("entries6", entries6);

    mutations = recover(logs, extent);
    assertEquals(1, mutations.size());
    assertEquals(m2, mutations.get(0));
  }

  @Test
  public void testFileWithoutOpen() throws IOException {
    Mutation m1 = new ServerMutation(new Text("r1"));
    m1.put("f1", "q1", "v1");

    Mutation m2 = new ServerMutation(new Text("r2"));
    m2.put("f1", "q1", "v2");

    // Its expected that every log file should have an open event as the first event. Should throw
    // an error if not present.

    KeyValue[] entries1 = {createKeyValue(DEFINE_TABLET, 100, 10, extent),
        createKeyValue(MUTATION, 100, 10, m1), createKeyValue(COMPACTION_FINISH, 102, 10, null),
        createKeyValue(MUTATION, 105, 10, m2)};

    Arrays.sort(entries1);

    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("entries1", entries1);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("not " + LogEvents.OPEN);
    recover(logs, extent);
  }
}

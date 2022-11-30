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
package org.apache.accumulo.tserver.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

@SuppressWarnings("removal")
public class DefaultCompactionStrategyTest {

  private static Pair<Key,Key> keys(String firstString, String secondString) {
    Key first = null;
    if (firstString != null) {
      first = new Key(new Text(firstString));
    }
    Key second = null;
    if (secondString != null) {
      second = new Key(new Text(secondString));
    }
    return new Pair<>(first, second);
  }

  public static ServerContext getServerContext() {
    ServerContext context = EasyMock.createMock(ServerContext.class);
    EasyMock.replay(context);
    return context;
  }

  static final Map<String,Pair<Key,Key>> fakeFiles = new HashMap<>();

  static {
    fakeFiles.put("file1", keys("b", "m"));
    fakeFiles.put("file2", keys("n", "z"));
    fakeFiles.put("file3", keys("a", "y"));
    fakeFiles.put("file4", keys(null, null));
  }

  static final DefaultConfiguration dfault = DefaultConfiguration.getInstance();

  private static class TestCompactionRequest extends MajorCompactionRequest {

    Integer mfpt = null;

    TestCompactionRequest(KeyExtent extent, MajorCompactionReason reason,
        Map<StoredTabletFile,DataFileValue> files) {
      super(extent, reason, dfault, getServerContext());
      setFiles(files);
    }

    TestCompactionRequest(KeyExtent extent, MajorCompactionReason reason,
        Map<StoredTabletFile,DataFileValue> files, AccumuloConfiguration config) {
      super(extent, reason, config, getServerContext());
      setFiles(files);
    }

    public void setMaxFilesPerTablet(int mfpt) {
      this.mfpt = mfpt;
    }

    @Override
    public int getMaxFilesPerTablet() {
      if (mfpt != null) {
        return mfpt;
      }
      return super.getMaxFilesPerTablet();
    }

  }

  static Map<StoredTabletFile,DataFileValue> createFileMap(Object... objs) {
    Map<StoredTabletFile,DataFileValue> files = new HashMap<>();
    for (int i = 0; i < objs.length; i += 2) {
      files.put(new StoredTabletFile("hdfs://nn1/accumulo/tables/5/t-0001/" + objs[i]),
          new DataFileValue(((Number) objs[i + 1]).longValue(), 0));
    }
    return files;
  }

  private TestCompactionRequest createRequest(MajorCompactionReason reason, Object... objs) {
    return createRequest(new KeyExtent(TableId.of("0"), null, null), reason, objs);
  }

  private TestCompactionRequest createRequest(KeyExtent extent, MajorCompactionReason reason,
      Object... objs) {
    return new TestCompactionRequest(extent, reason, createFileMap(objs));
  }

  private static Set<String> asSet(String... strings) {
    return asSet(Arrays.asList(strings));
  }

  private static Set<String> asStringSet(Collection<StoredTabletFile> refs) {
    HashSet<String> result = new HashSet<>();
    for (TabletFile ref : refs) {
      result.add(ref.getPathStr());
    }
    return result;
  }

  private static Set<String> asSet(Collection<String> strings) {
    HashSet<String> result = new HashSet<>();
    for (String string : strings) {
      result.add("hdfs://nn1/accumulo/tables/5/t-0001/" + string);
    }
    return result;
  }

  @Test
  public void testGetCompactionPlan() throws Exception {

    // test are expecting this default
    assertEquals(10,
        DefaultConfiguration.getInstance().getCount(Property.TSERV_MAJC_THREAD_MAXOPEN));

    DefaultCompactionStrategy s = new DefaultCompactionStrategy();

    // do nothing
    TestCompactionRequest request =
        createRequest(MajorCompactionReason.IDLE, "file1", 10, "file2", 10);
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
    request = createRequest(MajorCompactionReason.NORMAL, "file0", 100, "file1", 10, "file2", 10,
        "file3", 10);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(3, plan.inputFiles.size());
    assertEquals(asStringSet(plan.inputFiles), asSet("file1,file2,file3".split(",")));

    // Two windows (of size 10 or less) meet the compaction criteria. Should select the smallest set
    // of files that meet the criteria.
    request = createRequest(MajorCompactionReason.NORMAL, "file0", 100, "file1", 100, "file2", 100,
        "file3", 10, "file4", 10, "file5", 10, "file6", 10, "file7", 10, "file8", 10, "file9", 10,
        "fileA", 10);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(8, plan.inputFiles.size());
    assertEquals(asStringSet(plan.inputFiles),
        asSet("file3,file4,file5,file6,file7,file8,file9,fileA".split(",")));

    // The last 10 files do not meet compaction ratio critea. Should move window of 10 files up
    // looking for files that meet criteria.
    request = createRequest(MajorCompactionReason.NORMAL, "file0", 19683, "file1", 19683, "file2",
        19683, "file3", 6561, "file4", 2187, "file5", 729, "file6", 243, "file7", 81, "file8", 27,
        "file9", 9, "fileA", 3, "fileB", 1);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(10, plan.inputFiles.size());
    assertEquals(asStringSet(plan.inputFiles),
        asSet("file0,file1,file2,file3,file4,file5,file6,file7,file8,file9".split(",")));

    // No window of files meets the compaction criteria, but there are more files than the max
    // allowed. Should compact the smallest 2.
    request = createRequest(MajorCompactionReason.NORMAL, "file1", 19683, "file2", 19683, "file3",
        6561, "file4", 2187, "file5", 729, "file6", 243, "file7", 81, "file8", 27, "file9", 9,
        "fileA", 3, "fileB", 1);
    request.setMaxFilesPerTablet(10);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(2, plan.inputFiles.size());
    assertEquals(asStringSet(plan.inputFiles), asSet("fileA,fileB".split(",")));

    // The last 9 files meet the compaction criteria, but 10 files need to be compacted. Should move
    // window of 10 files up looking for files that meet criteria.
    request = createRequest(MajorCompactionReason.NORMAL, "file01", 1500, "file02", 1400, "file03",
        1300, "file04", 1200, "file05", 1100, "file06", 1000, "file07", 900, "file08", 800,
        "file09", 700, "file10", 600, "file11", 500, "file12", 400, "file13", 400, "file14", 300,
        "file15", 200, "file16", 100, "file17", 9, "file18", 8, "file19", 7, "file20", 6, "file21",
        5, "file22", 4, "file23", 3, "file24", 2, "file25", 1);
    request.setMaxFilesPerTablet(15);
    s.gatherInformation(request);
    plan = s.getCompactionPlan(request);
    assertEquals(10, plan.inputFiles.size());
    assertEquals(asStringSet(plan.inputFiles),
        asSet("file12,file13,file14,file15,file16,file17,file18,file19,file20,file21".split(",")));

  }

  class SimulatedTablet {

    private int maxFilesPerTablet;
    private ConfigurationCopy config;

    int nextFile = 0;
    Map<StoredTabletFile,DataFileValue> files = new HashMap<>();

    long totalRead = 0;
    long added = 0;

    SimulatedTablet(int maxFilesToCompact, int maxFilesPertablet) {
      this.maxFilesPerTablet = maxFilesPertablet;

      config = new ConfigurationCopy(DefaultConfiguration.getInstance());
      config.set(Property.TSERV_MAJC_THREAD_MAXOPEN, maxFilesToCompact + "");
    }

    void addFiles(int num, int size, int entries) {
      for (int i = 0; i < num; i++) {
        String name =
            "hdfs://nn1/accumulo/tables/5/t-0001/I" + String.format("%06d", nextFile) + ".rf";
        nextFile++;

        files.put(new StoredTabletFile(name), new DataFileValue(size, entries));
        added += size;
      }
    }

    long compact(MajorCompactionReason reason) {
      TestCompactionRequest request = new TestCompactionRequest(
          new KeyExtent(TableId.of("0"), (Text) null, null), reason, files, config);
      request.setMaxFilesPerTablet(maxFilesPerTablet);

      DefaultCompactionStrategy s = new DefaultCompactionStrategy();

      if (s.shouldCompact(request)) {
        CompactionPlan plan = s.getCompactionPlan(request);

        long totalSize = 0;
        long totalEntries = 0;

        for (StoredTabletFile fr : plan.inputFiles) {
          DataFileValue dfv = files.remove(fr);

          totalSize += dfv.getSize();
          totalEntries += dfv.getNumEntries();

          totalRead += dfv.getSize();
        }

        String name =
            "hdfs://nn1/accumulo/tables/5/t-0001/C" + String.format("%06d", nextFile) + ".rf";
        nextFile++;

        files.put(new StoredTabletFile(name), new DataFileValue(totalSize, totalEntries));

        return totalSize;

      } else {
        return 0;
      }
    }

    long getTotalRead() {
      return totalRead;
    }

    public long getTotalAdded() {
      return added;
    }

    void print() {
      List<Entry<StoredTabletFile,DataFileValue>> entries = new ArrayList<>(files.entrySet());

      entries.sort((e1, e2) -> Long.compare(e2.getValue().getSize(), e1.getValue().getSize()));

      for (Entry<StoredTabletFile,DataFileValue> entry : entries) {

        System.out.printf("%s %,d %,d\n", entry.getKey().getFileName(), entry.getValue().getSize(),
            entry.getValue().getNumEntries());
      }
    }

    public int getNumFiles() {
      return files.size();
    }
  }

  @Test
  public void simulationTest() throws Exception {
    for (int n = 1; n < 10; n++) {
      LongSummaryStatistics lss = new LongSummaryStatistics();
      SimulatedTablet simuTablet = new SimulatedTablet(10, 15);

      for (int i = 0; i < 1000; i++) {
        simuTablet.addFiles(n, 1000, 10);

        simuTablet.compact(MajorCompactionReason.NORMAL);

        lss.accept(simuTablet.getNumFiles());
      }

      while (simuTablet.compact(MajorCompactionReason.NORMAL) > 0) {
        lss.accept(simuTablet.getNumFiles());
      }

      assertTrue(simuTablet.getTotalRead() < 6 * simuTablet.getTotalAdded());
      assertTrue(lss.getAverage() < (n >= 8 ? 15 : 7));
    }
  }
}

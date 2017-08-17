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
package org.apache.accumulo.server.client;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class BulkImporterTest {

  static final SortedSet<KeyExtent> fakeMetaData = new TreeSet<>();
  static final Table.ID tableId = Table.ID.of("1");

  static {
    fakeMetaData.add(new KeyExtent(tableId, new Text("a"), null));
    for (String part : new String[] {"b", "bm", "c", "cm", "d", "dm", "e", "em", "f", "g", "h", "i", "j", "k", "l"}) {
      fakeMetaData.add(new KeyExtent(tableId, new Text(part), fakeMetaData.last().getEndRow()));
    }
    fakeMetaData.add(new KeyExtent(tableId, null, fakeMetaData.last().getEndRow()));
  }

  class MockTabletLocator extends TabletLocator {
    int invalidated = 0;

    @Override
    public TabletLocation locateTablet(ClientContext context, Text row, boolean skipRow, boolean retry) throws AccumuloException, AccumuloSecurityException,
        TableNotFoundException {
      return new TabletLocation(fakeMetaData.tailSet(new KeyExtent(tableId, row, null)).first(), "localhost", "1");
    }

    @Override
    public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations, Map<String,TabletServerMutations<T>> binnedMutations,
        List<T> failures) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      throw new NotImplementedException();
    }

    @Override
    public List<Range> binRanges(ClientContext context, List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException,
        AccumuloSecurityException, TableNotFoundException {
      throw new NotImplementedException();
    }

    @Override
    public void invalidateCache(KeyExtent failedExtent) {
      invalidated++;
    }

    @Override
    public void invalidateCache(Collection<KeyExtent> keySet) {
      throw new NotImplementedException();
    }

    @Override
    public void invalidateCache() {
      throw new NotImplementedException();
    }

    @Override
    public void invalidateCache(Instance instance, String server) {
      throw new NotImplementedException();
    }
  }

  @Test
  public void testFindOverlappingTablets() throws Exception {
    MockTabletLocator locator = new MockTabletLocator();
    FileSystem fs = FileSystem.getLocal(CachedConfiguration.getInstance());
    ClientContext context = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();
    EasyMock.replay(context);
    String file = "target/testFile.rf";
    fs.delete(new Path(file), true);
    FileSKVWriter writer = FileOperations.getInstance().newWriterBuilder().forFile(file, fs, fs.getConf()).withTableConfiguration(context.getConfiguration())
        .build();
    writer.startDefaultLocalityGroup();
    Value empty = new Value(new byte[] {});
    writer.append(new Key("a", "cf", "cq"), empty);
    writer.append(new Key("a", "cf", "cq1"), empty);
    writer.append(new Key("a", "cf", "cq2"), empty);
    writer.append(new Key("a", "cf", "cq3"), empty);
    writer.append(new Key("a", "cf", "cq4"), empty);
    writer.append(new Key("a", "cf", "cq5"), empty);
    writer.append(new Key("d", "cf", "cq"), empty);
    writer.append(new Key("d", "cf", "cq1"), empty);
    writer.append(new Key("d", "cf", "cq2"), empty);
    writer.append(new Key("d", "cf", "cq3"), empty);
    writer.append(new Key("d", "cf", "cq4"), empty);
    writer.append(new Key("d", "cf", "cq5"), empty);
    writer.append(new Key("dd", "cf", "cq1"), empty);
    writer.append(new Key("ichabod", "cf", "cq"), empty);
    writer.append(new Key("icky", "cf", "cq1"), empty);
    writer.append(new Key("iffy", "cf", "cq2"), empty);
    writer.append(new Key("internal", "cf", "cq3"), empty);
    writer.append(new Key("is", "cf", "cq4"), empty);
    writer.append(new Key("iterator", "cf", "cq5"), empty);
    writer.append(new Key("xyzzy", "cf", "cq"), empty);
    writer.close();
    VolumeManager vm = VolumeManagerImpl.get(context.getConfiguration());
    List<TabletLocation> overlaps = BulkImporter.findOverlappingTablets(context, vm, locator, new Path(file));
    Assert.assertEquals(5, overlaps.size());
    Collections.sort(overlaps);
    Assert.assertEquals(new KeyExtent(tableId, new Text("a"), null), overlaps.get(0).tablet_extent);
    Assert.assertEquals(new KeyExtent(tableId, new Text("d"), new Text("cm")), overlaps.get(1).tablet_extent);
    Assert.assertEquals(new KeyExtent(tableId, new Text("dm"), new Text("d")), overlaps.get(2).tablet_extent);
    Assert.assertEquals(new KeyExtent(tableId, new Text("j"), new Text("i")), overlaps.get(3).tablet_extent);
    Assert.assertEquals(new KeyExtent(tableId, null, new Text("l")), overlaps.get(4).tablet_extent);

    List<TabletLocation> overlaps2 = BulkImporter.findOverlappingTablets(context, vm, locator, new Path(file), new KeyExtent(tableId, new Text("h"), new Text(
        "b")));
    Assert.assertEquals(3, overlaps2.size());
    Assert.assertEquals(new KeyExtent(tableId, new Text("d"), new Text("cm")), overlaps2.get(0).tablet_extent);
    Assert.assertEquals(new KeyExtent(tableId, new Text("dm"), new Text("d")), overlaps2.get(1).tablet_extent);
    Assert.assertEquals(new KeyExtent(tableId, new Text("j"), new Text("i")), overlaps2.get(2).tablet_extent);
    Assert.assertEquals(locator.invalidated, 1);
  }

  @Test
  public void testSequentialTablets() throws Exception {
    // ACCUMULO-3967 make sure that the startRow we compute in BulkImporter is actually giving
    // a correct startRow so that findOverlappingTablets works as intended.

    // 1;2;1
    KeyExtent extent = new KeyExtent(Table.ID.of("1"), new Text("2"), new Text("1"));
    Assert.assertEquals(new Text("1\0"), BulkImporter.getStartRowForExtent(extent));

    // 1;2<
    extent = new KeyExtent(Table.ID.of("1"), new Text("2"), null);
    Assert.assertEquals(null, BulkImporter.getStartRowForExtent(extent));

    // 1<<
    extent = new KeyExtent(Table.ID.of("1"), null, null);
    Assert.assertEquals(null, BulkImporter.getStartRowForExtent(extent));

    // 1;8;7777777
    extent = new KeyExtent(Table.ID.of("1"), new Text("8"), new Text("7777777"));
    Assert.assertEquals(new Text("7777777\0"), BulkImporter.getStartRowForExtent(extent));
  }
}

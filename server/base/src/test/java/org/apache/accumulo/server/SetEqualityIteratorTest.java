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
package org.apache.accumulo.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.server.metadata.iterators.SetEqualityIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SetEqualityIteratorTest {

  private SetEqualityIterator setEqualityIterator;
  private SetEqualityIterator setEqualityIteratorNoFiles;
  private SetEqualityIterator setEqualityIteratorOneFile;
  private SortedMapIterator sortedMapIterator;
  private SortedMapIterator sortedMapIteratorNoFiles;
  private SortedMapIterator sortedMapIteratorOneFile;

  private KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

  private StoredTabletFile file1 =
      new ReferencedTabletFile(new Path("dfs://nn1/acc/tables/1/t-0001/sf1.rf")).insert();
  private StoredTabletFile file2 =
      new ReferencedTabletFile(new Path("dfs://nn1/acc/tables/1/t-0001/sf2.rf")).insert();
  private StoredTabletFile file3 =
      new ReferencedTabletFile(new Path("dfs://nn1/acc/tables/1/t-0001/sf3.rf")).insert();

  @BeforeEach
  public void setUp() throws IOException {

    // Create tablet metadata with no files
    TabletMetadata tmNoFiles = TabletMetadata.builder(extent).putFlushId(7).build();

    // Create tablet metadata with one file
    StoredTabletFile singleFile =
        new ReferencedTabletFile(new Path("dfs://nn1/acc/tables/1/t-0001/sf1.rf")).insert();
    TabletMetadata tmOneFile = TabletMetadata.builder(extent)
        .putFile(singleFile, new DataFileValue(100, 50)).putFlushId(8).build();

    // Create tablet metadata with multiple files
    TabletMetadata tmMultipleFiles = TabletMetadata.builder(extent)
        .putFile(file1, new DataFileValue(0, 0)).putFile(file2, new DataFileValue(555, 23))
        .putFile(file3, new DataFileValue(234, 13)).putFlushId(6).build();

    var extent2 = new KeyExtent(extent.tableId(), null, extent.endRow());
    // create another tablet metadata using extent2 w/ diff files and add it to sortedMap. This
    // will add another row to the test data which ensures that iterator does not go to another row.
    StoredTabletFile file4 =
        new ReferencedTabletFile(new Path("dfs://nn1/acc/tables/1/t-0002/sf4.rf")).insert();
    StoredTabletFile file5 =
        new ReferencedTabletFile(new Path("dfs://nn1/acc/tables/1/t-0002/sf5.rf")).insert();
    StoredTabletFile file6 =
        new ReferencedTabletFile(new Path("dfs://nn1/acc/tables/1/t-0002/sf6.rf")).insert();
    TabletMetadata tmMultipleFiles2 = TabletMetadata.builder(extent2)
        .putFile(file4, new DataFileValue(100, 50)).putFile(file5, new DataFileValue(200, 75))
        .putFile(file6, new DataFileValue(300, 100)).putFlushId(7).build();

    // Convert TabletMetadata to a SortedMap
    SortedMap<Key,Value> sortedMapNoFiles = new TreeMap<>(tmNoFiles.getKeyValues());
    SortedMap<Key,Value> sortedMapOneFile = new TreeMap<>(tmOneFile.getKeyValues());
    SortedMap<Key,Value> sortedMap = new TreeMap<>(tmMultipleFiles.getKeyValues());
    SortedMap<Key,Value> sortedMap2 = new TreeMap<>(tmMultipleFiles2.getKeyValues());
    // Add the second tablet metadata to the sortedMap
    sortedMap.putAll(sortedMap2);

    // Create a SortedMapIterator using the SortedMap
    sortedMapIterator = new SortedMapIterator(sortedMap);
    sortedMapIteratorNoFiles = new SortedMapIterator(sortedMapNoFiles);
    sortedMapIteratorOneFile = new SortedMapIterator(sortedMapOneFile);

    // Set the SortedMapIterator as the source for SetEqualityIterator
    setEqualityIterator = new SetEqualityIterator();
    setEqualityIterator.init(sortedMapIterator, Collections.emptyMap(), null);
    setEqualityIteratorNoFiles = new SetEqualityIterator();
    setEqualityIteratorNoFiles.init(sortedMapIteratorNoFiles, Collections.emptyMap(), null);
    setEqualityIteratorOneFile = new SetEqualityIterator();
    setEqualityIteratorOneFile.init(sortedMapIteratorOneFile, Collections.emptyMap(), null);
  }

  @Test
  public void testTabletWithNoFiles() throws IOException {
    // Creating a test range
    Text tabletRow = new Text(extent.toMetaRow());
    Text family = MetadataSchema.TabletsSection.DataFileColumnFamily.NAME;

    Range range = Range.exact(tabletRow, family);

    // Invoking the seek method
    setEqualityIteratorNoFiles.seek(range, Collections.emptyList(), false);

    // Asserting the result
    assertEquals(new Key(tabletRow, family), setEqualityIteratorNoFiles.getTopKey());
    // The iterator should produce a value that is equal to the expected value on the condition
    var condition = SetEqualityIterator.createCondition(Collections.emptySet(),
        storedTabletFile -> ((StoredTabletFile) storedTabletFile).getMetadata().getBytes(UTF_8),
        family);
    assertArrayEquals(condition.getValue().toArray(),
        setEqualityIteratorNoFiles.getTopValue().get());
  }

  @Test
  public void testTabletWithOneFile() throws IOException {
    // Creating a test range
    Text tabletRow = new Text(extent.toMetaRow());
    Text family = MetadataSchema.TabletsSection.DataFileColumnFamily.NAME;

    Range range = Range.exact(tabletRow, family);

    // Invoking the seek method
    setEqualityIteratorOneFile.seek(range, Collections.emptyList(), false);

    // Asserting the result
    assertEquals(new Key(tabletRow, family), setEqualityIteratorOneFile.getTopKey());
    // The iterator should produce a value that is equal to the expected value on the condition
    var condition = SetEqualityIterator.createCondition(Collections.singleton(file1),
        storedTabletFile -> storedTabletFile.getMetadata().getBytes(UTF_8), family);
    assertArrayEquals(condition.getValue().toArray(),
        setEqualityIteratorOneFile.getTopValue().get());
  }

  @Test
  public void testTabletWithMultipleFiles() throws IOException {
    // Creating a test range
    Text tabletRow = new Text(extent.toMetaRow());
    Text family = MetadataSchema.TabletsSection.DataFileColumnFamily.NAME;

    Range range = Range.exact(tabletRow, family);

    // Invoking the seek method
    setEqualityIterator.seek(range, Collections.emptyList(), false);

    // Asserting the result
    assertEquals(new Key(tabletRow, family), setEqualityIterator.getTopKey());
    // The iterator should produce a value that is equal to the expected value on the condition
    var condition = SetEqualityIterator.createCondition(Set.of(file1, file2, file3),
        storedTabletFile -> storedTabletFile.getMetadata().getBytes(UTF_8), family);
    assertArrayEquals(condition.getValue().toArray(), setEqualityIterator.getTopValue().get());

  }

}

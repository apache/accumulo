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
import java.util.*;

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
  private SortedMapIterator sortedMapIterator;

  private KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

  private StoredTabletFile file1 =
      new ReferencedTabletFile(new Path("dfs://nn1/acc/tables/1/t-0001/sf1.rf")).insert();
  private StoredTabletFile file2 =
      new ReferencedTabletFile(new Path("dfs://nn1/acc/tables/1/t-0001/sf2.rf")).insert();
  private StoredTabletFile file3 =
      new ReferencedTabletFile(new Path("dfs://nn1/acc/tables/1/t-0001/sf3.rf")).insert();

  @BeforeEach
  public void setUp() throws IOException {

    TabletMetadata tm = TabletMetadata.builder(extent).putFile(file1, new DataFileValue(0, 0))
        .putFile(file2, new DataFileValue(555, 23)).putFile(file3, new DataFileValue(234, 13))
        .putFlushId(6).build();

    // Convert TabletMetadata to a SortedMap
    SortedMap<Key,Value> sortedMap = new TreeMap<>(tm.getKeyValues());

    var extent2 = new KeyExtent(extent.tableId(), null, extent.endRow());
    // TODO create another tablet metadata using extent2 w/ diff files and add it to sortedMap. This
    // will add another row to the test data which ensures that iterator does not go to another row.

    // Create a SortedMapIterator using the SortedMap
    sortedMapIterator = new SortedMapIterator(sortedMap);

    // Set the SortedMapIterator as the source for SetEqualityIterator
    setEqualityIterator = new SetEqualityIterator();
    setEqualityIterator.init(sortedMapIterator, Collections.emptyMap(), null);
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

  // TODO test tablets with no files and tablet with one file.. will probably need to refactor code
  // in setUp so that it can be used to create tablet metadata with different sets of files and then
  // create an iterator over that tablet

}

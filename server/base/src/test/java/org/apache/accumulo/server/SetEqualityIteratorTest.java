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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.server.metadata.iterators.SetEqualityIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class SetEqualityIteratorTest {

  private SetEqualityIterator setEqualityIterator;
  private SortedMapIterator sortedMapIterator;

  @BeforeEach
  public void setUp() throws IOException {
    // Create a TServerInstance for server1
    TServerInstance ser1 = new TServerInstance(HostAndPort.fromParts("server1", 8555), "s001");

    // Create a TabletMetadata with sample data
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

    // Create a mutation to represent the assignment of a tablet to a server
    Mutation assignmentMutation = TabletColumnFamily.createPrevRowMutation(extent);
    assignmentMutation.at().family(FutureLocationColumnFamily.NAME).qualifier(ser1.getSession())
        .put(ser1.getHostPort());

    // Add the mutation to the key values using putFile with ReferencedTabletFile
    ReferencedTabletFile mutationFile = new ReferencedTabletFile(new Path("mutation_file"));
    TabletMetadata tm =
        TabletMetadata.builder(extent).putFile(mutationFile, new DataFileValue(0, 0))
            .putFile(new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/sf1.rf"))
                .insert(), new DataFileValue(555, 23))
            .putFile(new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/sf2.rf"))
                .insert(), new DataFileValue(234, 13))
            .putFlushId(6).build();

    // Convert TabletMetadata to a SortedMap
    SortedMap<Key,Value> sortedMap = new TreeMap<>(tm.getKeyValues());

    // Create a SortedMapIterator using the SortedMap
    sortedMapIterator = new SortedMapIterator(sortedMap);

    // Set the SortedMapIterator as the source for SetEqualityIterator
    setEqualityIterator = new SetEqualityIterator();
    setEqualityIterator.init(sortedMapIterator, Collections.emptyMap(), null);
  }

  @Test
  public void testSeek() throws IOException {
    // Creating a test range
    Text tabletRow = new Text("row");
    Text family = new Text("family");
    Key startKey = new Key(tabletRow, family);
    Key endKey = new Key(tabletRow, family).followingKey(PartialKey.ROW_COLFAM);
    Range range = new Range(startKey, true, endKey, false);

    // Invoking the seek method
    setEqualityIterator.seek(range, Collections.emptyList(), false);

    // Asserting the result
    assertEquals(new Key("row1", "family"), setEqualityIterator.getTopKey());
    // assertEquals(new Value("..."), setEqualityIterator.getTopValue());
  }
}

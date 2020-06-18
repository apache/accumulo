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
package org.apache.accumulo.core.clientImpl.mapreduce;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.junit.Test;

/**
 * @deprecated since 2.0.0
 */
@Deprecated
public class BatchInputSplitTest {

  @Test
  public void testSimpleWritable() throws IOException {
    List<Range> ranges = Collections.singletonList(new Range(new Key("a"), new Key("b")));
    BatchInputSplit split =
        new BatchInputSplit("table", TableId.of("1"), ranges, new String[] {"localhost"});

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    split.write(dos);

    BatchInputSplit newSplit = new BatchInputSplit();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    newSplit.readFields(dis);

    assertEquals(split.getTableName(), newSplit.getTableName());
    assertEquals(split.getTableId(), newSplit.getTableId());
    assertEquals(split.getRanges(), newSplit.getRanges());
    assertTrue(Arrays.equals(split.getLocations(), newSplit.getLocations()));
  }

  @Test
  public void testAllFieldsWritable() throws IOException {
    List<Range> ranges = Collections.singletonList(new Range(new Key("a"), new Key("b")));
    BatchInputSplit split =
        new BatchInputSplit("table", TableId.of("1"), ranges, new String[] {"localhost"});

    Set<Pair<Text,Text>> fetchedColumns = new HashSet<>();

    fetchedColumns.add(new Pair<>(new Text("colf1"), new Text("colq1")));
    fetchedColumns.add(new Pair<>(new Text("colf2"), new Text("colq2")));

    // Fake some iterators
    ArrayList<IteratorSetting> iterators = new ArrayList<>();
    IteratorSetting setting = new IteratorSetting(50, SummingCombiner.class);
    setting.addOption("foo", "bar");
    iterators.add(setting);

    setting = new IteratorSetting(100, WholeRowIterator.class);
    setting.addOption("bar", "foo");
    iterators.add(setting);

    split.setTableName("table");
    split.setFetchedColumns(fetchedColumns);
    split.setIterators(iterators);
    split.setLogLevel(Level.WARN);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    split.write(dos);

    BatchInputSplit newSplit = new BatchInputSplit();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    newSplit.readFields(dis);

    assertEquals(split.getRanges(), newSplit.getRanges());
    assertArrayEquals(split.getLocations(), newSplit.getLocations());

    assertEquals(split.getTableName(), newSplit.getTableName());
    assertEquals(split.getFetchedColumns(), newSplit.getFetchedColumns());
    assertEquals(split.getIterators(), newSplit.getIterators());
    assertEquals(split.getLogLevel(), newSplit.getLogLevel());
  }
}

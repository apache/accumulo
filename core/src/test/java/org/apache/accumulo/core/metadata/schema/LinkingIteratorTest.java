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
package org.apache.accumulo.core.metadata.schema;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.create;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

public class LinkingIteratorTest {

  private static class IterFactory implements Function<Range,Iterator<TabletMetadata>> {
    private int count;
    private List<TabletMetadata> initial;
    private List<TabletMetadata> subsequent;

    IterFactory(List<TabletMetadata> initial, List<TabletMetadata> subsequent) {
      this.initial = initial;
      this.subsequent = subsequent;
      count = 0;
    }

    @Override
    public Iterator<TabletMetadata> apply(Range range) {
      Stream<TabletMetadata> stream = count++ == 0 ? initial.stream() : subsequent.stream();
      return stream.filter(tm -> range.contains(new Key(tm.getExtent().toMetaRow()))).iterator();
    }
  }

  private static void check(List<TabletMetadata> expected, IterFactory iterFactory, Range range) {
    List<KeyExtent> actual = new ArrayList<>();
    new LinkingIterator(iterFactory, range).forEachRemaining(tm -> actual.add(tm.getExtent()));
    assertEquals(Lists.transform(expected, TabletMetadata::getExtent), actual);
  }

  private static void check(List<TabletMetadata> expected, IterFactory iterFactory) {
    check(expected, iterFactory, new Range());
  }

  private static void check(List<TabletMetadata> expected, IterFactory iterFactory,
      TableId tableId) {
    check(expected, iterFactory, TabletsSection.getRange(tableId));
  }

  @Test
  public void testHole() {

    List<TabletMetadata> tablets1 = Arrays.asList(create("4", null, "f"), create("4", "f", "m"),
        create("4", "r", "x"), create("4", "x", null));
    List<TabletMetadata> tablets2 = Arrays.asList(create("4", null, "f"), create("4", "f", "m"),
        create("4", "m", "r"), create("4", "r", "x"), create("4", "x", null));

    check(tablets2, new IterFactory(tablets1, tablets2));
  }

  @Test
  public void testMerge() {
    // test for case when a tablet is merged away
    List<TabletMetadata> tablets1 = Arrays.asList(create("4", null, "f"), create("4", "f", "m"),
        create("4", "f", "r"), create("4", "x", null));
    List<TabletMetadata> tablets2 = Arrays.asList(create("4", null, "f"), create("4", "f", "r"),
        create("4", "r", "x"), create("4", "x", null));

    LinkingIterator li = new LinkingIterator(new IterFactory(tablets1, tablets2), new Range());
    assertThrows(TabletDeletedException.class, () -> {
      while (li.hasNext()) {
        li.next();
      }
    });
  }

  @Test
  public void testBadTableTransition1() {
    // test when last tablet in table does not have null end row
    List<TabletMetadata> tablets1 =
        Arrays.asList(create("4", null, "f"), create("4", "f", "m"), create("5", null, null));
    List<TabletMetadata> tablets2 = Arrays.asList(create("4", null, "f"), create("4", "f", "m"),
        create("4", "m", null), create("5", null, null));

    check(tablets2, new IterFactory(tablets1, tablets2));
  }

  @Test
  public void testBadTableTransition2() {
    // test when first tablet in table does not have null prev end row
    List<TabletMetadata> tablets1 =
        Arrays.asList(create("4", null, "f"), create("4", "f", null), create("5", "h", null));
    List<TabletMetadata> tablets2 = Arrays.asList(create("4", null, "f"), create("4", "f", null),
        create("5", null, "h"), create("5", "h", null));

    check(tablets2, new IterFactory(tablets1, tablets2));
  }

  @Test
  public void testFirstTabletSplits() {
    // check when first tablet has a prev end row that points to a non existent tablet. This could
    // be caused by the first table splitting concurrently with a metadata scan of the first tablet.
    List<TabletMetadata> tablets1 = Arrays.asList(create("4", "f", "m"), create("4", "m", null));
    List<TabletMetadata> tablets2 =
        Arrays.asList(create("4", null, "f"), create("4", "f", "m"), create("4", "m", null));

    check(tablets2, new IterFactory(tablets1, tablets2), TableId.of("4"));
    check(tablets2, new IterFactory(tablets1, tablets2),
        new KeyExtent(TableId.of("4"), null, new Text("e")).toMetaRange());

    // following should not care about missing tablet
    check(tablets1, new IterFactory(tablets1, tablets2),
        new KeyExtent(TableId.of("4"), null, new Text("g")).toMetaRange());
    check(tablets1, new IterFactory(tablets1, tablets2),
        new KeyExtent(TableId.of("4"), null, new Text("f")).toMetaRange());
  }

  @Test
  public void testIncompleteTable() {
    // the last tablet in a table should have a null end row. Ensure the code detects when this does
    // not happen.
    List<TabletMetadata> tablets1 = Arrays.asList(create("4", null, "f"), create("4", "f", "m"));

    LinkingIterator li = new LinkingIterator(new IterFactory(tablets1, tablets1),
        TabletsSection.getRange(TableId.of("4")));

    assertThrows(IllegalStateException.class, () -> {
      while (li.hasNext()) {
        li.next();
      }
    });
  }

  @Test
  public void testIncompleteTableWithRange() {
    // because the scan range does not got to end of table, this should not care about missing
    // tablets at end of table.
    List<TabletMetadata> tablets1 = Arrays.asList(create("4", null, "f"), create("4", "f", "m"));
    check(tablets1, new IterFactory(tablets1, tablets1),
        new KeyExtent(TableId.of("4"), new Text("r"), new Text("e")).toMetaRange());
  }
}

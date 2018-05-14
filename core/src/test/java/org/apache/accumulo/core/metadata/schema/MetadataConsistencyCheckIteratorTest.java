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

package org.apache.accumulo.core.metadata.schema;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.create;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class MetadataConsistencyCheckIteratorTest {

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
      return stream.filter(tm -> range.contains(new Key(tm.getExtent().getMetadataEntry())))
          .iterator();
    }
  }

  private static void check(List<TabletMetadata> expected, IterFactory iterFactory) {
    List<KeyExtent> actual = new ArrayList<>();
    new MetadataConsistencyCheckIterator(iterFactory, new Range())
        .forEachRemaining(tm -> actual.add(tm.getExtent()));
    Assert.assertEquals(Lists.transform(expected, TabletMetadata::getExtent), actual);
  }

  @Test
  public void testHole() {

    List<TabletMetadata> tablets1 = Arrays.asList(create("4", null, "f"), create("4", "f", "m"),
        create("4", "r", "x"), create("4", "x", null));
    List<TabletMetadata> tablets2 = Arrays.asList(create("4", null, "f"), create("4", "f", "m"),
        create("4", "m", "r"), create("4", "r", "x"), create("4", "x", null));

    check(tablets2, new IterFactory(tablets1, tablets2));
  }

  @Test(expected = TabletDeletedException.class)
  public void testMerge() {
    // test for case when a tablet is merged away
    List<TabletMetadata> tablets1 = Arrays.asList(create("4", null, "f"), create("4", "f", "m"),
        create("4", "f", "r"), create("4", "x", null));
    List<TabletMetadata> tablets2 = Arrays.asList(create("4", null, "f"), create("4", "f", "r"),
        create("4", "r", "x"), create("4", "x", null));

    MetadataConsistencyCheckIterator mdcci = new MetadataConsistencyCheckIterator(
        new IterFactory(tablets1, tablets2), new Range());

    while (mdcci.hasNext()) {
      mdcci.next();
    }
  }

  @Test
  public void testBadTableTransition1() {
    // test when last tablet in table does not have null end row
    List<TabletMetadata> tablets1 = Arrays.asList(create("4", null, "f"), create("4", "f", "m"),
        create("5", null, null));
    List<TabletMetadata> tablets2 = Arrays.asList(create("4", null, "f"), create("4", "f", "m"),
        create("4", "m", null), create("5", null, null));

    check(tablets2, new IterFactory(tablets1, tablets2));
  }

  @Test
  public void testBadTableTransition2() {
    // test when first tablet in table does not have null prev end row
    List<TabletMetadata> tablets1 = Arrays.asList(create("4", null, "f"), create("4", "f", null),
        create("5", "h", null));
    List<TabletMetadata> tablets2 = Arrays.asList(create("4", null, "f"), create("4", "f", null),
        create("5", null, "h"), create("5", "h", null));

    check(tablets2, new IterFactory(tablets1, tablets2));
  }
}

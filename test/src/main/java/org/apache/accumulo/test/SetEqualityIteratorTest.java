package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.accumulo.server.metadata.iterators.SetEqualityIterator;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class SetEqualityIteratorTest {

  private SetEqualityIterator setEqualityIterator;
  private SortedMapIterator sortedMapIterator;

  @Before
  public void setUp() throws IOException {
    // Create a SortedMap with sample data
    SortedMap<Key,Value> sortedMap = new TreeMap<>();
    sortedMap.put(new Key("row1", "family", "qualifier"), new Value("value1".getBytes()));
    sortedMap.put(new Key("row2", "family", "qualifier"), new Value("value2".getBytes()));

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

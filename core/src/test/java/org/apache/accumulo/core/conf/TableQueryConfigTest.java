package org.apache.accumulo.core.conf;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class TableQueryConfigTest {
  
  private static final String TEST_TABLE = "TEST_TABLE";
  private TableQueryConfig tableQueryConfig;
  
  @Before
  public void setUp() {
    tableQueryConfig = new TableQueryConfig(TEST_TABLE);
  }
  
  @Test
  public void testSerialization_OnlyTable() throws IOException {
    byte[] serialized = serialize(tableQueryConfig);
    TableQueryConfig actualConfig = deserialize(serialized);
    
    assertEquals(tableQueryConfig, actualConfig);
  }
  
  @Test
  public void testSerialization_ranges() throws IOException {
    List<Range> ranges = new ArrayList<Range>();
    ranges.add(new Range("a", "b"));
    ranges.add(new Range("c", "d"));
    tableQueryConfig.setRanges(ranges);
    
    byte[] serialized = serialize(tableQueryConfig);
    TableQueryConfig actualConfig = deserialize(serialized);
    
    assertEquals(ranges, actualConfig.getRanges());
  }
  
  @Test
  public void testSerialization_columns() throws IOException {
    Set<Pair<Text,Text>> columns = new HashSet<Pair<Text,Text>>();
    columns.add(new Pair<Text,Text>(new Text("cf1"), new Text("cq1")));
    columns.add(new Pair<Text,Text>(new Text("cf2"), null));
    tableQueryConfig.fetchColumns(columns);
    
    byte[] serialized = serialize(tableQueryConfig);
    TableQueryConfig actualConfig = deserialize(serialized);
    
    assertEquals(actualConfig.getFetchedColumns(), columns);
  }
  
  @Test
  public void testSerialization_iterators() throws IOException {
    List<IteratorSetting> settings = new ArrayList<IteratorSetting>();
    settings.add(new IteratorSetting(50, "iter", "iterclass"));
    settings.add(new IteratorSetting(55, "iter2", "iterclass2"));
    tableQueryConfig.setIterators(settings);
    byte[] serialized = serialize(tableQueryConfig);
    TableQueryConfig actualConfig = deserialize(serialized);
    assertEquals(actualConfig.getIterators(), settings);
    
  }
  
  private byte[] serialize(TableQueryConfig tableQueryConfig) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    tableQueryConfig.write(new DataOutputStream(baos));
    baos.close();
    return baos.toByteArray();
  }
  
  private TableQueryConfig deserialize(byte[] bytes) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    TableQueryConfig actualConfig = new TableQueryConfig(new DataInputStream(bais));
    bais.close();
    return actualConfig;
  }
}

package org.apache.accumulo.server.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class ServerMutationTest {
  
  @Test
  public void test() throws Exception {
    ServerMutation m = new ServerMutation(new Text("r1"));
    m.put(new Text("cf1"), new Text("cq1"), new Value("v1".getBytes()));
    m.put(new Text("cf2"), new Text("cq2"), 56, new Value("v2".getBytes()));
    m.setSystemTimestamp(42);
    
    List<ColumnUpdate> updates = m.getUpdates();
    
    assertEquals(2, updates.size());
    
    assertEquals("r1", new String(m.getRow()));
    ColumnUpdate cu = updates.get(0);
    
    assertEquals("cf1", new String(cu.getColumnFamily()));
    assertEquals("cq1", new String(cu.getColumnQualifier()));
    assertEquals("", new String(cu.getColumnVisibility()));
    assertFalse(cu.hasTimestamp());
    assertEquals(42l, cu.getTimestamp());
    
    ServerMutation m2 = new ServerMutation();
    ReflectionUtils.copy(CachedConfiguration.getInstance(), m, m2);
    
    updates = m2.getUpdates();
    
    assertEquals(2, updates.size());
    assertEquals("r1", new String(m2.getRow()));
    
    cu = updates.get(0);
    assertEquals("cf1", new String(cu.getColumnFamily()));
    assertEquals("cq1", new String(cu.getColumnQualifier()));
    assertFalse(cu.hasTimestamp());
    assertEquals(42l, cu.getTimestamp());
    
    cu = updates.get(1);
    
    assertEquals("r1", new String(m2.getRow()));
    assertEquals("cf2", new String(cu.getColumnFamily()));
    assertEquals("cq2", new String(cu.getColumnQualifier()));
    assertTrue(cu.hasTimestamp());
    assertEquals(56, cu.getTimestamp());
    
    
  }
  
}

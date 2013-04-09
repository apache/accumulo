package org.apache.accumulo.server.tabletserver;

import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;


public class CheckTabletMetadataTest {
  
  private static Key nk(String row, ColumnFQ cfq) {
    return new Key(new Text(row), cfq.getColumnFamily(), cfq.getColumnQualifier());
  }
  
  private static Key nk(String row, Text cf, String cq) {
    return new Key(row, cf.toString(), cq);
  }

  private static void put(TreeMap<Key,Value> tabletMeta, String row, ColumnFQ cfq, byte[] val) {
    Key k = new Key(new Text(row), cfq.getColumnFamily(), cfq.getColumnQualifier());
    tabletMeta.put(k, new Value(val));
  }
  
  private static void put(TreeMap<Key,Value> tabletMeta, String row, Text cf, String cq, String val) {
    Key k = new Key(new Text(row), cf, new Text(cq));
    tabletMeta.put(k, new Value(val.getBytes()));
  }

  private static void assertFail(TreeMap<Key,Value> tabletMeta, KeyExtent ke, TServerInstance tsi) {
    try {
      Assert.assertNull(TabletServer.checkTabletMetadata(ke, tsi, tabletMeta, ke.getMetadataEntry()));
    } catch (Exception e) {

    }
  }

  private static void assertFail(TreeMap<Key,Value> tabletMeta, KeyExtent ke, TServerInstance tsi, Key keyToDelete) {
    TreeMap<Key,Value> copy = new TreeMap<Key,Value>(tabletMeta);
    Assert.assertNotNull(copy.remove(keyToDelete));
    try {
      Assert.assertNull(TabletServer.checkTabletMetadata(ke, tsi, copy, ke.getMetadataEntry()));
    } catch (Exception e) {

    }
  }

  @Test
  public void testBadTabletMetadata() throws Exception {
    
    KeyExtent ke = new KeyExtent(new Text("1"), null, null);
    
    TreeMap<Key,Value> tabletMeta = new TreeMap<Key,Value>();
    
    put(tabletMeta, "1<", Constants.METADATA_PREV_ROW_COLUMN, KeyExtent.encodePrevEndRow(null).get());
    put(tabletMeta, "1<", Constants.METADATA_DIRECTORY_COLUMN, "/t1".getBytes());
    put(tabletMeta, "1<", Constants.METADATA_TIME_COLUMN, "M0".getBytes());
    put(tabletMeta, "1<", Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY, "4", "127.0.0.1:9997");
    
    TServerInstance tsi = new TServerInstance("127.0.0.1:9997", 4);
    
    Assert.assertNotNull(TabletServer.checkTabletMetadata(ke, tsi, tabletMeta, ke.getMetadataEntry()));
    
    assertFail(tabletMeta, ke, new TServerInstance("127.0.0.1:9998", 4));
    assertFail(tabletMeta, ke, new TServerInstance("127.0.0.1:9998", 5));
    assertFail(tabletMeta, ke, new TServerInstance("127.0.0.1:9997", 5));
    assertFail(tabletMeta, ke, new TServerInstance("127.0.0.2:9997", 4));
    assertFail(tabletMeta, ke, new TServerInstance("127.0.0.2:9997", 5));
    
    assertFail(tabletMeta, new KeyExtent(new Text("1"), null, new Text("m")), tsi);
    
    assertFail(tabletMeta, new KeyExtent(new Text("1"), new Text("r"), new Text("m")), tsi);
    
    assertFail(tabletMeta, ke, tsi, nk("1<", Constants.METADATA_PREV_ROW_COLUMN));

    assertFail(tabletMeta, ke, tsi, nk("1<", Constants.METADATA_DIRECTORY_COLUMN));
    
    assertFail(tabletMeta, ke, tsi, nk("1<", Constants.METADATA_TIME_COLUMN));
    
    assertFail(tabletMeta, ke, tsi, nk("1<", Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY, "4"));
    
    TreeMap<Key,Value> copy = new TreeMap<Key,Value>(tabletMeta);
    put(copy, "1<", Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY, "4", "127.0.0.1:9997");
    assertFail(copy, ke, tsi);
    assertFail(copy, ke, tsi, nk("1<", Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY, "4"));
    
    copy = new TreeMap<Key,Value>(tabletMeta);
    put(copy, "1<", Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY, "5", "127.0.0.1:9998");
    assertFail(copy, ke, tsi);
    put(copy, "1<", Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY, "6", "127.0.0.1:9999");
    assertFail(copy, ke, tsi);
    
    copy = new TreeMap<Key,Value>(tabletMeta);
    put(copy, "1<", Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY, "5", "127.0.0.1:9998");
    assertFail(copy, ke, tsi);
    
    assertFail(new TreeMap<Key,Value>(), ke, tsi);

  }
}

package org.apache.accumulo.test.functional;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class AdvertiseAndBindIT extends AccumuloClusterHarness {
  
  private static final List<Map<String,String>> PROPERTIES = new LinkedList<>();
  static {
    // no overrides
    PROPERTIES.add(Map.of());
    // bind address only
    PROPERTIES.add(Map.of(Property.RPC_PROCESS_BIND_ADDRESS.getKey(), "127.0.0.1"));
    // advertise address only
    PROPERTIES.add(Map.of(Property.RPC_PROCESS_ADVERTISE_ADDRESS.getKey(), "127.0.0.1"));
    // advertise and bind address
    PROPERTIES.add(Map.of(Property.RPC_PROCESS_BIND_ADDRESS.getKey(), "127.0.0.1",
        Property.RPC_PROCESS_ADVERTISE_ADDRESS.getKey(), "127.0.0.1"));
  }
  private static int configurationCounter = 0;

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
  }
  
  @ParameterizedTest
  @EnumSource
  public void testSimple() {
    
  }
  
  
}

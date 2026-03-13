package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class MultipleManagerMetricsIT extends ConfigurableMacBase {

  private static final Logger LOG = LoggerFactory.getLogger(MultipleManagerMetricsIT.class);
  private static TestStatsDSink sink;

  private final AtomicReference<String> currentPrimary = new AtomicReference<>();
  private final AtomicBoolean stopThread = new AtomicBoolean(false);
  
  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Set this lower so that locks timeout faster
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "5s");
    cfg.setProperty(Property.MANAGER_STARTUP_MANAGER_AVAIL_MIN_COUNT, "2");
    cfg.setProperty(Property.MANAGER_STARTUP_MANAGER_AVAIL_MAX_WAIT, "10s");
    cfg.getClusterServerConfiguration().setNumManagers(2);
    
    // Tell the server processes to use a StatsDMeterRegistry and the simple logging registry
    // that will be configured to push all metrics to the sink we started.
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_CACHE_METRICS_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, TestStatsDRegistryFactory.class.getName());
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);
    
    super.configure(cfg, hadoopCoreSite);
  }
  
  @BeforeAll
  public static void before() throws Exception {
    sink = new TestStatsDSink();
  }

  @AfterAll
  public static void after() throws Exception {
    sink.close();
  }
  
  private String getPrimary() {
    Set<ServerId> primaries = getCluster().getServerContext().instanceOperations().getServers(ServerId.Type.MANAGER);
    assertEquals(1, primaries.size());
    ServerId primary = primaries.iterator().next();
    return primary.toHostPortString();
  }
  
//  private Set<String> getManagers() {
//    Set<String> results = new HashSet<>();
//    Set<ServiceLockPath> managers = getCluster().getServerContext().getServerPaths().getAssistantManagers(AddressSelector.all(), true);
//    managers.forEach(m -> results.add(m.getServer()));
//    return results;
//  }
  
  private void killPrimary() throws KeeperException, InterruptedException {
    ServiceLockPath slp = getCluster().getServerContext().getServerPaths().getManager(true);
    getCluster().getServerContext().getZooSession().asReaderWriter().recursiveDelete(slp.toString(), NodeMissingPolicy.SKIP);
  }
  
  @Test
  public void testPrimaryManagerTagChanges() throws Exception {
    
    AtomicReference<Throwable> threadError = new AtomicReference<>();
    
    Thread t = new Thread(() -> {
      List<String> statsDMetrics;
      while (!stopThread.get()) {
        if (!(statsDMetrics = sink.getLines()).isEmpty()) {
          List<TestStatsDSink.Metric> metrics = statsDMetrics.stream().filter(line -> line.startsWith("accumulo"))
              .map(TestStatsDSink::parseStatsDMetric)
              .filter(m -> m.getTags().get(MetricsInfo.PROCESS_NAME_TAG_KEY).equals(ServerId.Type.MANAGER.name()))
              .filter(m -> m.getTags().get(Manager.PRIMARY_TAG_KEY).equals("true"))
              .toList();
          if (metrics.size() > 0) {
            TestStatsDSink.Metric last = metrics.get(metrics.size() - 1);
            String host = last.getTags().get(MetricsInfo.HOST_TAG_KEY);
            String port = last.getTags().get(MetricsInfo.PORT_TAG_KEY);
            if (host != null && port != null) {
              currentPrimary.set(HostAndPort.fromParts(host, Integer.parseInt(port)).toString());
            }
          }
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          threadError.set(e);
        }
      }
    });
    
    t.start();
    try {
      String previousPrimary = null;
      
      for (int i = 0; i < 5; i++) {
        // Wait for primary lock to be acquired
        Wait.waitFor(() -> getCluster().getServerContext().getServerPaths().getManager(true) != null, 60_000);
        
        String primary = getPrimary();
        LOG.info("Primary manager at: {}", primary);
        
        // Check that the primary address switched
        if (previousPrimary != null) {
          assertNotEquals(previousPrimary, primary);
          getCluster().start();
        }
        
        // Wait for primary metric to match current primary
        final String tmp = primary;
        Wait.waitFor(() -> currentPrimary.get() != null && currentPrimary.get().equals(tmp), 60_000);
        
        // Kill the Primary
        previousPrimary = primary;
        killPrimary();
      }
    } finally {
      stopThread.set(true);
      t.join();
    }
    assertNull(threadError.get());    
  }

}

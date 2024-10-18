package org.apache.accumulo.test.metrics;

import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.apache.accumulo.core.metrics.thrift.MetricService.Client;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class MetricsThriftRPCIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL, "1s");
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_USER_TAGS, "tag1=value1,tag2=value2");
    cfg.setProperty(Property.GENERAL_MICROMETER_CACHE_METRICS_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED, "true");
  }
  
  @Test
  public void testRpc() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      ClientContext cc = (ClientContext) client;
      Set<ServerId> managers = client.instanceOperations().getServers(ServerId.Type.MANAGER);
      for (ServerId server : managers) {
        Client metricsClient = ThriftUtil.getClient(ThriftClientTypes.METRICS, HostAndPort.fromParts(server.getHost(), server.getPort()), cc);
        try {
        MetricResponse response = metricsClient.getMetrics(TraceUtil.traceInfo(), getCluster().getServerContext().rpcCreds());
        } finally {
          ThriftUtil.returnClient(metricsClient, cc);
        }
      }
      Set<ServerId> compactors = client.instanceOperations().getServers(ServerId.Type.COMPACTOR);
      for (ServerId server : compactors) {
        Client metricsClient = ThriftUtil.getClient(ThriftClientTypes.METRICS, HostAndPort.fromParts(server.getHost(), server.getPort()), cc);
        try {
        MetricResponse response = metricsClient.getMetrics(TraceUtil.traceInfo(), getCluster().getServerContext().rpcCreds());
        } finally {
          ThriftUtil.returnClient(metricsClient, cc);
        }
      }
      Set<ServerId> sservers = client.instanceOperations().getServers(ServerId.Type.SCAN_SERVER);
      for (ServerId server : sservers) {
        Client metricsClient = ThriftUtil.getClient(ThriftClientTypes.METRICS, HostAndPort.fromParts(server.getHost(), server.getPort()), cc);
        try {
        MetricResponse response = metricsClient.getMetrics(TraceUtil.traceInfo(), getCluster().getServerContext().rpcCreds());
        } finally {
          ThriftUtil.returnClient(metricsClient, cc);
        }
      }
      Set<ServerId> tservers = client.instanceOperations().getServers(ServerId.Type.TABLET_SERVER);
      for (ServerId server : tservers) {
        Client metricsClient = ThriftUtil.getClient(ThriftClientTypes.METRICS, HostAndPort.fromParts(server.getHost(), server.getPort()), cc);
        try {
        MetricResponse response = metricsClient.getMetrics(TraceUtil.traceInfo(), getCluster().getServerContext().rpcCreds());
        } finally {
          ThriftUtil.returnClient(metricsClient, cc);
        }
      }
    }
  }

}

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
package org.apache.accumulo.test.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.apache.accumulo.core.metrics.thrift.MetricService.Client;
import org.apache.accumulo.core.metrics.thrift.MetricSource;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class MetricsThriftRpcIT extends ConfigurableMacBase {

  private static final Logger log = LoggerFactory.getLogger(MetricsThriftRpcIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL, "1s");
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_USER_TAGS, "tag1=value1,tag2=value2");
    cfg.setProperty(Property.GENERAL_MICROMETER_CACHE_METRICS_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED, "true");
    cfg.getClusterServerConfiguration().setNumDefaultCompactors(1);
    cfg.getClusterServerConfiguration().setNumDefaultScanServers(1);
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
    cfg.getClusterServerConfiguration().addCompactorResourceGroup("CTEST", 3);
    cfg.getClusterServerConfiguration().addScanServerResourceGroup("STEST", 2);
    cfg.getClusterServerConfiguration().addTabletServerResourceGroup("TTEST", 1);
  }

  private int handleMetrics(final MetricResponse response) {
    if (response.getMetricsSize() == 0) {
      log.info("type: {}, host: {}, group: {} has no metrics", response.getServerType(),
          response.getServer(), response.getResourceGroup());
      return response.getMetricsSize();
    }
    for (final ByteBuffer binary : response.getMetrics()) {
      FMetric fm = FMetric.getRootAsFMetric(binary);
      final List<String> tags = new ArrayList<>(fm.tagsLength());
      for (int i = 0; i < fm.tagsLength(); i++) {
        FTag t = fm.tags(i);
        tags.add(t.key() + " = " + t.value());
      }
      log.info(
          "type: {}, host: {}, group: {}, time: {}, name: {}, type: {}, tags: {}, dval: {}, ival: {}, lval: {}",
          response.getServerType(), response.getServer(), response.getResourceGroup(),
          response.getTimestamp(), fm.name(), fm.type(), tags, fm.dvalue(), fm.ivalue(),
          fm.lvalue());
    }
    return response.getMetricsSize();
  }

  @Test
  public void testRpc() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      MetricsIT.doWorkToGenerateMetrics(client, getClass());
      // Wait one minute for server to collect some metrics
      Thread.sleep(60_000);
      ClientContext cc = (ClientContext) client;
      Set<ServerId> managers = client.instanceOperations().getServers(ServerId.Type.MANAGER);
      assertEquals(1, managers.size());
      ServerId managerServer = managers.iterator().next();
      Client metricsClient = ThriftUtil.getClient(ThriftClientTypes.METRICS,
          HostAndPort.fromParts(managerServer.getHost(), managerServer.getPort()), cc);
      try {
        MetricResponse response = metricsClient.getMetrics(TraceUtil.traceInfo(),
            getCluster().getServerContext().rpcCreds());
        assertEquals(managerServer.getResourceGroup(), response.getResourceGroup());
        assertEquals(MetricSource.MANAGER, response.getServerType());
        assertTrue(handleMetrics(response) > 0);
      } finally {
        ThriftUtil.returnClient(metricsClient, cc);
      }
      ServiceLockPath zgcPath = cc.getServerPaths().getGarbageCollector(true);
      assertNotNull(zgcPath, "Garbage Collector not found in ZooKeeper");
      Optional<ServiceLockData> sld = cc.getZooCache().getLockData(zgcPath);
      assertTrue(sld.isPresent(), "Garbage Collector ZooKeeper lock data not found");
      String location = sld.orElseThrow().getAddressString(ThriftService.GC);
      HostAndPort hp = HostAndPort.fromString(location);
      metricsClient = ThriftUtil.getClient(ThriftClientTypes.METRICS, hp, cc);
      try {
        MetricResponse response = metricsClient.getMetrics(TraceUtil.traceInfo(),
            getCluster().getServerContext().rpcCreds());
        assertEquals(Constants.DEFAULT_RESOURCE_GROUP_NAME, response.getResourceGroup());
        assertEquals(MetricSource.GARBAGE_COLLECTOR, response.getServerType());
        assertTrue(handleMetrics(response) > 0);
      } finally {
        ThriftUtil.returnClient(metricsClient, cc);
      }

      Set<ServerId> compactors = client.instanceOperations().getServers(ServerId.Type.COMPACTOR);
      assertEquals(4, compactors.size());
      for (ServerId server : compactors) {
        metricsClient = ThriftUtil.getClient(ThriftClientTypes.METRICS,
            HostAndPort.fromParts(server.getHost(), server.getPort()), cc);
        try {
          MetricResponse response = metricsClient.getMetrics(TraceUtil.traceInfo(),
              getCluster().getServerContext().rpcCreds());
          assertEquals(server.getResourceGroup(), response.getResourceGroup());
          assertEquals(MetricSource.COMPACTOR, response.getServerType());
          assertTrue(handleMetrics(response) > 0);
        } finally {
          ThriftUtil.returnClient(metricsClient, cc);
        }
      }
      Set<ServerId> sservers = client.instanceOperations().getServers(ServerId.Type.SCAN_SERVER);
      assertEquals(3, sservers.size());
      for (ServerId server : sservers) {
        metricsClient = ThriftUtil.getClient(ThriftClientTypes.METRICS,
            HostAndPort.fromParts(server.getHost(), server.getPort()), cc);
        try {
          MetricResponse response = metricsClient.getMetrics(TraceUtil.traceInfo(),
              getCluster().getServerContext().rpcCreds());
          assertEquals(server.getResourceGroup(), response.getResourceGroup());
          assertEquals(MetricSource.SCAN_SERVER, response.getServerType());
          assertTrue(handleMetrics(response) > 0);
        } finally {
          ThriftUtil.returnClient(metricsClient, cc);
        }
      }
      Set<ServerId> tservers = client.instanceOperations().getServers(ServerId.Type.TABLET_SERVER);
      assertEquals(2, tservers.size());
      for (ServerId server : tservers) {
        metricsClient = ThriftUtil.getClient(ThriftClientTypes.METRICS,
            HostAndPort.fromParts(server.getHost(), server.getPort()), cc);
        try {
          MetricResponse response = metricsClient.getMetrics(TraceUtil.traceInfo(),
              getCluster().getServerContext().rpcCreds());
          assertEquals(server.getResourceGroup(), response.getResourceGroup());
          assertEquals(MetricSource.TABLET_SERVER, response.getServerType());
          assertTrue(handleMetrics(response) > 0);
        } finally {
          ThriftUtil.returnClient(metricsClient, cc);
        }
      }
    }
  }

}

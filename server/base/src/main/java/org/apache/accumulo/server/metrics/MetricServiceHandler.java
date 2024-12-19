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
package org.apache.accumulo.server.metrics;

import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.apache.accumulo.core.metrics.thrift.MetricService;
import org.apache.accumulo.core.metrics.thrift.MetricSource;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.server.ServerContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;
import com.google.flatbuffers.FlatBufferBuilder;

import io.micrometer.core.instrument.Metrics;

public class MetricServiceHandler implements MetricService.Iface {

  private static final Logger LOG = LoggerFactory.getLogger(MetricServiceHandler.class);

  private final MetricSource type;
  private final String resourceGroup;
  private final ServerContext ctx;

  private String host;

  public MetricServiceHandler(MetricSource source, String resourceGroup, ServerContext ctx) {
    this.type = source;
    this.resourceGroup = resourceGroup;
    this.ctx = ctx;
  }

  public void setHost(HostAndPort host) {
    this.host = host.toString();
  }

  @Override
  public MetricResponse getMetrics(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {

    if (!(ctx.getSecurityOperation().isSystemUser(credentials)
        && ctx.getSecurityOperation().authenticateUser(credentials, credentials))) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    final FlatBufferBuilder builder = new FlatBufferBuilder(1024);
    final MetricResponseWrapper response = new MetricResponseWrapper(builder);

    if (host == null) {
      LOG.error("Host is not set, this should have been done after starting the Thrift service.");
      return response;
    }

    response.setServerType(type);
    response.setServer(host);
    response.setResourceGroup(resourceGroup);
    response.setTimestamp(System.currentTimeMillis());

    if (ctx.getMetricsInfo().isMetricsEnabled()) {
      Metrics.globalRegistry.getMeters().forEach(m -> {
        if (m.getId().getName().startsWith("accumulo.")) {
          m.match(response::writeMeter, response::writeMeter, response::writeTimer,
              response::writeDistributionSummary, response::writeLongTaskTimer,
              response::writeMeter, response::writeMeter, response::writeFunctionTimer,
              response::writeMeter);
        }
      });
    }
    builder.clear();
    return response;
  }

}

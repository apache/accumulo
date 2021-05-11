/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test;

import java.io.IOException;
import java.net.UnknownHostException;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.coordinator.ExternalCompactionMetrics;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.thrift.TException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class TestCompactionCoordinator extends CompactionCoordinator
    implements org.apache.accumulo.core.compaction.thrift.CompactionCoordinator.Iface {

  private static final Logger LOG = LoggerFactory.getLogger(TestCompactionCoordinator.class);
  private static final Gson GSON = new Gson();

  private final ExternalCompactionMetrics metrics = new ExternalCompactionMetrics();
  private Server metricServer = null;

  protected TestCompactionCoordinator(ServerOpts opts, String[] args) {
    super(opts, args);
  }

  private Server startHttpMetricServer() throws Exception {
    int port = 9099;
    String hostname = getHostname();
    Server metricServer = new Server(new QueuedThreadPool(4, 1));
    ServerConnector c = new ServerConnector(metricServer);
    c.setHost(hostname);
    c.setPort(port);
    metricServer.addConnector(c);
    ContextHandlerCollection handlers = new ContextHandlerCollection();
    metricServer.setHandler(handlers);
    ContextHandler metricContext = new ContextHandler("/metrics");
    metricContext.setHandler(new AbstractHandler() {
      @Override
      public void handle(String target, Request baseRequest, HttpServletRequest request,
          HttpServletResponse response) throws IOException, ServletException {
        baseRequest.setHandled(true);
        response.setStatus(200);
        response.setContentType("application/json");
        metrics.setRunning(RUNNING.size());
        response.getWriter().print(GSON.toJson(metrics));
      }
    });
    handlers.addHandler(metricContext);

    ContextHandler detailsContext = new ContextHandler("/details");
    detailsContext.setHandler(new AbstractHandler() {
      @Override
      public void handle(String target, Request baseRequest, HttpServletRequest request,
          HttpServletResponse response) throws IOException, ServletException {
        baseRequest.setHandled(true);
        response.setStatus(200);
        response.setContentType("application/json");
        response.getWriter().print(GSON.toJson(RUNNING));
      }
    });
    handlers.addHandler(detailsContext);

    metricServer.start();
    LOG.info("Metrics HTTP server listening on {}:{}", hostname, port);
    return metricServer;
  }

  @Override
  protected ServerAddress startCoordinatorClientService() throws UnknownHostException {
    try {
      return super.startCoordinatorClientService();
    } finally {
      try {
        metricServer = startHttpMetricServer();
      } catch (Exception e1) {
        throw new RuntimeException("Failed to start metric http server", e1);
      }
    }
  }

  @Override
  public TExternalCompactionJob getCompactionJob(TInfo tinfo, TCredentials credentials,
      String queueName, String compactorAddress, String externalCompactionId) throws TException {
    TExternalCompactionJob job = super.getCompactionJob(tinfo, credentials, queueName,
        compactorAddress, externalCompactionId);
    if (null != job && null != job.getExternalCompactionId()) {
      metrics.incrementStarted();
    }
    return job;
  }

  @Override
  public void compactionCompleted(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TKeyExtent textent, TCompactionStats stats) throws TException {
    try {
      super.compactionCompleted(tinfo, credentials, externalCompactionId, textent, stats);
    } finally {
      metrics.incrementCompleted();
    }
  }

  @Override
  public void compactionFailed(TInfo tinfo, TCredentials credentials, String externalCompactionId,
      TKeyExtent extent) throws TException {
    try {
      super.compactionFailed(tinfo, credentials, externalCompactionId, extent);
    } finally {
      metrics.incrementFailed();
    }
  }

  @Override
  public void close() {
    super.close();
    if (null != metricServer) {
      try {
        metricServer.stop();
      } catch (Exception e) {
        LOG.error("Error stopping metric server", e);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    try (TestCompactionCoordinator coordinator =
        new TestCompactionCoordinator(new ServerOpts(), args)) {
      coordinator.runServer();
    }
  }

}

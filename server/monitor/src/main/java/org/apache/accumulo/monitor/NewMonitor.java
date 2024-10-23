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
package org.apache.accumulo.monitor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;

import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;
import io.javalin.security.RouteRole;

public class NewMonitor {

  public static class MetricResponseSerializer extends JsonSerializer<MetricResponse> {

    @Override
    public void serialize(MetricResponse value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeNumberField("timestamp", value.getTimestamp());
      gen.writeStringField("serverType", value.getServerType().toString());
      gen.writeStringField("resourceGroup", value.getResourceGroup());
      gen.writeStringField("host", value.getServer());
      gen.writeArrayFieldStart("metrics");
      for (final ByteBuffer binary : value.getMetrics()) {
        FMetric fm = FMetric.getRootAsFMetric(binary);
        gen.writeStartObject();
        gen.writeStringField("name", fm.name());
        gen.writeStringField("type", fm.type());
        gen.writeArrayFieldStart("tags");
        for (int i = 0; i < fm.tagsLength(); i++) {
          FTag t = fm.tags(i);
          gen.writeStartObject();
          gen.writeStringField(t.key(), t.value());
          gen.writeEndObject();
        }
        gen.writeEndArray();
        // Write the non-zero number as the value
        if (fm.lvalue() > 0) {
          gen.writeNumberField("value", fm.lvalue());
        } else if (fm.ivalue() > 0) {
          gen.writeNumberField("value", fm.ivalue());
        } else if (fm.dvalue() > 0.0d) {
          gen.writeNumberField("value", fm.dvalue());
        } else {
          gen.writeNumberField("value", 0);
        }
        gen.writeEndObject();
      }
      gen.writeEndArray();
      gen.writeEndObject();
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(NewMonitor.class);

  private final ServerContext ctx;
  private final String hostname;

  public NewMonitor(ServerContext ctx, String hostname) {
    this.ctx = ctx;
    this.hostname = hostname;
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET",
      justification = "TODO Replace before merging")
  public void start() throws IOException {

    MetricsFetcher metrics = new MetricsFetcher(ctx);

    Threads.createThread("Metric Fetcher Thread", metrics).start();

    // Find a free socket
    ServerSocket ss = new ServerSocket();
    ss.setReuseAddress(true);
    ss.bind(new InetSocketAddress(hostname, 0));
    ss.close();
    final int port = ss.getLocalPort();

    Javalin.create(config -> {
      config.bundledPlugins.enableDevLogging();
      config.bundledPlugins.enableRouteOverview("/routes", new RouteRole[] {});
      config.jsonMapper(new JavalinJackson().updateMapper(mapper -> {
        SimpleModule module = new SimpleModule();
        module.addSerializer(MetricResponse.class, new MetricResponseSerializer());
        mapper.registerModule(module);
      }));
    }).get("/metrics", ctx -> ctx.json(metrics.getAll()))
        .get("/metrics/groups", ctx -> ctx.json(metrics.getResourceGroups()))
        .get("/metrics/manager", ctx -> ctx.json(metrics.getManager()))
        .get("/metrics/gc", ctx -> ctx.json(metrics.getGarbageCollector()))
        .get("/metrics/compactors/{group}",
            ctx -> ctx.json(metrics.getCompactors(ctx.pathParam("group"))))
        .get("/metrics/sservers/{group}",
            ctx -> ctx.json(metrics.getSServers(ctx.pathParam("group"))))
        .get("/metrics/tservers/{group}",
            ctx -> ctx.json(metrics.getTServers(ctx.pathParam("group"))))
        .start(hostname, port);

    LOG.info("New Monitor listening on {}", port);
  }
}

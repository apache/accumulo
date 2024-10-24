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
import java.util.EnumSet;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
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

  private static final EnumSet<Property> requireForSecure =
      EnumSet.of(Property.MONITOR_SSL_KEYSTORE, Property.MONITOR_SSL_KEYSTOREPASS,
          Property.MONITOR_SSL_TRUSTSTORE, Property.MONITOR_SSL_TRUSTSTOREPASS);

  public static final int NEW_MONITOR_PORT = 43331;

  private final ServerContext ctx;
  private final String hostname;
  private final boolean secure;

  public NewMonitor(ServerContext ctx, String hostname) {
    this.ctx = ctx;
    this.hostname = hostname;
    secure = requireForSecure.stream().map(ctx.getConfiguration()::get)
        .allMatch(s -> s != null && !s.isEmpty());
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET",
      justification = "TODO Replace before merging")
  public void start() throws IOException {

    MetricsFetcher metrics = new MetricsFetcher(ctx);

    Threads.createThread("Metric Fetcher Thread", metrics).start();

    // Find a free socket
    ServerSocket ss = new ServerSocket();
    ss.setReuseAddress(true);
    ss.bind(new InetSocketAddress(hostname, NEW_MONITOR_PORT));
    ss.close();
    final int httpPort = ss.getLocalPort();

    Javalin.create(config -> {
      config.bundledPlugins.enableDevLogging();
      config.bundledPlugins.enableRouteOverview("/routes", new RouteRole[] {});
      config.jsonMapper(new JavalinJackson().updateMapper(mapper -> {
        SimpleModule module = new SimpleModule();
        module.addSerializer(MetricResponse.class, new MetricResponseSerializer());
        mapper.registerModule(module);
      }));

      if (secure) {
        LOG.debug("Configuring Jetty to use TLS");

        final AccumuloConfiguration conf = ctx.getConfiguration();

        final SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        // If the key password is the same as the keystore password, we don't
        // have to explicitly set it. Thus, if the user doesn't provide a key
        // password, don't set anything.
        final String keyPass = conf.get(Property.MONITOR_SSL_KEYPASS);
        if (!Property.MONITOR_SSL_KEYPASS.getDefaultValue().equals(keyPass)) {
          sslContextFactory.setKeyManagerPassword(keyPass);
        }
        sslContextFactory.setKeyStorePath(conf.get(Property.MONITOR_SSL_KEYSTORE));
        sslContextFactory.setKeyStorePassword(conf.get(Property.MONITOR_SSL_KEYSTOREPASS));
        sslContextFactory.setKeyStoreType(conf.get(Property.MONITOR_SSL_KEYSTORETYPE));
        sslContextFactory.setTrustStorePath(conf.get(Property.MONITOR_SSL_TRUSTSTORE));
        sslContextFactory.setTrustStorePassword(conf.get(Property.MONITOR_SSL_TRUSTSTOREPASS));
        sslContextFactory.setTrustStoreType(conf.get(Property.MONITOR_SSL_TRUSTSTORETYPE));

        final String includedCiphers = conf.get(Property.MONITOR_SSL_INCLUDE_CIPHERS);
        if (!Property.MONITOR_SSL_INCLUDE_CIPHERS.getDefaultValue().equals(includedCiphers)) {
          sslContextFactory.setIncludeCipherSuites(includedCiphers.split(","));
        }

        final String excludedCiphers = conf.get(Property.MONITOR_SSL_EXCLUDE_CIPHERS);
        if (!Property.MONITOR_SSL_EXCLUDE_CIPHERS.getDefaultValue().equals(excludedCiphers)) {
          sslContextFactory.setExcludeCipherSuites(excludedCiphers.split(","));
        }

        final String includeProtocols = conf.get(Property.MONITOR_SSL_INCLUDE_PROTOCOLS);
        if (includeProtocols != null && !includeProtocols.isEmpty()) {
          sslContextFactory.setIncludeProtocols(includeProtocols.split(","));
        }

        HttpConnectionFactory httpFactory = new HttpConnectionFactory();
        SslConnectionFactory sslFactory =
            new SslConnectionFactory(sslContextFactory, httpFactory.getProtocol());
        config.jetty.addConnector((s, httpConfig) -> {
          ServerConnector conn = new ServerConnector(s, sslFactory, httpFactory);
          conn.setHost(hostname);
          conn.setPort(httpPort);
          return conn;
        });
      }

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
        .start(hostname, httpPort);

    LOG.info("New Monitor listening on port: {}", httpPort);
  }
}

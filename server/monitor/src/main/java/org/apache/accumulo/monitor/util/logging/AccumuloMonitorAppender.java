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
package org.apache.accumulo.monitor.util.logging;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.monitor.rest.logs.LogResource;
import org.apache.accumulo.monitor.rest.logs.SingleLogEvent;
import org.apache.accumulo.server.ServerContext;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;

import com.google.gson.Gson;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A custom Log4j2 Appender which follows the registered location of the active Accumulo monitor
 * service, and forwards log messages to its log4j REST endpoint at
 * {@link LogResource#append(SingleLogEvent)}.
 */
@Plugin(name = "AccumuloMonitor", category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE, printObject = true)
public class AccumuloMonitorAppender extends AbstractAppender {

  @PluginBuilderFactory
  public static <B extends Builder<B>> B newBuilder() {
    return new Builder<B>().asBuilder();
  }

  public static class Builder<B extends Builder<B>> extends AbstractAppender.Builder<B>
      implements org.apache.logging.log4j.core.util.Builder<AccumuloMonitorAppender> {

    @Override
    public AccumuloMonitorAppender build() {
      return new AccumuloMonitorAppender(getName(), getFilter(), isIgnoreExceptions(),
          getPropertyArray());
    }

  }

  private final Gson gson = new Gson();
  private final HttpClient httpClient = HttpClient.newHttpClient();
  private final Supplier<Optional<URI>> monitorLocator;

  private ServerContext context;
  private String path;
  private Pair<Long,Optional<URI>> lastResult = new Pair<>(0L, Optional.empty());

  private AccumuloMonitorAppender(final String name, final Filter filter,
      final boolean ignoreExceptions, final Property[] properties) {
    super(name, filter, null, ignoreExceptions, properties);
    final ZcStat stat = new ZcStat();
    monitorLocator = () -> {
      // lazily set up context/path
      if (context == null) {
        context = new ServerContext(SiteConfiguration.auto());
        path = context.getZooKeeperRoot() + Constants.ZMONITOR_HTTP_ADDR;
      }
      // get the current location from the cache
      byte[] loc = context.getZooCache().get(path, stat);
      Pair<Long,Optional<URI>> last = lastResult;
      if (stat.getMzxid() != last.getFirst()) {
        // only create new objects if there's a change
        last = new Pair<>(stat.getMzxid(), Optional.ofNullable(loc)
            .map(bytes -> URI.create(new String(bytes, UTF_8) + "rest/logs/append")));
        lastResult = last;
      }
      return last.getSecond();
    };
  }

  @Override
  public void append(final LogEvent event) {
    monitorLocator.get().ifPresent(uri -> {
      try {
        var pojo = new SingleLogEvent();
        pojo.timestamp = event.getTimeMillis();
        pojo.application = System.getProperty("accumulo.application", "unknown");
        pojo.logger = event.getLoggerName();
        pojo.level = event.getLevel().name();
        pojo.message = event.getMessage().getFormattedMessage();
        pojo.stacktrace = throwableToStacktrace(event.getThrown());

        String jsonEvent = gson.toJson(pojo);

        var req = HttpRequest.newBuilder(uri).POST(BodyPublishers.ofString(jsonEvent, UTF_8))
            .setHeader("Content-Type", "application/json").build();
        @SuppressWarnings("unused")
        var future = httpClient.sendAsync(req, BodyHandlers.discarding());
      } catch (final Exception e) {
        error("Unable to send HTTP in appender [" + getName() + "]", event, e);
      }
    });
  }

  @SuppressFBWarnings(value = "INFORMATION_EXPOSURE_THROUGH_AN_ERROR_MESSAGE",
      justification = "throwable is intended to be printed to output stream, to send to monitor")
  private static String throwableToStacktrace(Throwable t) {
    if (t == null) {
      return null;
    }
    StringWriter writer = new StringWriter();
    t.printStackTrace(new PrintWriter(writer));
    return writer.toString();
  }

  @Override
  public String toString() {
    return "AccumuloMonitorAppender{name=" + getName() + ", state=" + getState() + '}';
  }

}

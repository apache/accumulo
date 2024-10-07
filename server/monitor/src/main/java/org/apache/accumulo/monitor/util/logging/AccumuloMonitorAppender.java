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
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
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
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends AbstractAppender.Builder<Builder>
      implements org.apache.logging.log4j.core.util.Builder<AccumuloMonitorAppender> {

    @PluginBuilderAttribute
    private boolean async = true;

    @PluginBuilderAttribute
    private int queueSize = 1024;

    @PluginBuilderAttribute
    private int maxThreads = 2;

    public Builder setAsync(boolean async) {
      this.async = async;
      return this;
    }

    public boolean getAsync() {
      return async;
    }

    public Builder setQueueSize(int size) {
      queueSize = size;
      return this;
    }

    public int getQueueSize() {
      return queueSize;
    }

    public Builder setMaxThreads(int maxThreads) {
      this.maxThreads = maxThreads;
      return this;
    }

    public int getMaxThreads() {
      return maxThreads;
    }

    @Override
    public AccumuloMonitorAppender build() {
      return new AccumuloMonitorAppender(getName(), getFilter(), isIgnoreExceptions(),
          getPropertyArray(), getQueueSize(), getMaxThreads(), getAsync());
    }

  }

  private final Gson gson = new Gson();
  private final HttpClient httpClient;
  private final Supplier<Optional<URI>> monitorLocator;
  private final ThreadPoolExecutor executor;
  private final boolean async;
  private final int queueSize;
  private final AtomicLong appends = new AtomicLong(0);
  private final AtomicLong discards = new AtomicLong(0);
  private final AtomicLong errors = new AtomicLong(0);
  private final ConcurrentMap<Integer,AtomicLong> statusCodes = new ConcurrentSkipListMap<>();

  private ServerContext context;
  private String path;
  private Pair<Long,Optional<URI>> lastResult = new Pair<>(0L, Optional.empty());

  private AccumuloMonitorAppender(final String name, final Filter filter,
      final boolean ignoreExceptions, final Property[] properties, int queueSize, int maxThreads,
      boolean async) {
    super(name, filter, null, ignoreExceptions, properties);

    this.executor = async ? new ThreadPoolExecutor(0, maxThreads, 30, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>()) : null;
    final var builder = HttpClient.newBuilder();
    this.httpClient = (async ? builder.executor(executor) : builder).build();
    this.queueSize = queueSize;
    this.async = async;

    final var stat = new ZcStat();
    this.monitorLocator = () -> {
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

  private String getStats() {
    return "discards:" + discards.get() + " errors:" + errors.get() + " appends:" + appends.get()
        + " statusCodes:" + statusCodes;
  }

  private void processResponse(HttpResponse<?> response) {
    var statusCode = response.statusCode();
    statusCodes.computeIfAbsent(statusCode, sc -> new AtomicLong()).getAndIncrement();
    if (statusCode >= 400 && statusCode < 600) {
      error("Unable to send HTTP in appender [" + getName() + "]. Status: " + statusCode + " "
          + getStats());
    }
  }

  @Override
  public void append(final LogEvent event) {
    appends.getAndIncrement();
    monitorLocator.get().ifPresentOrElse(uri -> {
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

        if (async) {
          if (executor.getQueue().size() < queueSize) {
            httpClient.sendAsync(req, BodyHandlers.discarding()).thenAccept(this::processResponse)
                .exceptionally(e -> {
                  errors.getAndIncrement();
                  error("Unable to send HTTP in appender [" + getName() + "] " + getStats(), event,
                      e);
                  return null;
                });
          } else {
            discards.getAndIncrement();
            error("Unable to send HTTP in appender [" + getName() + "]. Queue full. " + getStats());
          }
        } else {
          processResponse(httpClient.send(req, BodyHandlers.discarding()));
        }
      } catch (final Exception e) {
        errors.getAndIncrement();
        error("Unable to send HTTP in appender [" + getName() + "] " + getStats(), event, e);
      }
    }, () -> {
      discards.getAndIncrement();
      error("Unable to send HTTP in appender [" + getName() + "]. No monitor is running. "
          + getStats());
    });
  }

  @Override
  protected boolean stop(long timeout, TimeUnit timeUnit, boolean changeLifeCycleState) {
    if (changeLifeCycleState) {
      setStopping();
    }
    if (executor != null) {
      executor.shutdown();
    }
    return super.stop(timeout, timeUnit, changeLifeCycleState);
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

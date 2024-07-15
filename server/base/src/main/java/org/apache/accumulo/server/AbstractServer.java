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
package org.apache.accumulo.server;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.metrics.ProcessMetrics;
import org.apache.accumulo.server.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;

public abstract class AbstractServer implements AutoCloseable, MetricsProducer, Runnable {

  private final ServerContext context;
  protected final String applicationName;
  private final String hostname;
  private final Logger log;
  private final ProcessMetrics processMetrics;
  protected final long idleReportingPeriodNanos;
  private volatile long idlePeriodStartNanos = 0L;

  protected AbstractServer(String appName, ServerOpts opts, String[] args) {
    this.log = LoggerFactory.getLogger(getClass().getName());
    this.applicationName = appName;
    opts.parseArgs(appName, args);
    this.hostname = Objects.requireNonNull(opts.getAddress());
    var siteConfig = opts.getSiteConfiguration();
    SecurityUtil.serverLogin(siteConfig);
    context = new ServerContext(siteConfig);
    log.info("Version " + Constants.VERSION);
    log.info("Instance " + context.getInstanceID());
    context.init(appName);
    ClassLoaderUtil.initContextFactory(context.getConfiguration());
    TraceUtil.initializeTracer(context.getConfiguration());
    if (context.getSaslParams() != null) {
      // Server-side "client" check to make sure we're logged in as a user we expect to be
      context.enforceKerberosLogin();
    }
    processMetrics = new ProcessMetrics();
    idleReportingPeriodNanos = TimeUnit.MILLISECONDS.toNanos(
        context.getConfiguration().getTimeInMillis(Property.GENERAL_IDLE_PROCESS_INTERVAL));
  }

  /**
   * Updates the idle status of the server to set the idle process metric. The server must be idle
   * for multiple calls over a specified period for the metric to reflect the idle state. If the
   * server is busy or the idle period hasn't started, it resets the idle tracking.
   *
   * @param isIdle whether the server is idle
   */
  protected void updateIdleStatus(boolean isIdle) {
    boolean shouldResetIdlePeriod = !isIdle || idleReportingPeriodNanos == 0;
    boolean isIdlePeriodNotStarted = idlePeriodStartNanos == 0;
    boolean hasExceededIdlePeriod =
        (System.nanoTime() - idlePeriodStartNanos) > idleReportingPeriodNanos;

    if (shouldResetIdlePeriod) {
      // Reset idle period and set idle metric to false
      idlePeriodStartNanos = 0;
      processMetrics.setIdleValue(false);
    } else if (isIdlePeriodNotStarted) {
      // Start tracking idle period
      idlePeriodStartNanos = System.nanoTime();
    } else if (hasExceededIdlePeriod) {
      // Set idle metric to true and reset the start of the idle period
      processMetrics.setIdleValue(true);
      idlePeriodStartNanos = 0;
    }
  }

  /**
   * Run this server in a main thread
   */
  public void runServer() throws Exception {
    final AtomicReference<Throwable> err = new AtomicReference<>();
    Thread service = new Thread(TraceUtil.wrap(this), applicationName);
    service.setUncaughtExceptionHandler((thread, exception) -> err.set(exception));
    service.start();
    service.join();
    Throwable thrown = err.get();
    if (thrown != null) {
      if (thrown instanceof Error) {
        throw (Error) thrown;
      }
      if (thrown instanceof Exception) {
        throw (Exception) thrown;
      }
      throw new RuntimeException("Weird throwable type thrown", thrown);
    }
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    // makes mocking subclasses easier
    if (processMetrics != null) {
      processMetrics.registerMetrics(registry);
    }
  }

  public String getHostname() {
    return hostname;
  }

  public ServerContext getContext() {
    return context;
  }

  public AccumuloConfiguration getConfiguration() {
    return getContext().getConfiguration();
  }

  public String getApplicationName() {
    return applicationName;
  }

  @Override
  public void close() {}

}

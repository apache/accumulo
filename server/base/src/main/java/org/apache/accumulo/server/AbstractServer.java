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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.mem.LowMemoryDetector;
import org.apache.accumulo.server.metrics.ProcessMetrics;
import org.apache.accumulo.server.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;

public abstract class AbstractServer implements AutoCloseable, MetricsProducer, Runnable {

  private final ServerContext context;
  protected final String applicationName;
  private final String hostname;
  private final String resourceGroup;
  private final ProcessMetrics processMetrics;
  protected final long idleReportingPeriodNanos;
  private volatile long idlePeriodStartNanos = 0L;

  protected AbstractServer(String appName, ConfigOpts opts, String[] args) {
    this.applicationName = appName;
    opts.parseArgs(appName, args);
    var siteConfig = opts.getSiteConfiguration();
    this.hostname = siteConfig.get(Property.GENERAL_PROCESS_BIND_ADDRESS);
    this.resourceGroup = getResourceGroupPropertyValue(siteConfig);
    SecurityUtil.serverLogin(siteConfig);
    context = new ServerContext(siteConfig);
    Logger log = LoggerFactory.getLogger(getClass());
    log.info("Version " + Constants.VERSION);
    log.info("Instance " + context.getInstanceID());
    context.init(appName);
    ClassLoaderUtil.initContextFactory(context.getConfiguration());
    TraceUtil.initializeTracer(context.getConfiguration());
    if (context.getSaslParams() != null) {
      // Server-side "client" check to make sure we're logged in as a user we expect to be
      context.enforceKerberosLogin();
    }
    final LowMemoryDetector lmd = context.getLowMemoryDetector();
    ScheduledFuture<?> future = context.getScheduledExecutor().scheduleWithFixedDelay(
        () -> lmd.logGCInfo(context.getConfiguration()), 0,
        lmd.getIntervalMillis(context.getConfiguration()), TimeUnit.MILLISECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);
    processMetrics = new ProcessMetrics(context);
    idleReportingPeriodNanos = TimeUnit.MILLISECONDS.toNanos(
        context.getConfiguration().getTimeInMillis(Property.GENERAL_IDLE_PROCESS_INTERVAL));
  }

  protected void idleProcessCheck(Supplier<Boolean> idleCondition) {
    boolean idle = idleCondition.get();
    if (!idle || idleReportingPeriodNanos == 0) {
      idlePeriodStartNanos = 0;
    } else if (idlePeriodStartNanos == 0) {
      idlePeriodStartNanos = System.nanoTime();
    } else if ((System.nanoTime() - idlePeriodStartNanos) > idleReportingPeriodNanos) {
      // increment the counter and reset the start of the idle period.
      processMetrics.incrementIdleCounter();
      idlePeriodStartNanos = 0;
    } else {
      // idleStartPeriod is non-zero, but we have not hit the idleStopPeriod yet
    }
  }

  protected String getResourceGroupPropertyValue(SiteConfiguration conf) {
    return Constants.DEFAULT_RESOURCE_GROUP_NAME;
  }

  public String getResourceGroup() {
    return resourceGroup;
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
      throw new IllegalStateException("Weird throwable type thrown", thrown);
    }
  }

  /**
   * Called
   */
  @Override
  public void registerMetrics(MeterRegistry registry) {
    // makes mocking subclasses easier
    if (processMetrics != null) {
      processMetrics.registerMetrics(registry);
    }
    getContext().setMeterRegistry(registry);
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

  @Override
  public void close() {
    MetricsUtil.close();
  }

}

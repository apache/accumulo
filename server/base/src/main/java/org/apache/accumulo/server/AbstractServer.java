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
import java.util.function.Supplier;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.metrics.ProcessMetrics;
import org.apache.accumulo.server.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractServer implements AutoCloseable, Runnable {

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

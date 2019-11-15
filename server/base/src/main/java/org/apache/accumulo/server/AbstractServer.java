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
package org.apache.accumulo.server;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractServer implements AutoCloseable, Runnable {

  private final ServerContext context;
  private final String applicationName;
  private final String hostname;
  private final Logger log;
  private final MetricsSystem metricsSystem;

  protected AbstractServer(String appName, ServerOpts opts, String[] args) {
    this.log = LoggerFactory.getLogger(getClass().getName());
    this.applicationName = appName;
    this.hostname = Objects.requireNonNull(opts.getAddress());
    opts.parseArgs(appName, args);
    var siteConfig = opts.getSiteConfiguration();
    context = new ServerContext(siteConfig);
    SecurityUtil.serverLogin(siteConfig);
    log.info("Version " + Constants.VERSION);
    log.info("Instance " + context.getInstanceID());
    ServerUtil.init(context, appName);
    this.metricsSystem = Metrics.initSystem(getClass().getSimpleName());
    TraceUtil.enableServerTraces(hostname, appName, context.getConfiguration());
    if (context.getSaslParams() != null) {
      // Server-side "client" check to make sure we're logged in as a user we expect to be
      context.enforceKerberosLogin();
    }
  }

  /**
   * Run this server in a main thread
   */
  public void runServer() throws Exception {
    final AtomicReference<Throwable> err = new AtomicReference<>();
    Thread service = new Thread(this, applicationName);
    service.setUncaughtExceptionHandler((thread, exception) -> {
      err.set(exception);
    });
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

  public MetricsSystem getMetricsSystem() {
    return metricsSystem;
  }

  @Override
  public void close() {
    TraceUtil.disable();
  }

}

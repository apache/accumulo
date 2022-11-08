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
package org.apache.accumulo.core.classloader;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default implementation of ContextClassLoaderFactory. This implementation is subject to change
 * over time. It currently implements the legacy context class loading behavior based on Accumulo's
 * custom class loaders and commons-vfs2. In future, it may simply return the system class loader
 * for all requested contexts. This class is used internally to Accumulo only, and should not be
 * used by users in their configuration.
 */
public class DefaultContextClassLoaderFactory implements ContextClassLoaderFactory {

  private static final AtomicBoolean isInstantiated = new AtomicBoolean(false);
  private static final Logger LOG = LoggerFactory.getLogger(DefaultContextClassLoaderFactory.class);
  private static final String className = DefaultContextClassLoaderFactory.class.getName();

  @SuppressWarnings("removal")
  private static final Property VFS_CONTEXT_CLASSPATH_PROPERTY =
      Property.VFS_CONTEXT_CLASSPATH_PROPERTY;

  public DefaultContextClassLoaderFactory(final AccumuloConfiguration accConf) {
    if (!isInstantiated.compareAndSet(false, true)) {
      throw new IllegalStateException("Can only instantiate " + className + " once");
    }
    Supplier<Map<String,String>> contextConfigSupplier =
        () -> accConf.getAllPropertiesWithPrefix(VFS_CONTEXT_CLASSPATH_PROPERTY);
    setContextConfig(contextConfigSupplier);
    LOG.debug("ContextManager configuration set");
    startCleanupThread(accConf, contextConfigSupplier);
  }

  @SuppressWarnings("deprecation")
  private static void setContextConfig(Supplier<Map<String,String>> contextConfigSupplier) {
    org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader
        .setContextConfig(contextConfigSupplier);
  }

  private static void startCleanupThread(final AccumuloConfiguration conf,
      final Supplier<Map<String,String>> contextConfigSupplier) {
    ScheduledFuture<?> future = ThreadPools.getClientThreadPools((t, e) -> {
      LOG.error("context classloader cleanup thread has failed.", e);
    }).createGeneralScheduledExecutorService(conf)
        .scheduleWithFixedDelay(Threads.createNamedRunnable(className + "-cleanup", () -> {
          LOG.trace("{}-cleanup thread, properties: {}", className, conf);
          Set<String> contextsInUse = contextConfigSupplier.get().keySet().stream()
              .map(p -> p.substring(VFS_CONTEXT_CLASSPATH_PROPERTY.getKey().length()))
              .collect(Collectors.toSet());
          LOG.trace("{}-cleanup thread, contexts in use: {}", className, contextsInUse);
          removeUnusedContexts(contextsInUse);
        }), 1, 1, MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
    LOG.debug("Context cleanup timer started at 60s intervals");
  }

  @SuppressWarnings("deprecation")
  private static void removeUnusedContexts(Set<String> contextsInUse) {
    org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader
        .removeUnusedContexts(contextsInUse);
  }

  @SuppressWarnings("deprecation")
  @Override
  public ClassLoader getClassLoader(String contextName) {
    return org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader
        .getContextClassLoader(contextName);
  }

}

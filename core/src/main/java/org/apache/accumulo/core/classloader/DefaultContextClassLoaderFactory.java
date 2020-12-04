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
package org.apache.accumulo.core.classloader;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.apache.accumulo.core.util.ThreadPools;
import org.apache.accumulo.core.util.Threads;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default implementation of ContextClassLoaderFactory. This implementation is subject to change
 * over time. It currently implements the legacy context class loading behavior based on Accumulo's
 * custom class loaders and commons-vfs2. In future, it may simply return the system class loader
 * for all requested contexts. This class is used internally to Accumulo only, and should not be
 * used by users in their configuration.
 */
@SuppressWarnings({"deprecation", "removal"})
public class DefaultContextClassLoaderFactory implements ContextClassLoaderFactory {

  private static final AtomicBoolean isInstantiated = new AtomicBoolean(false);
  private static final Logger LOG = LoggerFactory.getLogger(DefaultContextClassLoaderFactory.class);
  private static final String className = DefaultContextClassLoaderFactory.class.getName();

  public DefaultContextClassLoaderFactory(final AccumuloConfiguration accConf) {
    if (!isInstantiated.compareAndSet(false, true)) {
      throw new IllegalStateException("Can only instantiate " + className + " once");
    }
    Supplier<Map<String,String>> contextConfigSupplier =
        () -> accConf.getAllPropertiesWithPrefix(Property.VFS_CONTEXT_CLASSPATH_PROPERTY);
    AccumuloVFSClassLoader.setContextConfig(contextConfigSupplier);
    LOG.debug("ContextManager configuration set");
    startCleanupThread(contextConfigSupplier);
  }

  private static void startCleanupThread(final Supplier<Map<String,String>> contextConfigSupplier) {
    final ConfigurationCopy cc = new ConfigurationCopy(contextConfigSupplier.get());
    String size = cc.get(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);
    if (null == size || size.isEmpty()) {
      cc.set(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE,
          Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE.getDefaultValue());
    }
    ThreadPools.getGeneralScheduledExecutorService(cc)
        .scheduleWithFixedDelay(Threads.createNamedRunnable(className + "-cleanup", () -> {
          LOG.trace("{}-cleanup thread, properties: {}", className, cc);
          Set<String> contextsInUse = cc
              .getAllPropertiesWithPrefixStripped(Property.VFS_CONTEXT_CLASSPATH_PROPERTY).keySet();
          LOG.trace("{}-cleanup thread, contexts in use: {}", className, contextsInUse);
          AccumuloVFSClassLoader.removeUnusedContexts(contextsInUse);
        }), 60_000, 60_000, TimeUnit.MILLISECONDS);
    LOG.debug("Context cleanup timer started at 60s intervals");
  }

  @Override
  public ClassLoader getClassLoader(String contextName) {
    return AccumuloVFSClassLoader.getContextClassLoader(contextName);
  }

}

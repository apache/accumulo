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

import java.io.IOException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Supplier;

import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.start.classloader.vfs.ContextManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated(since = "2.1.0", forRemoval = true)
public class LegacyVFSContextClassLoaderFactory implements ContextClassLoaderFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(LegacyVFSContextClassLoaderFactory.class);

  public void initialize(Supplier<Map<String,String>> contextProperties) {
    try {
      AccumuloVFSClassLoader.getContextManager()
          .setContextConfig(new ContextManager.DefaultContextsConfig() {
            @Override
            public Map<String,String> getVfsContextClasspathProperties() {
              return contextProperties.get();
            }
          });
      LOG.debug("ContextManager configuration set");
      new Timer("LegacyVFSContextClassLoaderFactory-cleanup", true)
          .scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
              try {
                AccumuloVFSClassLoader.getContextManager()
                    .removeUnusedContexts(contextProperties.get().keySet());
              } catch (IOException e) {
                LOG.warn("{}", e.getMessage(), e);
              }
            }
          }, 60000, 60000);
      LOG.debug("Context cleanup timer started at 60s intervals");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public ClassLoader getClassLoader(String contextName) throws IllegalArgumentException {
    try {
      return AccumuloVFSClassLoader.getContextManager().getClassLoader(contextName);
    } catch (IOException e) {
      throw new RuntimeException("Error getting context class loader for context: " + contextName,
          e);
    }
  }

}

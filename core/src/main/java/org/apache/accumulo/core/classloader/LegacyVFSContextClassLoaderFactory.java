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

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ClassLoaderFactory;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.start.classloader.vfs.ContextManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated(since = "2.1.0", forRemoval = true)
public class LegacyVFSContextClassLoaderFactory implements ClassLoaderFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(LegacyVFSContextClassLoaderFactory.class);
  private ClassLoaderFactoryConfiguration conf = null;

  public void initialize(ClassLoaderFactoryConfiguration conf) {
    this.conf = conf;
    try {
      AccumuloVFSClassLoader.getContextManager()
          .setContextConfig(new ContextManager.DefaultContextsConfig() {
            @Override
            public Map<String,String> getVfsContextClasspathProperties() {
              return getContextProperties();
            }
          });
      new Timer("LegacyVFSContextClassLoaderFactory-cleanup", true)
          .scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
              try {
                AccumuloVFSClassLoader.getContextManager()
                    .removeUnusedContexts(getContextProperties().keySet());
              } catch (IOException e) {
                LOG.warn("{}", e.getMessage(), e);
              }
            }
          }, 60000, 60000);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  private Map<String,String> getContextProperties() {
    return new ConfigurationCopy(this.conf.get())
        .getAllPropertiesWithPrefix(Property.VFS_CONTEXT_CLASSPATH_PROPERTY);
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

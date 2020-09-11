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
package org.apache.accumulo.core.table;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ClassLoaderFactory;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.start.classloader.vfs.ContextManager;

@Deprecated(since = "2.1.0", forRemoval=true)
public class LegacyVFSContextClassLoaderFactory implements ClassLoaderFactory {

  public void initialize(AccumuloConfiguration conf) {
    try {
      AccumuloVFSClassLoader.getContextManager()
          .setContextConfig(new ContextManager.DefaultContextsConfig() {
            @Override
            public Map<String,String> getVfsContextClasspathProperties() {
              return conf.getAllPropertiesWithPrefix(Property.VFS_CONTEXT_CLASSPATH_PROPERTY);
            }
          });
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

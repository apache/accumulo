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

import java.lang.reflect.InvocationTargetException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextClassLoaders {

  private static final Logger LOG = LoggerFactory.getLogger(ContextClassLoaders.class);

  public static final String CONTEXT_CLASS_LOADER_FACTORY = "general.context.class.loader.factory";

  private static ContextClassLoaderFactory FACTORY;
  private static AccumuloConfiguration CONF;

  /**
   * Initialize the ContextClassLoaderFactory
   *
   * @param conf
   *          AccumuloConfiguration object
   */
  public static synchronized void initialize(AccumuloConfiguration conf) throws Exception {
    if (null == CONF) {
      CONF = conf;
      LOG.info("Creating ContextClassLoaderFactory");
      var factoryName = CONF.get(Property.GENERAL_CONTEXT_CLASSLOADER_FACTORY.toString());
      if (null == factoryName || factoryName.isEmpty()) {
        LOG.info("No ClassLoaderFactory specified");
        return;
      }
      try {
        var factoryClass = Class.forName(factoryName);
        if (ContextClassLoaderFactory.class.isAssignableFrom(factoryClass)) {
          LOG.info("Creating ContextClassLoaderFactory: {}", factoryName);
          FACTORY = ((Class<? extends ContextClassLoaderFactory>) factoryClass)
              .getDeclaredConstructor().newInstance();
          FACTORY.initialize(new ConfigurationImpl(CONF));
        } else {
          throw new RuntimeException(factoryName + " does not implement ContextClassLoaderFactory");
        }
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
          | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
          | SecurityException e) {
        LOG.error(
            "Unable to load and initialize class: {}. Ensure that the jar containing the ContextClassLoaderFactory is on the classpath",
            factoryName);
        throw e;
      }
    } else {
      LOG.debug("ContextClassLoaderFactory already initialized.");
    }
  }

  /**
   * Get the ContextClassLoaderFactory
   *
   * @return the configured context classloader factory
   */
  public static ContextClassLoaderFactory getContextClassLoaderFactory() {
    return FACTORY;
  }

  public static void resetForTests() {
    CONF = null;
  }

}

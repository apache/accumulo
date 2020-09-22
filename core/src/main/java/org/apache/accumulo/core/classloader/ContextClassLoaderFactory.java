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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ClassLoaderFactory;
import org.apache.accumulo.core.spi.common.ClassLoaderFactory.ClassLoaderFactoryConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextClassLoaderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ContextClassLoaderFactory.class);

  public static final String CONTEXT_FACTORY = "general.context.factory";

  private static ClassLoaderFactory FACTORY;
  private static final Map<String,ClassLoader> CONTEXTS = new ConcurrentHashMap<>();
  private static AccumuloConfiguration CONF;

  /**
   * Initialize the ContextClassLoaderFactory
   *
   * @param conf
   *          AccumuloConfiguration object
   */
  @SuppressWarnings("unchecked")
  public static void initialize(AccumuloConfiguration conf) throws Exception {
    if (null == CONF) {
      CONF = conf;
      LOG.info("Creating context ClassLoaderFactory");
      var factoryName = CONF.get(Property.GENERAL_CONTEXT_CLASSLOADER_FACTORY);
      if (null == factoryName || factoryName.isBlank()) {
        LOG.info("No ClassLoaderFactory specified");
        return;
      }
      try {
        var factoryClass = Class.forName(factoryName);
        if (ClassLoaderFactory.class.isAssignableFrom(factoryClass)) {
          LOG.info("Creating context ClassLoaderFactory: {}", factoryName);
          FACTORY = ((Class<? extends ClassLoaderFactory>) factoryClass).getDeclaredConstructor()
              .newInstance();
          FACTORY.initialize(new ClassLoaderFactoryConfiguration() {
            @Override
            public Iterator<Entry<String,String>> get() {
              return CONF.iterator();
            }
          });
        } else {
          throw new RuntimeException(factoryName + " does not implement ClassLoaderFactory");
        }
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
          | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
          | SecurityException e) {
        LOG.error(
            "Unable to load and initialize class: {}. Ensure that the jar containing the ClassLoaderFactory is on the classpath",
            factoryName);
        throw e;
      }
    } else {
      LOG.warn("ContextClassLoaderFactory already initialized.");
    }
  }

  /**
   * Return the ClassLoader for the given contextName
   *
   * @param contextName
   *          name
   * @return ClassLoader for contextName
   * @throws RuntimeException
   *           if contextName not configured
   */
  public static ClassLoader getClassLoader(String contextName) {
    ClassLoader c = CONTEXTS.get(contextName);
    if (null == c) {
      try {
        c = FACTORY.getClassLoader(contextName);
      } catch (IllegalArgumentException e) {
        LOG.error("ClassLoaderFactory is not configured for context: {}", contextName);
        throw new RuntimeException(
            "ClassLoaderFactory is not configured for context: " + contextName);
      }
      CONTEXTS.put(contextName, c);
    }
    return c;
  }

  @Override
  public String toString() {
    return CONTEXTS.toString();
  }

  public static void resetForTests() {
    CONF = null;
  }

}

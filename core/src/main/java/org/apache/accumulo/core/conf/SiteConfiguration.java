/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.conf;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * A configuration drawn from an XML configuration file specified by the system property
 * org.apache.accumulo.config.file, which points to the file as a resource. The default value is
 * "accumulo-site.xml". <b>Note</b>: Client code should not use this class, and it may be deprecated in the
 * future.
 */
public class SiteConfiguration extends AccumuloConfiguration {
  private static final Logger log = Logger.getLogger(SiteConfiguration.class);
  
  private static AccumuloConfiguration parent = null;
  private static SiteConfiguration instance = null;
  
  private static Configuration xmlConfig;
  
  private SiteConfiguration(AccumuloConfiguration parent) {
    SiteConfiguration.parent = parent;
  }
  
  synchronized public static SiteConfiguration getInstance(AccumuloConfiguration parent) {
    if (instance == null) {
      instance = new SiteConfiguration(parent);
      ConfigSanityCheck.validate(instance);
    }
    return instance;
  }
  
  synchronized private static Configuration getXmlConfig() {
    String configFile = System.getProperty("org.apache.accumulo.config.file", "accumulo-site.xml");
    if (xmlConfig == null) {
      xmlConfig = new Configuration(false);
      
      if (SiteConfiguration.class.getClassLoader().getResource(configFile) == null)
        log.warn(configFile + " not found on classpath", new Throwable());
      else
        xmlConfig.addResource(configFile);
    }
    return xmlConfig;
  }
  
  @Override
  public String get(Property property) {
    String key = property.getKey();
    
    String value = getXmlConfig().get(key);
    
    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null)
        log.error("Using default value for " + key + " due to improperly formatted " + property.getType() + ": " + value);
      value = parent.get(property);
    }
    return value;
  }
  
  @Override
  public void getProperties(Map<String,String> props, PropertyFilter filter) {
    parent.getProperties(props, filter);

    for (Entry<String,String> entry : getXmlConfig())
      if (filter.accept(entry.getKey()))
        props.put(entry.getKey(), entry.getValue());
  }

  /**
   * method here to support testing, do not call
   */
  synchronized public static void clearInstance() {
    instance = null;
  }

  /**
   * method here to support testing, do not call
   */
  public void clear() {
    getXmlConfig().clear();
  }
  
  
  /**
   * method here to support testing, do not call
   */
  public synchronized void clearAndNull() {
    if (xmlConfig != null) {
      xmlConfig.clear();
      xmlConfig = null;
    }
  }
  
  /**
   * method here to support testing, do not call
   */
  public void set(Property property, String value) {
    set(property.getKey(), value);
  }
  
  /**
   * method here to support testing, do not call
   */
  public void set(String key, String value) {
    getXmlConfig().set(key, value);
  }
}

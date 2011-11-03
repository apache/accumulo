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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Instance;
import org.apache.log4j.Logger;

public abstract class AccumuloConfiguration implements Iterable<Entry<String,String>> {
  private static final Logger log = Logger.getLogger(AccumuloConfiguration.class);
  
  private static Map<String,TableConfiguration> tableInstances = new HashMap<String,TableConfiguration>(1);
  
  public abstract String get(Property property);
  
  public abstract Iterator<Entry<String,String>> iterator();
  
  private void checkType(Property property, PropertyType type) {
    if (!property.getType().equals(type)) {
      String msg = "Configuration method intended for type " + type + " called with a " + property.getType() + " argument";
      log.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }
  
  public long getMemoryInBytes(Property property) {
    checkType(property, PropertyType.MEMORY);
    
    String memString = get(property);
    int multiplier = 0;
    switch (memString.charAt(memString.length() - 1)) {
      case 'G':
        multiplier += 10;
      case 'M':
        multiplier += 10;
      case 'K':
        multiplier += 10;
      case 'B':
        return Long.parseLong(memString.substring(0, memString.length() - 1)) << multiplier;
      default:
        return Long.parseLong(memString);
    }
  }
  
  public long getTimeInMillis(Property property) {
    checkType(property, PropertyType.TIMEDURATION);
    
    String timeString = get(property);
    int multiplier = 1;
    switch (timeString.charAt(timeString.length() - 1)) {
      case 'd':
        multiplier *= 24;
      case 'h':
        multiplier *= 60;
      case 'm':
        multiplier *= 60;
      case 's':
        multiplier *= 1000;
        if (timeString.length() > 1 && timeString.endsWith("ms")) // millis
          // case
          return Long.parseLong(timeString.substring(0, timeString.length() - 2));
        return Long.parseLong(timeString.substring(0, timeString.length() - 1)) * multiplier;
      default:
        return Long.parseLong(timeString) * 1000;
    }
  }
  
  public boolean getBoolean(Property property) {
    checkType(property, PropertyType.BOOLEAN);
    return Boolean.parseBoolean(get(property));
  }
  
  public double getFraction(Property property) {
    checkType(property, PropertyType.FRACTION);
    
    String fractionString = get(property);
    if (fractionString.charAt(fractionString.length() - 1) == '%')
      return Double.parseDouble(fractionString.substring(0, fractionString.length() - 1)) / 100.0;
    return Double.parseDouble(fractionString);
  }
  
  public int getPort(Property property) {
    checkType(property, PropertyType.PORT);
    
    String portString = get(property);
    int port = Integer.parseInt(portString);
    if (port != 0) {
      if (port < 1024 || port > 65535) {
        log.error("Invalid port number " + port + "; Using default " + property.getDefaultValue());
        port = Integer.parseInt(property.getDefaultValue());
      }
    }
    return port;
  }
  
  public int getCount(Property property) {
    checkType(property, PropertyType.COUNT);
    
    String countString = get(property);
    return Integer.parseInt(countString);
  }
  
  public static synchronized AccumuloConfiguration getSystemConfiguration() {
    return getZooConfiguration();
  }
  
  private static synchronized ZooConfiguration getZooConfiguration() {
    return ZooConfiguration.getInstance(getSiteConfiguration());
  }
  
  public static synchronized AccumuloConfiguration getSystemConfiguration(Instance instance) {
    return getZooConfiguration(instance);
  }
  
  private static synchronized ZooConfiguration getZooConfiguration(Instance instance) {
    return ZooConfiguration.getInstance(instance, getSiteConfiguration());
  }
  
  public static synchronized SiteConfiguration getSiteConfiguration() {
    return SiteConfiguration.getInstance(getDefaultConfiguration());
  }
  
  public static synchronized DefaultConfiguration getDefaultConfiguration() {
    return DefaultConfiguration.getInstance();
  }
  
  public static TableConfiguration getTableConfiguration(String instanceId, String tableId) {
    synchronized (tableInstances) {
      TableConfiguration conf = tableInstances.get(tableId);
      if (conf == null) {
        conf = new TableConfiguration(instanceId, tableId, getZooConfiguration());
        ConfigSanityCheck.validate(conf);
        tableInstances.put(tableId, conf);
      }
      return conf;
    }
  }
  
  static void removeTableIdInstance(String tableId) {
    synchronized (tableInstances) {
      tableInstances.remove(tableId);
    }
  }
  
  static void expireAllTableObservers() {
    synchronized (tableInstances) {
      for (Entry<String,TableConfiguration> entry : tableInstances.entrySet()) {
        entry.getValue().expireAllObservers();
      }
    }
  }
}

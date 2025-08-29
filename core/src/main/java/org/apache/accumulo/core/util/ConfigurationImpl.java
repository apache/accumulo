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
package org.apache.accumulo.core.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.PropertyType;
import org.apache.accumulo.core.spi.common.ServiceEnvironment.Configuration;

/**
 * The implementation class used for providing SPI configuration without exposing internal types.
 */
public class ConfigurationImpl implements Configuration {

  private final AccumuloConfiguration acfg;
  private final AccumuloConfiguration.Deriver<Map<String,String>> tableCustomDeriver;
  private final AccumuloConfiguration.Deriver<Map<String,String>> customDeriver;

  public ConfigurationImpl(AccumuloConfiguration acfg) {
    this.acfg = acfg;
    this.customDeriver =
        acfg.newDeriver(aconf -> buildCustom(aconf, Property.GENERAL_ARBITRARY_PROP_PREFIX));
    this.tableCustomDeriver =
        acfg.newDeriver(aconf -> buildCustom(aconf, Property.TABLE_ARBITRARY_PROP_PREFIX));
  }

  @Override
  public boolean isSet(String key) {
    Property prop = Property.getPropertyByKey(key);
    if (prop != null) {
      return acfg.isPropertySet(prop);
    } else {
      return acfg.get(key) != null;
    }
  }

  @Override
  public String get(String key) {
    // Get prop to check if sensitive, also looking up by prop may be more efficient.
    Property prop = Property.getPropertyByKey(key);
    if (prop != null) {
      if (prop.isSensitive()) {
        return null;
      }
      return acfg.get(prop);
    } else {
      return acfg.get(key);
    }
  }

  @Override
  public Map<String,String> getWithPrefix(String prefix) {
    Property propertyPrefix = Property.getPropertyByKey(prefix);
    if (propertyPrefix != null && propertyPrefix.getType() == PropertyType.PREFIX) {
      return acfg.getAllPropertiesWithPrefix(propertyPrefix);
    } else {
      return StreamSupport.stream(acfg.spliterator(), false)
          .filter(prop -> prop.getKey().startsWith(prefix))
          .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }
  }

  @Override
  public Map<String,String> getCustom() {
    return customDeriver.derive();
  }

  @Override
  public String getCustom(String keySuffix) {
    return getCustom().get(keySuffix);
  }

  @Override
  public Map<String,String> getTableCustom() {
    return tableCustomDeriver.derive();
  }

  @Override
  public String getTableCustom(String keySuffix) {
    return getTableCustom().get(keySuffix);
  }

  private static Map<String,String> buildCustom(AccumuloConfiguration conf, Property customPrefix) {
    return conf.getAllPropertiesWithPrefix(customPrefix).entrySet().stream().collect(
        Collectors.toUnmodifiableMap(e -> e.getKey().substring(customPrefix.getKey().length()),
            Entry::getValue));
  }

  @Override
  public Iterator<Entry<String,String>> iterator() {
    return StreamSupport.stream(acfg.spliterator(), false)
        .filter(e -> !Property.isSensitive(e.getKey())).iterator();
  }

  @Override
  public <T> Supplier<T>
      getDerived(Function<PluginEnvironment.Configuration,T> computeDerivedValue) {
    Configuration outerConfiguration = this;
    AccumuloConfiguration.Deriver<T> deriver =
        acfg.newDeriver(entries -> computeDerivedValue.apply(outerConfiguration));
    return deriver::derive;
  }
}

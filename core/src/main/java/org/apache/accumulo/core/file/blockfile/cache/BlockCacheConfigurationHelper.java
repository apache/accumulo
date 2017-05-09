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
package org.apache.accumulo.core.file.blockfile.cache;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;

public class BlockCacheConfigurationHelper {

  private final ConfigurationCopy conf;
  private final BlockCacheFactory<?,?> factory;
  private String basePropertyName;

  public BlockCacheConfigurationHelper(CacheType type, BlockCacheFactory<?,?> factory) {
    this(new ConfigurationCopy(), type, factory);
  }

  public BlockCacheConfigurationHelper(AccumuloConfiguration conf, CacheType type, BlockCacheFactory<?,?> factory) {
    this(new ConfigurationCopy(conf), type, factory);
  }

  public BlockCacheConfigurationHelper(ConfigurationCopy conf, CacheType type, BlockCacheFactory<?,?> factory) {
    this.conf = conf;
    this.factory = factory;
    this.basePropertyName = type.getPropertyPrefix(factory.getCacheImplName());
  }

  public void switchCacheType(CacheType type) {
    this.basePropertyName = type.getPropertyPrefix(factory.getCacheImplName());
  }

  public String getFullPropertyName(String propertySuffix) {
    return this.basePropertyName + propertySuffix;
  }

  public void set(String propertySuffix, String value) {
    conf.setIfAbsent(getFullPropertyName(propertySuffix), value);
  }

  public String getValue(String propertySuffix) {
    return conf.get(getFullPropertyName(propertySuffix));
  }

  public ConfigurationCopy getConfiguration() {
    return this.conf;
  }

}

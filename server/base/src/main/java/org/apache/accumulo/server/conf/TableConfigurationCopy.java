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
package org.apache.accumulo.server.conf;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.Property;

/**
 * 
 */
public class TableConfigurationCopy extends TableConfiguration {

  private String tableId;
  private Map<String,String> conf;

  public TableConfigurationCopy(String tableId, Map<String,String> conf) {
    super(null, null, null, null);
    this.tableId = tableId;
    this.conf = conf;
  }

  @Override
  public String get(Property property) {
    String key = property.getKey();
    return conf.get(key);
  }

  @Override
  public void getProperties(Map<String,String> props, PropertyFilter filter) {
    for (Entry<String,String> entry : conf.entrySet()) {
      if (filter.accept(entry.getKey())) {
        props.put(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public String getTableId() {
    return tableId;
  }

  @Override
  public NamespaceConfiguration getNamespaceConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public NamespaceConfiguration getParentConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void invalidateCache() {
    // Nothing to invalidate as this is a static copy
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}

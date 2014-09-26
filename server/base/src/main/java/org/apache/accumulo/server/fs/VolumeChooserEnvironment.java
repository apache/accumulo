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
package org.apache.accumulo.server.fs;

import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration.PropertyFilter;
import org.apache.accumulo.server.conf.TableConfiguration;

public class VolumeChooserEnvironment {

  private TableConfiguration tableConfig;

  /**
   * @param table
   *          current TableConfiguration to use
   */
  public VolumeChooserEnvironment(TableConfiguration table) {
    tableConfig = table;
  }

  /**
   * @return the current table configuration
   */
  public TableConfiguration getTableConfiguration() {
    return tableConfig;
  }

  /**
   * @return the current tableId
   */
  public String getTableId() {
    return tableConfig.getTableId();
  }

  /**
   * @param props
   *          map to populate with properties
   * @param filter
   *          property filter (required)
   */
  public void getProperties(Map<String,String> props, PropertyFilter filter) {
    tableConfig.getProperties(props, filter);
  }

}

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
package org.apache.accumulo.core.client.admin.compaction;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.data.TableId;

/**
 * Enables dynamically overriding of per table properties used to create the output file for a
 * compaction. For example it could override the per table property for compression.
 *
 * @since 2.1.0
 */
public interface CompactionConfigurer {
  /**
   * @since 2.1.0
   */
  public interface InitParameters {
    TableId getTableId();

    Map<String,String> getOptions();

    PluginEnvironment getEnvironment();
  }

  void init(InitParameters iparams);

  /**
   * @since 2.1.0
   */
  public interface InputParameters {
    TableId getTableId();

    public Collection<CompactableFile> getInputFiles();

    PluginEnvironment getEnvironment();
  }

  /**
   * Specifies how the output file should be created for a compaction.
   *
   * @since 2.1.0
   */
  public class Overrides {
    private final Map<String,String> tablePropertyOverrides;

    public Overrides(Map<String,String> tablePropertyOverrides) {
      this.tablePropertyOverrides = Map.copyOf(tablePropertyOverrides);
    }

    /**
     * @return Table properties to override.
     */
    public Map<String,String> getOverrides() {
      return tablePropertyOverrides;
    }
  }

  Overrides override(InputParameters params);
}

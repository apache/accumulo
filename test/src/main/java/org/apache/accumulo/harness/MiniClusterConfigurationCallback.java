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
package org.apache.accumulo.harness;

import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;

/**
 * Callback interface to inject configuration into the MiniAccumuloCluster or Hadoop core-site.xml
 * file used by the MiniAccumuloCluster
 */
@FunctionalInterface
public interface MiniClusterConfigurationCallback {

  class NoCallback implements MiniClusterConfigurationCallback {

    private NoCallback() {}

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      return;
    }
  }

  MiniClusterConfigurationCallback NO_CALLBACK = new NoCallback();

  void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite);

}

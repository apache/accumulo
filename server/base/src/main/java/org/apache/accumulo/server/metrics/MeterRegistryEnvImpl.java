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
package org.apache.accumulo.server.metrics;

import static org.apache.accumulo.core.spi.metrics.MeterRegistryFactory.METRICS_PROP_SUBSTRING;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.metrics.MeterRegistryFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;

public class MeterRegistryEnvImpl implements MeterRegistryFactory.InitParameters {

  private final ServerContext context;

  public MeterRegistryEnvImpl(final ServerContext context) {
    this.context = context;
  }

  @Override
  public Map<String,String> getOptions() {
    Map<String,String> options = new HashMap<>();
    options.forEach((k, v) -> {
      if (k.startsWith(METRICS_PROP_SUBSTRING)) {
        String name = k.substring(METRICS_PROP_SUBSTRING.length());
        options.put(name, v);
      }
    });
    return options;
  }

  @Override
  public ServiceEnvironment getServiceEnv() {
    return new ServiceEnvironmentImpl(context);
  }
}

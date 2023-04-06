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
package org.apache.accumulo.server.rpc;

import org.apache.commons.lang3.StringUtils;

/**
 * The type of configured Thrift server to start. This is meant more as a developer knob to ensure
 * that appropriate Thrift servers can be constructed to make a better test on the overhead of SSL
 * or SASL.
 *
 * Both SSL and SASL don't presently work with TFramedTransport which means that the Thrift servers
 * with asynchronous support will fail with these transports. As such, we want to ensure that any
 * benchmarks against "unsecure" Accumulo use the same type of Thrift server.
 */
public enum ThriftServerType {
  CUSTOM_HS_HA("custom_hs_ha"),
  THREADPOOL("threadpool"),
  SSL("ssl"),
  SASL("sasl"),
  THREADED_SELECTOR("threaded_selector");

  private final String name;

  private ThriftServerType(String name) {
    this.name = name;
  }

  public static ThriftServerType get(String name) {
    if (StringUtils.isBlank(name)) {
      return getDefault();
    }
    return ThriftServerType.valueOf(name.trim().toUpperCase());
  }

  @Override
  public String toString() {
    return name;
  }

  public static ThriftServerType getDefault() {
    return CUSTOM_HS_HA;
  }
}

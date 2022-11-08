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
package org.apache.accumulo.core.clientImpl;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.securityImpl.thrift.TDelegationTokenConfig;
import org.junit.jupiter.api.Test;

public class DelegationTokenConfigSerializerTest {

  @Test
  public void test() {
    DelegationTokenConfig cfg = new DelegationTokenConfig();
    cfg.setTokenLifetime(8323, HOURS);

    TDelegationTokenConfig tCfg = DelegationTokenConfigSerializer.serialize(cfg);
    assertEquals(tCfg.getLifetime(), cfg.getTokenLifetime(MILLISECONDS));

    assertEquals(cfg, DelegationTokenConfigSerializer.deserialize(tCfg));
  }

}

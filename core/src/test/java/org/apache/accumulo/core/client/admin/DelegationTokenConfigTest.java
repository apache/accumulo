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
package org.apache.accumulo.core.client.admin;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class DelegationTokenConfigTest {

  @Test
  public void testTimeUnit() {
    DelegationTokenConfig config1 = new DelegationTokenConfig(),
        config2 = new DelegationTokenConfig();

    config1.setTokenLifetime(1000, MILLISECONDS);
    config2.setTokenLifetime(1, SECONDS);

    assertEquals(config1.getTokenLifetime(MILLISECONDS), config2.getTokenLifetime(MILLISECONDS));
    assertEquals(config1, config2);
    assertEquals(config1.hashCode(), config2.hashCode());
  }

  @Test
  public void testNoTimeout() {
    DelegationTokenConfig config = new DelegationTokenConfig();

    config.setTokenLifetime(0, MILLISECONDS);

    assertEquals(0, config.getTokenLifetime(MILLISECONDS));

  }

  @Test
  public void testInvalidLifetime() {
    assertThrows(IllegalArgumentException.class,
        () -> new DelegationTokenConfig().setTokenLifetime(-1, DAYS));
  }

  @Test
  public void testSetInvalidTimeUnit() {
    assertThrows(NullPointerException.class,
        () -> new DelegationTokenConfig().setTokenLifetime(5, null));
  }

  @Test
  public void testGetInvalidTimeUnit() {
    assertThrows(NullPointerException.class,
        () -> new DelegationTokenConfig().getTokenLifetime(null));
  }
}

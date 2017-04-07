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
package org.apache.accumulo.core.client.admin;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class DelegationTokenConfigTest {

  @Test
  public void testTimeUnit() {
    DelegationTokenConfig config1 = new DelegationTokenConfig(), config2 = new DelegationTokenConfig();

    config1.setTokenLifetime(1000, TimeUnit.MILLISECONDS);
    config2.setTokenLifetime(1, TimeUnit.SECONDS);

    assertEquals(config1.getTokenLifetime(TimeUnit.MILLISECONDS), config2.getTokenLifetime(TimeUnit.MILLISECONDS));
    assertEquals(config1, config2);
    assertEquals(config1.hashCode(), config2.hashCode());
  }

  @Test
  public void testNoTimeout() {
    DelegationTokenConfig config = new DelegationTokenConfig();

    config.setTokenLifetime(0, TimeUnit.MILLISECONDS);

    assertEquals(0, config.getTokenLifetime(TimeUnit.MILLISECONDS));

  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidLifetime() {
    new DelegationTokenConfig().setTokenLifetime(-1, TimeUnit.DAYS);
  }

  @Test(expected = NullPointerException.class)
  public void testSetInvalidTimeUnit() {
    new DelegationTokenConfig().setTokenLifetime(5, null);
  }

  @Test(expected = NullPointerException.class)
  public void testGetInvalidTimeUnit() {
    new DelegationTokenConfig().getTokenLifetime(null);
  }
}

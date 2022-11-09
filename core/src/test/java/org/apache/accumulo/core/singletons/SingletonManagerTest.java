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
package org.apache.accumulo.core.singletons;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SingletonManagerTest {

  TestService service1;
  TestService service2;

  @BeforeEach
  public void setup() {
    SingletonManager.reset();
    assertEquals(0, SingletonManager.getReservationCount());

    service1 = new TestService(true);
    service2 = new TestService(false);

    SingletonManager.register(service1);
    SingletonManager.register(service2);

    assertEquals(new TestService(true, 0, 0), service1);
    assertEquals(new TestService(true, 1, 0), service2);

    assertEquals(Mode.CLIENT, SingletonManager.getMode());
  }

  @Test
  public void testClient() {
    SingletonReservation resv1 = SingletonManager.getClientReservation();

    assertEquals(1, SingletonManager.getReservationCount());

    SingletonReservation resv2 = SingletonManager.getClientReservation();

    assertEquals(2, SingletonManager.getReservationCount());
    assertEquals(new TestService(true, 0, 0), service1);
    assertEquals(new TestService(true, 1, 0), service2);

    resv1.close();

    assertEquals(1, SingletonManager.getReservationCount());
    assertEquals(new TestService(true, 0, 0), service1);
    assertEquals(new TestService(true, 1, 0), service2);

    // calling close again should have no effect
    resv1.close();

    assertEquals(1, SingletonManager.getReservationCount());
    assertEquals(new TestService(true, 0, 0), service1);
    assertEquals(new TestService(true, 1, 0), service2);

    resv2.close();

    assertEquals(0, SingletonManager.getReservationCount());
    assertEquals(new TestService(false, 0, 1), service1);
    assertEquals(new TestService(false, 1, 1), service2);

    SingletonReservation resv3 = SingletonManager.getClientReservation();

    assertEquals(1, SingletonManager.getReservationCount());
    assertEquals(new TestService(true, 1, 1), service1);
    assertEquals(new TestService(true, 2, 1), service2);

    resv3.close();

    assertEquals(0, SingletonManager.getReservationCount());
    assertEquals(new TestService(false, 1, 2), service1);
    assertEquals(new TestService(false, 2, 2), service2);
  }

  @Test
  public void testConnectorRemoved() {
    SingletonReservation resv1 = SingletonManager.getClientReservation();
    resv1.close();

    assertEquals(new TestService(false, 0, 1), service1);
    assertEquals(new TestService(false, 1, 1), service2);

    // this should do nothing
    @SuppressWarnings("deprecation")
    var e = assertThrows(IllegalArgumentException.class,
        () -> SingletonManager.setMode(Mode.CONNECTOR));
    assertTrue(e.getMessage().contains("CONNECTOR"));

    assertEquals(new TestService(false, 0, 1), service1);
    assertEquals(new TestService(false, 1, 1), service2);
  }

  @Test
  public void testServerPreventsDisable() {

    SingletonManager.setMode(Mode.SERVER);
    assertEquals(Mode.SERVER, SingletonManager.getMode());

    SingletonReservation resv1 = SingletonManager.getClientReservation();

    assertEquals(1, SingletonManager.getReservationCount());

    SingletonReservation resv2 = SingletonManager.getClientReservation();

    assertEquals(2, SingletonManager.getReservationCount());

    resv1.close();
    resv2.close();

    assertEquals(0, SingletonManager.getReservationCount());

    assertEquals(new TestService(true, 0, 0), service1);
    assertEquals(new TestService(true, 1, 0), service2);

    // can not leave server mode, so this should have no effect
    SingletonManager.setMode(Mode.CLIENT);
    assertEquals(Mode.SERVER, SingletonManager.getMode());

    assertEquals(new TestService(true, 0, 0), service1);
    assertEquals(new TestService(true, 1, 0), service2);
  }

  @Test
  public void testServerEnables() {
    SingletonReservation resv1 = SingletonManager.getClientReservation();
    resv1.close();

    assertEquals(new TestService(false, 0, 1), service1);
    assertEquals(new TestService(false, 1, 1), service2);

    // this should enable services
    SingletonManager.setMode(Mode.SERVER);
    assertEquals(Mode.SERVER, SingletonManager.getMode());

    assertEquals(new TestService(true, 1, 1), service1);
    assertEquals(new TestService(true, 2, 1), service2);

    // can not leave server mode, so this should have no effect
    SingletonManager.setMode(Mode.CLIENT);
    assertEquals(Mode.SERVER, SingletonManager.getMode());

    assertEquals(new TestService(true, 1, 1), service1);
    assertEquals(new TestService(true, 2, 1), service2);
  }

  private static class TestService implements SingletonService {

    boolean enabled;
    int enables = 0;
    int disables = 0;

    TestService(boolean enabled) {
      this.enabled = enabled;
    }

    TestService(boolean enabled, int enables, int disables) {
      this.enabled = enabled;
      this.enables = enables;
      this.disables = disables;
    }

    @Override
    public boolean isEnabled() {
      return enabled;
    }

    @Override
    public void enable() {
      assertFalse(enabled);
      enabled = true;
      enables++;

    }

    @Override
    public void disable() {
      assertTrue(enabled);
      enabled = false;
      disables++;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TestService) {
        TestService ots = (TestService) o;
        return enabled == ots.enabled && enables == ots.enables && disables == ots.disables;
      }
      return false;
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "enabled:" + enabled + " enables:" + enables + " disables:" + disables;
    }
  }
}

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

package org.apache.accumulo.core.singletons;

import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * This class automates management of static singletons that maintain state for Accumulo clients.
 * Historically, Accumulo client code that used Connector had no control over these singletons. The
 * new AccumuloClient API that replaces Connector is closeable. When all AccumuloClients are closed
 * then resources used by the singletons are released. This class coordinates releasing those
 * resources. For compatibility purposes this class will not release resources when the user has
 * created Connectors.
 *
 * <p>
 * This class is intermediate solution to resource management. Ideally there would be no static
 * state and AccumuloClients would own all of their state and clean it up on close. If
 * AccumuloClient is not closable at inception, then it is harder to make it closable later. If
 * AccumuloClient is not closable, then its hard to remove the static state. This class enables
 * making AccumuloClient closable at inception so that static state can be removed later.
 *
 * @see org.apache.accumulo.core.util.CleanUp
 */

public class SingletonManager {

  /**
   * These enums determine the behavior of the SingletonManager.
   *
   */
  public enum Mode {
    /**
     * In this mode singletons are disabled when the number of active client reservations goes to
     * zero.
     */
    CLIENT,
    /**
     * In this mode singletons are never disabled.
     */
    SERVER,
    /**
     * In this mode singletons are never disabled unless the mode is set back to CLIENT. The user
     * can do this by using util.CleanUp (an old API created for users).
     */
    CONNECTOR
  }

  private static long reservations;
  private static Mode mode;
  private static boolean enabled;
  private static boolean transitionedFromClientToConnector;
  private static List<SingletonService> services;

  @VisibleForTesting
  static void reset() {
    reservations = 0;
    mode = Mode.CLIENT;
    enabled = true;
    transitionedFromClientToConnector = false;
    services = new ArrayList<>();
  }

  static {
    // test code calls this, so always use it inorder to give credibility to test results
    reset();
  }

  private static void enable(SingletonService service) {
    try {
      service.enable();
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
  }

  private static void disable(SingletonService service) {
    try {
      service.disable();
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
  }

  /**
   * Register a static singleton that should be disabled and enabled as needed.
   */
  public static synchronized void register(SingletonService service) {
    if (enabled && !service.isEnabled()) {
      enable(service);
    }

    if (!enabled && service.isEnabled()) {
      disable(service);
    }

    services.add(service);
  }

  /**
   * This method should be called when creating Accumulo clients using the public API. Accumulo
   * client created internally within the code probably should not call this method. While a client
   * hold a reservation singleton services are enabled.
   *
   * @return A reservation that must be closed when the AccumuloClient is closed.
   */
  public static synchronized SingletonReservation getClientReservation() {
    Preconditions.checkState(reservations >= 0);
    reservations++;
    transition();
    return new SingletonReservation();
  }

  static synchronized void releaseRerservation() {
    Preconditions.checkState(reservations > 0);
    reservations--;
    transition();
  }

  @VisibleForTesting
  public static long getReservationCount() {
    return reservations;
  }

  /**
   * Change how singletons are managed. The default mode is {@link Mode#CLIENT}
   */
  public static synchronized void setMode(Mode mode) {
    if (SingletonManager.mode == Mode.CLIENT && mode == Mode.CONNECTOR) {
      if (transitionedFromClientToConnector) {
        throw new IllegalStateException("Can only transition from " + Mode.CLIENT + " to "
            + Mode.CONNECTOR + " once.  This error indicates that "
            + "org.apache.accumulo.core.util.CleanUp.shutdownNow() was called and then later a "
            + "Connector was created.  Connectors can not be created after CleanUp.shutdownNow()"
            + " is called.");
      }

      transitionedFromClientToConnector = true;
    }

    // do not change from server mode, its a terminal mode that can not be left once entered
    if (SingletonManager.mode != Mode.SERVER) {
      SingletonManager.mode = mode;
    }
    transition();
  }

  @VisibleForTesting
  static synchronized Mode getMode() {
    return mode;
  }

  private static void transition() {
    if (enabled && reservations == 0 && mode == Mode.CLIENT) {
      for (SingletonService service : services) {
        disable(service);
      }
      enabled = false;
    }

    if (!enabled && (mode == Mode.CONNECTOR || mode == Mode.SERVER
        || (mode == Mode.CLIENT && reservations > 0))) {
      for (SingletonService service : services) {
        enable(service);
      }

      enabled = true;
    }
  }

}

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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * This class automates management of static singletons that maintain state for Accumulo clients.
 * Historically, Accumulo client code that used Connector had no control over these singletons. The
 * new AccumuloClient API that replaces Connector is closeable. When all AccumuloClients are closed
 * then resources used by the singletons are released. This class coordinates releasing those
 * resources.
 *
 * <p>
 * This class is intermediate solution to resource management. Ideally there would be no static
 * state and AccumuloClients would own all of their state and clean it up on close. If
 * AccumuloClient is not closable at inception, then it is harder to make it closable later. If
 * AccumuloClient is not closable, then its hard to remove the static state. This class enables
 * making AccumuloClient closable at inception so that static state can be removed later.
 *
 */
public class SingletonManager {

  private static final Logger log = LoggerFactory.getLogger(SingletonManager.class);

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
     * In this mode singletons are never disabled, unless the CLOSED mode is entered.
     */
    SERVER,
    /**
     * This mode was removed along with Connector in 3.0.0. It no longer does anything, but is kept
     * here to preserve enum ordinals.
     */
    @Deprecated(since = "3.0.0")
    CONNECTOR,
    /**
     * In this mode singletons are permanently disabled and entering this mode prevents
     * transitioning to other modes.
     */
    CLOSED

  }

  private static long reservations;
  private static Mode mode;
  private static boolean enabled;
  private static List<SingletonService> services;

  @VisibleForTesting
  static void reset() {
    reservations = 0;
    mode = Mode.CLIENT;
    enabled = true;
    services = new ArrayList<>();
  }

  static {
    reset();
  }

  private static void enable(SingletonService service) {
    try {
      service.enable();
    } catch (RuntimeException e) {
      log.error("Failed to enable singleton service", e);
    }
  }

  private static void disable(SingletonService service) {
    try {
      service.disable();
    } catch (RuntimeException e) {
      log.error("Failed to disable singleton service", e);
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
   * clients created internally within Accumulo code should probably call
   * {@link SingletonReservation#noop()} instead. While a client holds a reservation, singleton
   * services are enabled.
   *
   * @return A reservation that must be closed when the AccumuloClient is closed.
   */
  public static synchronized SingletonReservation getClientReservation() {
    Preconditions.checkState(reservations >= 0);
    reservations++;
    transition();
    return new SingletonReservation();
  }

  static synchronized void releaseReservation() {
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
    if (SingletonManager.mode == mode) {
      return;
    }
    if (SingletonManager.mode == Mode.CLOSED) {
      throw new IllegalStateException("Cannot leave closed mode once entered");
    }
    if (mode == Mode.CONNECTOR) {
      throw new IllegalArgumentException("CONNECTOR mode was removed");
    }

    /*
     * Always allow transition to closed and only allow transition to client/connector when the
     * current mode is not server.
     */
    if (SingletonManager.mode != Mode.SERVER || mode == Mode.CLOSED) {
      SingletonManager.mode = mode;
    }
    transition();
  }

  @VisibleForTesting
  public static synchronized Mode getMode() {
    return mode;
  }

  private static void transition() {
    if (enabled) {
      // if we're in an enabled state AND
      // the mode is CLOSED or there are no active clients,
      // then disable everything
      if (mode == Mode.CLOSED || (mode == Mode.CLIENT && reservations == 0)) {
        services.forEach(SingletonManager::disable);
        enabled = false;
      }
    } else {
      // if we're in a disabled state AND
      // the mode is SERVER or if there are active clients,
      // then enable everything
      if (mode == Mode.SERVER || (mode == Mode.CLIENT && reservations > 0)) {
        services.forEach(SingletonManager::enable);
        enabled = true;
      }
    }
  }
}

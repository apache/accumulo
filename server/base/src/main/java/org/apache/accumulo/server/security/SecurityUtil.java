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
package org.apache.accumulo.server.security;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecurityUtil {
  private static final Logger log = LoggerFactory.getLogger(SecurityUtil.class);
  private static final Logger renewalLog = LoggerFactory.getLogger("KerberosTicketRenewal");

  /**
   * This method is for logging a server in kerberos. If this is used in client code, it will fail
   * unless run as the accumulo keytab's owner. Instead, use {@link #login(String, String)}
   */
  public static void serverLogin(AccumuloConfiguration acuConf) {
    serverLogin(acuConf, acuConf.getPath(Property.GENERAL_KERBEROS_KEYTAB),
        acuConf.get(Property.GENERAL_KERBEROS_PRINCIPAL));
  }

  /**
   * Performs a Kerberos login using the given Kerberos principal and keytab if they are non-null
   * and positive length Strings. This method automatically spawns a thread to renew the given
   * ticket upon successful login using {@link Property#GENERAL_KERBEROS_RENEWAL_PERIOD} as the
   * renewal period. This method does nothing if either {@code keyTab} or {@code principal} are null
   * or of zero length.
   *
   * @param acuConf The Accumulo configuration
   * @param keyTab The path to the Kerberos keytab file
   * @param principal The Kerberos principal
   */
  public static void serverLogin(AccumuloConfiguration acuConf, String keyTab, String principal) {
    if (keyTab == null || keyTab.isEmpty()) {
      return;
    }

    if (principal == null || principal.isEmpty()) {
      return;
    }

    if (login(principal, keyTab)) {
      try {
        startTicketRenewalThread(acuConf, UserGroupInformation.getCurrentUser(),
            acuConf.getTimeInMillis(Property.GENERAL_KERBEROS_RENEWAL_PERIOD));
        return;
      } catch (IOException e) {
        log.error("Failed to obtain Kerberos user after successfully logging in", e);
      }
    }

    throw new RuntimeException(
        "Failed to perform Kerberos login for " + principal + " using  " + keyTab);
  }

  /**
   * This will log in the given user in kerberos.
   *
   * @param principalConfig This is the principals name in the format NAME/HOST@REALM.
   *        {@link org.apache.hadoop.security.SecurityUtil#HOSTNAME_PATTERN} will automatically be
   *        replaced by the systems host name.
   * @return true if login succeeded, otherwise false
   */
  static boolean login(String principalConfig, String keyTabPath) {
    try {
      String principalName = getServerPrincipal(principalConfig);
      if (keyTabPath != null && principalName != null && !keyTabPath.isEmpty()
          && !principalName.isEmpty()) {
        log.info("Attempting to login with keytab as {}", principalName);
        UserGroupInformation.loginUserFromKeytab(principalName, keyTabPath);
        log.info("Successfully logged in as user {}", principalName);
        return true;
      }
    } catch (IOException io) {
      log.error("Error logging in user " + principalConfig + " using keytab at " + keyTabPath, io);
    }
    return false;
  }

  /**
   * {@link org.apache.hadoop.security.SecurityUtil#getServerPrincipal(String, String)}
   */
  public static String getServerPrincipal(String configuredPrincipal) {
    try {
      return org.apache.hadoop.security.SecurityUtil.getServerPrincipal(configuredPrincipal,
          InetAddress.getLocalHost().getCanonicalHostName());
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not convert configured server principal: " + configuredPrincipal, e);
    }
  }

  /**
   * Start a thread that periodically attempts to renew the current Kerberos user's ticket.
   *
   * @param conf Accumulo configuration
   * @param ugi The current Kerberos user.
   * @param renewalPeriod The amount of time between attempting renewals.
   */
  static void startTicketRenewalThread(AccumuloConfiguration conf, final UserGroupInformation ugi,
      final long renewalPeriod) {
    Threads.createThread("Kerberos Ticket Renewal", () -> {
      while (true) {
        try {
          renewalLog.debug("Invoking renewal attempt for Kerberos ticket");
          // While we run this "frequently", the Hadoop implementation will only perform the
          // login
          // at 80% of ticket lifetime.
          ugi.checkTGTAndReloginFromKeytab();
        } catch (IOException e) {
          // Should failures to renew the ticket be retried more quickly?
          renewalLog.error("Failed to renew Kerberos ticket", e);
        }

        // Wait for a bit before checking again.
        try {
          Thread.sleep(renewalPeriod);
        } catch (InterruptedException e) {
          renewalLog.error("Renewal thread interrupted", e);
          Thread.currentThread().interrupt();
          return;
        }
      }
    }).start();
  }
}

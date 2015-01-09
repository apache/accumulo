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
package org.apache.accumulo.server.security;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

/**
 *
 */
public class SecurityUtil {
  private static final Logger log = Logger.getLogger(SecurityUtil.class);
  public static boolean usingKerberos = false;

  /**
   * This method is for logging a server in kerberos. If this is used in client code, it will fail unless run as the accumulo keytab's owner. Instead, use
   * {@link #login(String, String)}
   */
  public static void serverLogin(AccumuloConfiguration acuConf) {
    String keyTab = acuConf.getPath(Property.GENERAL_KERBEROS_KEYTAB);
    if (keyTab == null || keyTab.length() == 0)
      return;

    usingKerberos = true;

    String principalConfig = acuConf.get(Property.GENERAL_KERBEROS_PRINCIPAL);
    if (principalConfig == null || principalConfig.length() == 0)
      return;

    if (login(principalConfig, keyTab)) {
      try {
        // This spawns a thread to periodically renew the logged in (accumulo) user
        UserGroupInformation.getLoginUser();
        return;
      } catch (IOException io) {
        log.error("Error starting up renewal thread. This shouldn't be happenining.", io);
      }
    }

    throw new RuntimeException("Failed to perform Kerberos login for " + principalConfig + " using  " + keyTab);
  }

  /**
   * This will log in the given user in kerberos.
   *
   * @param principalConfig
   *          This is the principals name in the format NAME/HOST@REALM. {@link org.apache.hadoop.security.SecurityUtil#HOSTNAME_PATTERN} will automatically be
   *          replaced by the systems host name.
   * @return true if login succeeded, otherwise false
   */
  public static boolean login(String principalConfig, String keyTabPath) {
    try {
      String principalName = org.apache.hadoop.security.SecurityUtil.getServerPrincipal(principalConfig, InetAddress.getLocalHost().getCanonicalHostName());
      if (keyTabPath != null && principalName != null && keyTabPath.length() != 0 && principalName.length() != 0) {
        UserGroupInformation.loginUserFromKeytab(principalName, keyTabPath);
        log.info("Succesfully logged in as user " + principalConfig);
        return true;
      }
    } catch (IOException io) {
      log.error("Error logging in user " + principalConfig + " using keytab at " + keyTabPath, io);
    }
    return false;
  }
}

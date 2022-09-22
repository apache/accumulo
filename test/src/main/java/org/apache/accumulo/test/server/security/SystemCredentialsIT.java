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
package org.apache.accumulo.test.server.security;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.junit.jupiter.api.Test;

public class SystemCredentialsIT extends ConfigurableMacBase {

  private static final int SCAN_FAILED = 7, AUTHENICATION_FAILED = 8;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Test
  public void testSystemCredentials() throws Exception {
    assertEquals(0,
        exec(SystemCredentialsIT.class, "good", getCluster().getZooKeepers()).waitFor());
    assertEquals(AUTHENICATION_FAILED,
        exec(SystemCredentialsIT.class, "bad", getCluster().getZooKeepers()).waitFor());
    assertEquals(AUTHENICATION_FAILED,
        exec(SystemCredentialsIT.class, "bad_password", getCluster().getZooKeepers()).waitFor());
  }

  public static void main(final String[] args) throws AccumuloException, TableNotFoundException {
    var siteConfig = SiteConfiguration.auto();
    try (ServerContext context = new ServerContext(siteConfig)) {
      Credentials creds;
      InstanceId badInstanceID = InstanceId.of(SystemCredentials.class.getName());
      if (args.length < 2) {
        throw new RuntimeException("Incorrect usage; expected to be run by test only");
      }
      switch (args[0]) {
        case "bad":
          creds = SystemCredentials.get(badInstanceID, siteConfig);
          break;
        case "good":
          creds = SystemCredentials.get(context.getInstanceID(), siteConfig);
          break;
        case "bad_password":
          creds = new SystemCredentials(badInstanceID, "!SYSTEM", new PasswordToken("fake"));
          break;
        default:
          throw new RuntimeException("Incorrect usage; expected to be run by test only");
      }
      try (AccumuloClient client = Accumulo.newClient().from(context.getProperties())
          .as(creds.getPrincipal(), creds.getToken()).build()) {
        client.securityOperations().authenticateUser(creds.getPrincipal(), creds.getToken());
        try (Scanner scan = client.createScanner(RootTable.NAME, Authorizations.EMPTY)) {
          scan.forEach((k, v) -> {});
        } catch (RuntimeException e) {
          e.printStackTrace(System.err);
          System.exit(SCAN_FAILED);
        }
      } catch (AccumuloSecurityException e) {
        e.printStackTrace(System.err);
        System.exit(AUTHENICATION_FAILED);
      }
    }
  }
}

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
package org.apache.accumulo.test.server.security;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.junit.Test;

public class SystemCredentialsIT extends ConfigurableMacBase {

  private static final int FAIL_CODE = 7, BAD_PASSWD_FAIL_CODE = 8;

  @Override
  protected int defaultTimeoutSeconds() {
    return 1 * 60;
  }

  @Test
  public void testSystemCredentials() throws Exception {
    assertEquals(0, exec(SystemCredentialsIT.class, "good", getCluster().getZooKeepers()).waitFor());
    assertEquals(FAIL_CODE, exec(SystemCredentialsIT.class, "bad", getCluster().getZooKeepers()).waitFor());
    assertEquals(BAD_PASSWD_FAIL_CODE, exec(SystemCredentialsIT.class, "bad_password", getCluster().getZooKeepers()).waitFor());
  }

  public static void main(final String[] args) throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
    Credentials creds = null;
    if (args.length < 2)
      throw new RuntimeException("Incorrect usage; expected to be run by test only");
    if (args[0].equals("bad")) {
      Instance inst = new Instance() {

        @Override
        public int getZooKeepersSessionTimeOut() {
          throw new UnsupportedOperationException();
        }

        @Override
        public String getZooKeepers() {
          throw new UnsupportedOperationException();
        }

        @Override
        public String getRootTabletLocation() {
          throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getMasterLocations() {
          throw new UnsupportedOperationException();
        }

        @Override
        public String getInstanceName() {
          throw new UnsupportedOperationException();
        }

        @Override
        public String getInstanceID() {
          return SystemCredentials.class.getName();
        }

        @Override
        public Connector getConnector(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
          throw new UnsupportedOperationException();
        }

        @Deprecated
        @Override
        public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
          throw new UnsupportedOperationException();
        }

        @Deprecated
        @Override
        public Connector getConnector(String user, ByteBuffer pass) throws AccumuloException, AccumuloSecurityException {
          throw new UnsupportedOperationException();
        }

        @Deprecated
        @Override
        public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
          throw new UnsupportedOperationException();
        }

      };
      creds = SystemCredentials.get(inst);
    } else if (args[0].equals("good")) {
      creds = SystemCredentials.get(HdfsZooInstance.getInstance());
    } else if (args[0].equals("bad_password")) {
      Instance inst = new Instance() {

        @Override
        public int getZooKeepersSessionTimeOut() {
          throw new UnsupportedOperationException();
        }

        @Override
        public String getZooKeepers() {
          throw new UnsupportedOperationException();
        }

        @Override
        public String getRootTabletLocation() {
          throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getMasterLocations() {
          throw new UnsupportedOperationException();
        }

        @Override
        public String getInstanceName() {
          throw new UnsupportedOperationException();
        }

        @Override
        public String getInstanceID() {
          return SystemCredentials.class.getName();
        }

        @Override
        public Connector getConnector(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
          throw new UnsupportedOperationException();
        }

        @Deprecated
        @Override
        public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
          throw new UnsupportedOperationException();
        }

        @Deprecated
        @Override
        public Connector getConnector(String user, ByteBuffer pass) throws AccumuloException, AccumuloSecurityException {
          throw new UnsupportedOperationException();
        }

        @Deprecated
        @Override
        public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
          throw new UnsupportedOperationException();
        }

      };
      creds = new SystemCredentials(inst, "!SYSTEM", new PasswordToken("fake"));
    } else {
      throw new RuntimeException("Incorrect usage; expected to be run by test only");
    }
    Instance instance = HdfsZooInstance.getInstance();
    Connector conn;
    try {
      conn = instance.getConnector(creds.getPrincipal(), creds.getToken());
    } catch (AccumuloSecurityException e) {
      e.printStackTrace(System.err);
      System.exit(BAD_PASSWD_FAIL_CODE);
      return;
    }
    try {
      Scanner scan = conn.createScanner(RootTable.NAME, Authorizations.EMPTY);
      for (Entry<Key,Value> e : scan) {
        e.hashCode();
      }
    } catch (RuntimeException e) {
      // catch the runtime exception from the scanner iterator
      if (e.getCause() instanceof AccumuloSecurityException
          && ((AccumuloSecurityException) e.getCause()).getSecurityErrorCode() == SecurityErrorCode.BAD_CREDENTIALS) {
        e.printStackTrace(System.err);
        System.exit(FAIL_CODE);
      }
    }
  }
}

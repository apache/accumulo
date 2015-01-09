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
package org.apache.accumulo.test.randomwalk.security;

import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class AlterSystemPerm extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn = state.getConnector();
    WalkingSecurity ws = new WalkingSecurity(state);

    String action = props.getProperty("task", "toggle");
    String perm = props.getProperty("perm", "random");

    String targetUser = WalkingSecurity.get(state).getSysUserName();

    SystemPermission sysPerm;
    if (perm.equals("random")) {
      Random r = new Random();
      int i = r.nextInt(SystemPermission.values().length);
      sysPerm = SystemPermission.values()[i];
    } else
      sysPerm = SystemPermission.valueOf(perm);

    boolean hasPerm = ws.hasSystemPermission(targetUser, sysPerm);

    // toggle
    if (!"take".equals(action) && !"give".equals(action)) {
      if (hasPerm != conn.securityOperations().hasSystemPermission(targetUser, sysPerm))
        throw new AccumuloException("Test framework and accumulo are out of sync!");
      if (hasPerm)
        action = "take";
      else
        action = "give";
    }

    if ("take".equals(action)) {
      try {
        conn.securityOperations().revokeSystemPermission(targetUser, sysPerm);
      } catch (AccumuloSecurityException ae) {
        switch (ae.getSecurityErrorCode()) {
          case GRANT_INVALID:
            if (sysPerm.equals(SystemPermission.GRANT))
              return;
            throw new AccumuloException("Got GRANT_INVALID when not dealing with GRANT", ae);
          case PERMISSION_DENIED:
            throw new AccumuloException("Test user doesn't have root", ae);
          case USER_DOESNT_EXIST:
            throw new AccumuloException("System user doesn't exist and they SHOULD.", ae);
          default:
            throw new AccumuloException("Got unexpected exception", ae);
        }
      }
      ws.revokeSystemPermission(targetUser, sysPerm);
    } else if ("give".equals(action)) {
      try {
        conn.securityOperations().grantSystemPermission(targetUser, sysPerm);
      } catch (AccumuloSecurityException ae) {
        switch (ae.getSecurityErrorCode()) {
          case GRANT_INVALID:
            if (sysPerm.equals(SystemPermission.GRANT))
              return;
          case PERMISSION_DENIED:
            throw new AccumuloException("Test user doesn't have root", ae);
          case USER_DOESNT_EXIST:
            throw new AccumuloException("System user doesn't exist and they SHOULD.", ae);
          default:
            throw new AccumuloException("Got unexpected exception", ae);
        }
      }
      ws.grantSystemPermission(targetUser, sysPerm);
    }
  }

}

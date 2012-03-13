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
package org.apache.accumulo.server.test.randomwalk.security;

import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;

public class SetAuths extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn;
    
    String authsString = props.getProperty("auths", "_random");
    
    String targetUser = props.getProperty("system");
    String target;
    boolean exists;
    boolean hasPermission;
    if ("table".equals(targetUser)) {
      target = SecurityHelper.getTabUserName(state);
      exists = SecurityHelper.getTabUserExists(state);
      conn = SecurityHelper.getSystemConnector(state);
      hasPermission = SecurityHelper.getSysPerm(state, SecurityHelper.getSysUserName(state), SystemPermission.ALTER_USER);
    } else {
      target = SecurityHelper.getSysUserName(state);
      exists = true;
      conn = state.getConnector();
      hasPermission = true;
    }
    Authorizations auths;
    if (authsString.equals("_random")) {
      String[] possibleAuths = SecurityHelper.getAuthsArray();
      
      Random r = new Random();
      int i = r.nextInt(possibleAuths.length);
      String[] authSet = new String[i];
      int length = possibleAuths.length;
      for (int j = 0; j < i; j++) {
        int nextRand = r.nextInt(length);
        authSet[j] = possibleAuths[nextRand];
        length--;
        possibleAuths[nextRand] = possibleAuths[length];
        possibleAuths[length] = authSet[j];
      }
      auths = new Authorizations(authSet);
    } else {
      auths = new Authorizations(authsString.split(","));
    }
    
    try {
      conn.securityOperations().changeUserAuthorizations(target, auths);
    } catch (AccumuloSecurityException ae) {
      switch (ae.getErrorCode()) {
        case PERMISSION_DENIED:
          if (hasPermission)
            throw new AccumuloException("Got a security exception when I should have had permission.", ae);
          else
            return;
        case USER_DOESNT_EXIST:
          if (exists)
            throw new AccumuloException("Got security exception when the user should have existed", ae);
          else
            return;
        default:
          throw new AccumuloException("Got unexpected exception", ae);
      }
    }
    SecurityHelper.setUserAuths(state, target, auths);
    if (!hasPermission)
      throw new AccumuloException("Didn't get Security Exception when we should have");
  }
  
}

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
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.test.randomwalk.Environment;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class ChangePass extends Test {

  @Override
  public void visit(State state, Environment env, Properties props) throws Exception {
    String target = props.getProperty("target");
    String source = props.getProperty("source");

    String principal;
    AuthenticationToken token;
    if (source.equals("system")) {
      principal = WalkingSecurity.get(state, env).getSysUserName();
      token = WalkingSecurity.get(state, env).getSysToken();
    } else {
      principal = WalkingSecurity.get(state, env).getTabUserName();
      token = WalkingSecurity.get(state, env).getTabToken();
    }
    Connector conn = env.getInstance().getConnector(principal, token);

    boolean hasPerm;
    boolean targetExists;
    if (target.equals("table")) {
      target = WalkingSecurity.get(state, env).getTabUserName();
    } else
      target = WalkingSecurity.get(state, env).getSysUserName();

    targetExists = WalkingSecurity.get(state, env).userExists(target);

    hasPerm = WalkingSecurity.get(state, env).canChangePassword(new Credentials(principal, token).toThrift(env.getInstance()), target);

    Random r = new Random();

    byte[] newPassw = new byte[r.nextInt(50) + 1];
    for (int i = 0; i < newPassw.length; i++)
      newPassw[i] = (byte) ((r.nextInt(26) + 65) & 0xFF);

    PasswordToken newPass = new PasswordToken(newPassw);
    try {
      conn.securityOperations().changeLocalUserPassword(target, newPass);
    } catch (AccumuloSecurityException ae) {
      switch (ae.getSecurityErrorCode()) {
        case PERMISSION_DENIED:
          if (hasPerm)
            throw new AccumuloException("Change failed when it should have succeeded to change " + target + "'s password", ae);
          return;
        case USER_DOESNT_EXIST:
          if (targetExists)
            throw new AccumuloException("User " + target + " doesn't exist and they SHOULD.", ae);
          return;
        case BAD_CREDENTIALS:
          if (!WalkingSecurity.get(state, env).userPassTransient(conn.whoami()))
            throw new AccumuloException("Bad credentials for user " + conn.whoami());
          return;
        default:
          throw new AccumuloException("Got unexpected exception", ae);
      }
    }
    WalkingSecurity.get(state, env).changePassword(target, newPass);
    // Waiting 1 second for password to propogate through Zk
    Thread.sleep(1000);
    if (!hasPerm)
      throw new AccumuloException("Password change succeeded when it should have failed for " + source + " changing the password for " + target + ".");
  }
}

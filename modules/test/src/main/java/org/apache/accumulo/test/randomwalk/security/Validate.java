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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.test.randomwalk.Environment;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.log4j.Logger;

public class Validate extends Test {

  @Override
  public void visit(State state, Environment env, Properties props) throws Exception {
    validate(state, env, log);
  }

  public static void validate(State state, Environment env, Logger log) throws Exception {
    Connector conn = env.getConnector();

    boolean tableExists = WalkingSecurity.get(state, env).getTableExists();
    boolean cloudTableExists = conn.tableOperations().list().contains(WalkingSecurity.get(state, env).getTableName());
    if (tableExists != cloudTableExists)
      throw new AccumuloException("Table existance out of sync");

    boolean tableUserExists = WalkingSecurity.get(state, env).userExists(WalkingSecurity.get(state, env).getTabUserName());
    boolean cloudTableUserExists = conn.securityOperations().listLocalUsers().contains(WalkingSecurity.get(state, env).getTabUserName());
    if (tableUserExists != cloudTableUserExists)
      throw new AccumuloException("Table User existance out of sync");

    Properties props = new Properties();
    props.setProperty("target", "system");
    Authenticate.authenticate(env.getUserName(), env.getToken(), state, env, props);
    props.setProperty("target", "table");
    Authenticate.authenticate(env.getUserName(), env.getToken(), state, env, props);

    for (String user : new String[] {WalkingSecurity.get(state, env).getSysUserName(), WalkingSecurity.get(state, env).getTabUserName()}) {
      for (SystemPermission sp : SystemPermission.values()) {
        boolean hasSp = WalkingSecurity.get(state, env).hasSystemPermission(user, sp);
        boolean accuHasSp;
        try {
          accuHasSp = conn.securityOperations().hasSystemPermission(user, sp);
          log.debug("Just checked to see if user " + user + " has system perm " + sp.name() + " with answer " + accuHasSp);
        } catch (AccumuloSecurityException ae) {
          if (ae.getSecurityErrorCode().equals(SecurityErrorCode.USER_DOESNT_EXIST)) {
            if (tableUserExists)
              throw new AccumuloException("Got user DNE error when they should", ae);
            else
              continue;
          } else
            throw new AccumuloException("Unexpected exception!", ae);
        }
        if (hasSp != accuHasSp)
          throw new AccumuloException(user + " existance out of sync for system perm " + sp + " hasSp/CloudhasSP " + hasSp + " " + accuHasSp);
      }

      for (TablePermission tp : TablePermission.values()) {
        boolean hasTp = WalkingSecurity.get(state, env).hasTablePermission(user, WalkingSecurity.get(state, env).getTableName(), tp);
        boolean accuHasTp;
        try {
          accuHasTp = conn.securityOperations().hasTablePermission(user, WalkingSecurity.get(state, env).getTableName(), tp);
          log.debug("Just checked to see if user " + user + " has table perm " + tp.name() + " with answer " + accuHasTp);
        } catch (AccumuloSecurityException ae) {
          if (ae.getSecurityErrorCode().equals(SecurityErrorCode.USER_DOESNT_EXIST)) {
            if (tableUserExists)
              throw new AccumuloException("Got user DNE error when they should", ae);
            else
              continue;
          } else if (ae.getSecurityErrorCode().equals(SecurityErrorCode.TABLE_DOESNT_EXIST)) {
            if (tableExists)
              throw new AccumuloException("Got table DNE when it should", ae);
            else
              continue;
          } else
            throw new AccumuloException("Unexpected exception!", ae);
        }
        if (hasTp != accuHasTp)
          throw new AccumuloException(user + " existance out of sync for table perm " + tp + " hasTp/CloudhasTP " + hasTp + " " + accuHasTp);
      }

    }

    Authorizations accuAuths;
    Authorizations auths;
    try {
      auths = WalkingSecurity.get(state, env).getUserAuthorizations(WalkingSecurity.get(state, env).getTabCredentials());
      accuAuths = conn.securityOperations().getUserAuthorizations(WalkingSecurity.get(state, env).getTabUserName());
    } catch (ThriftSecurityException ae) {
      if (ae.getCode() == org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode.USER_DOESNT_EXIST) {
        if (tableUserExists)
          throw new AccumuloException("Table user didn't exist when they should.", ae);
        else
          return;
      }
      throw new AccumuloException("Unexpected exception!", ae);
    }
    if (!auths.equals(accuAuths))
      throw new AccumuloException("Table User authorizations out of sync");
  }

}

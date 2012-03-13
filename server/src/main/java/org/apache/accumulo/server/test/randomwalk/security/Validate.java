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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.log4j.Logger;

public class Validate extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    validate(state, log);
  }
  
  public static void validate(State state, Logger log) throws Exception {
    Connector conn = state.getConnector();
    
    boolean tableExists = SecurityHelper.getTableExists(state);
    boolean cloudTableExists = conn.tableOperations().list().contains(SecurityHelper.getTableName(state));
    if (tableExists != cloudTableExists)
      throw new AccumuloException("Table existance out of sync");
    
    boolean tableUserExists = SecurityHelper.getTabUserExists(state);
    boolean cloudTableUserExists = conn.securityOperations().listUsers().contains(SecurityHelper.getTabUserName(state));
    if (tableUserExists != cloudTableUserExists)
      throw new AccumuloException("Table User existance out of sync");
    
    Properties props = new Properties();
    props.setProperty("target", "system");
    Authenticate.authenticate(conn, state, props);
    props.setProperty("target", "table");
    Authenticate.authenticate(conn, state, props);
    
    boolean tabUserExists = SecurityHelper.getTabUserExists(state);
    for (String user : new String[] {SecurityHelper.getSysUserName(state), SecurityHelper.getTabUserName(state)}) {
      for (SystemPermission sp : SystemPermission.values()) {
        boolean hasSp = SecurityHelper.getSysPerm(state, user, sp);
        boolean accuHasSp;
        try {
          accuHasSp = conn.securityOperations().hasSystemPermission(user, sp);
          log.debug("Just checked to see if user " + user + " has system perm " + sp.name() + " with answer " + accuHasSp);
        } catch (AccumuloSecurityException ae) {
          if (ae.getErrorCode().equals(SecurityErrorCode.USER_DOESNT_EXIST)) {
            if (tabUserExists)
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
        boolean hasTp = SecurityHelper.getTabPerm(state, user, tp);
        boolean accuHasTp;
        try {
          accuHasTp = conn.securityOperations().hasTablePermission(user, SecurityHelper.getTableName(state), tp);
          log.debug("Just checked to see if user " + user + " has table perm " + tp.name() + " with answer " + accuHasTp);
        } catch (AccumuloSecurityException ae) {
          if (ae.getErrorCode().equals(SecurityErrorCode.USER_DOESNT_EXIST)) {
            if (tabUserExists)
              throw new AccumuloException("Got user DNE error when they should", ae);
            else
              continue;
          } else if (ae.getErrorCode().equals(SecurityErrorCode.TABLE_DOESNT_EXIST)) {
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
    
    Authorizations auths = SecurityHelper.getUserAuths(state, SecurityHelper.getTabUserName(state));
    Authorizations accuAuths;
    try {
      accuAuths = conn.securityOperations().getUserAuthorizations(SecurityHelper.getTabUserName(state));
    } catch (AccumuloSecurityException ae) {
      if (ae.getErrorCode().equals(SecurityErrorCode.USER_DOESNT_EXIST)) {
        if (tabUserExists)
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

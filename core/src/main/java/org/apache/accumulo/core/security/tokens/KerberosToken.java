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
package org.apache.accumulo.core.security.tokens;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.GeneralSecurityException;
import java.security.PrivilegedAction;
import java.util.Arrays;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.apache.accumulo.core.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

public class KerberosToken implements SecurityToken {
  private String principal;
  private byte[] sessionKey;
  
  public KerberosToken() {
    System.setProperty("java.security.auth.login.config", "./conf/jaas.conf");
  }
  
  /**
   * Creats a kerberos token for the provided Principal using the provided keytab
   * 
   * @param principalConfig
   *          This is the principals name in the format NAME/HOST@REALM. {@link org.apache.hadoop.security.SecurityUtil#HOSTNAME_PATTERN} will automatically be
   *          replaced by the systems host name.
   * @param keyTabPath
   *          Fully qualified path to the principal's keytab file
   * @throws IOException
   */
  public KerberosToken(String principalConfig, String keyTabPath, String destinationId) throws IOException {
    this();
    SecurityUtil.login(principalConfig, keyTabPath);
    UserGroupInformation.getLoginUser().doAs(new GetToken(destinationId));
  }
  
  /**
   * Creates a kerberos token using the arguments, using the jaas.conf idenity "Client"
   * 
   * @param username
   *          users Kerberos username
   * @param password
   *          users Kerberos password
   * @param destination
   *          name of the Kerberos principal the Accumulo servers are
   * @throws GeneralSecurityException
   */
  public KerberosToken(String username, char[] password, String destination) throws GeneralSecurityException {
    this(username, password, destination, "Client");
  }
  
  /**
   * Creates a kerberos token using the arguments, using the jaas.conf idenity specified
   * 
   * @param username
   *          users Kerberos username
   * @param password
   *          users Kerberos password
   * @param destination
   *          name of the Kerberos principal the Accumulo servers are
   * @param contextName
   *          Identityof the users in the jaas.conf
   * @throws GeneralSecurityException
   */
  public KerberosToken(String username, char[] password, String destination, String contextName) throws GeneralSecurityException {
    this();
    LoginContext loginCtx = null;
    // "Client" references the JAAS configuration in the jaas.conf file.
    
    loginCtx = new LoginContext(contextName, new LoginCallbackHandler(username, password));
    loginCtx.login();
    final Subject subject = loginCtx.getSubject();
    
    // The GSS context initiation has to be performed as a privileged action.
    byte[] serviceTicket = Subject.doAs(subject, new GetToken(destination));
    
    principal = username;
    sessionKey = serviceTicket;
  }
  
  class GetToken implements PrivilegedAction<byte[]> {
    private final GSSContext context;
    
    public GetToken(String destination) {
      try {
        Oid oid = new Oid("1.2.840.113554.1.2.2");
        GSSManager manager = GSSManager.getInstance();
        GSSName serverName = manager.createName(destination, GSSName.NT_USER_NAME);
        context = manager.createContext(serverName, oid, null, GSSContext.INDEFINITE_LIFETIME);
      } catch (GSSException e1) {
        // This is a well documented OID. If it does happen, you have a MASSIVE issue
        throw new RuntimeException(e1);
      }
    }
    
    public byte[] run() {
      try {
        byte[] token = new byte[0];
        // This is a one pass context initialization.
        context.requestMutualAuth(false);
        context.requestCredDeleg(false);
        return context.initSecContext(token, 0, 0);
        
      } catch (GSSException e) {
        e.printStackTrace();
        return null;
      }
    }
  }
  
  public String getPrincipal() {
    return principal;
  }
  
  public byte[] getSessionKey() {
    return sessionKey;
  }
  
  public void setPrincipal(String principal) {
    this.principal = principal;
  }

  public void setSessionKey(byte[] sessionKey) {
    this.sessionKey = sessionKey;
  }

  private void readObject(ObjectInputStream aInputStream) throws IOException, ClassNotFoundException {
    aInputStream.defaultReadObject();
  }
  
  private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
    aOutputStream.defaultWriteObject();
  }
  
  public void destroy() {
    Arrays.fill(sessionKey, (byte) 0);
    sessionKey = null;
  }
  
  @Override
  public boolean isDestroyed() {
    return sessionKey == null;
  }
  
  public String toString() {
    return "KerberosToken(" + this.principal + ":" + new String(this.getSessionKey()) + ")";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((principal == null) ? 0 : principal.hashCode());
    result = prime * result + Arrays.hashCode(sessionKey);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof KerberosToken))
      return false;
    KerberosToken other = (KerberosToken) obj;
    if (principal == null) {
      if (other.principal != null)
        return false;
    } else if (!principal.equals(other.principal))
      return false;
    if (!Arrays.equals(sessionKey, other.sessionKey))
      return false;
    return true;
  }

  @Override
  public SecuritySerDe<? extends SecurityToken> getSerDe() {
    return new KerberosSerDe();
  }
}

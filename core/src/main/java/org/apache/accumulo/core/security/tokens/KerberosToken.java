package org.apache.accumulo.core.security.tokens;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.PrivilegedAction;
import java.util.Arrays;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.apache.accumulo.core.security.SecurityUtil;
import org.apache.accumulo.core.security.thrift.ThriftKerberosToken;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

public class KerberosToken extends ThriftKerberosToken implements AccumuloToken<ThriftKerberosToken,ThriftKerberosToken._Fields> {
  private static final long serialVersionUID = -3592193087970250922L;
  
  public KerberosToken() {
    super();
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
    
    user = username;
    ticket = ByteBuffer.wrap(serviceTicket);
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
    return user;
  }
  
  public byte[] getTicket() {
    return ByteBufferUtil.toBytes(ticket);
  }
  
  private void readObject(ObjectInputStream aInputStream) throws IOException, ClassNotFoundException {
    aInputStream.defaultReadObject();
  }
  
  private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
    aOutputStream.defaultWriteObject();
  }
  
  public void destroy() {
    Arrays.fill(ticket.array(), (byte) 0);
    ticket = null;
  }
  
  @Override
  public boolean isDestroyed() {
    return ticket == null;
  }
  
  public boolean equals(AccumuloToken<?,?> token) {
    if (token instanceof KerberosToken) {
      KerberosToken kt = (KerberosToken) token;
      return this.user.equals(kt.user) && Arrays.equals(this.getTicket(), kt.getTicket());
    } else
      return false;
  }
  
  public String toString() {
    return "KerberosToken("+this.user+":"+new String(this.getTicket())+")";
  }
}

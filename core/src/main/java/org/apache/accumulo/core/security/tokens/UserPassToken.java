package org.apache.accumulo.core.security.tokens;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftUserPassToken;

@SuppressWarnings("deprecation")
public class UserPassToken extends ThriftUserPassToken implements AccumuloToken<ThriftUserPassToken,ThriftUserPassToken._Fields>, PasswordUpdatable {
  private static final long serialVersionUID = 7331872580391311737L;
  
  public UserPassToken() {
    super();
  }
  
  public UserPassToken(String user, ByteBuffer password) {
    super(user, password);
  }
  
  public UserPassToken(String user, byte[] password) {
    super(user, ByteBuffer.wrap(password));
  }
  
  public UserPassToken(String user, CharSequence password) {
    this(user, password.toString().getBytes(Charset.forName("UTF-8")));
  }
  
  public UserPassToken(ThriftUserPassToken upt) {
    super(upt);
  }
  
  public void destroy() {
    Arrays.fill(password.array(), (byte) 0);
    password = null;
  }
  
  @Override
  public boolean isDestroyed() {
    return password == null;
  }
  
  public static UserPassToken convertAuthInfo(AuthInfo credentials) {
    return new UserPassToken(credentials.user, credentials.password);
  }
  
  @Override
  public String getPrincipal() {
    return user;
  }
  
  @Override
  public void updatePassword(byte[] newPassword) {
    this.password = ByteBuffer.wrap(Arrays.copyOf(newPassword, newPassword.length));
  }
  
  @Override
  public void updatePassword(PasswordUpdatable pu) {
    updatePassword(pu.getPassword());
  }
  
  public boolean equals(AccumuloToken<?,?> token) {
    if (token instanceof UserPassToken) {
      UserPassToken upt = (UserPassToken) token;
      return this.user.equals(upt.user) && Arrays.equals(this.getPassword(), upt.getPassword());
    } else {
      System.out.println("Compared UserPassToken to " + token.getClass());
      return false;
    }  }
  
  public String toString() {
    return "UserPassToken("+this.user+":"+new String(this.getPrincipal())+")";
  }
}

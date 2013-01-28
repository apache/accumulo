package org.apache.accumulo.core.security.tokens;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ByteBufferUtil;

@SuppressWarnings("deprecation")
public class UserPassToken implements SecurityToken, PasswordUpdatable {
  private String username;
  private byte[] password;

  public UserPassToken(String user, ByteBuffer password) {
    this(user, ByteBufferUtil.toBytes(password));
  }
  
  public UserPassToken(String user, byte[] password) {
    this.username = user;
    this.password = password;
  }
  
  public UserPassToken(String user, CharSequence password) {
    this(user, password.toString().getBytes(Charset.forName("UTF-8")));
  }
  
  public void destroy() {
    Arrays.fill(password, (byte) 0);
    password = null;
  }
  
  @Override
  public boolean isDestroyed() {
    return password == null;
  }
  
  /**
   * @deprecated since 1.5
   * @param credentials
   * @return
   */
  public static UserPassToken convertAuthInfo(AuthInfo credentials) {
    return new UserPassToken(credentials.user, credentials.password);
  }
  
  @Override
  public String getPrincipal() {
    return username;
  }
  
  public byte[] getPassword() {
    return password;
  }
  
  @Override
  public void updatePassword(byte[] newPassword) {
    this.password = Arrays.copyOf(newPassword, newPassword.length);
  }
  
  @Override
  public void updatePassword(PasswordUpdatable pu) {
    updatePassword(pu.getPassword());
  }
  
  public String toString() {
    return "UserPassToken("+this.username+":"+new String(this.getPrincipal())+")";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(password);
    result = prime * result + ((username == null) ? 0 : username.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof UserPassToken))
      return false;
    UserPassToken other = (UserPassToken) obj;
    if (!Arrays.equals(password, other.password))
      return false;
    if (username == null) {
      if (other.username != null)
        return false;
    } else if (!username.equals(other.username))
      return false;
    return true;
  }

  @Override
  public SecuritySerDe<? extends SecurityToken> getSerDe() {
    return new UserPassSerDe();
  }
  
}

package org.apache.accumulo.core.security.tokens;

public interface PasswordUpdatable {
  public void updatePassword(PasswordUpdatable newToken);
  public void updatePassword(byte[] newPassword);
  public byte[] getPassword();
}

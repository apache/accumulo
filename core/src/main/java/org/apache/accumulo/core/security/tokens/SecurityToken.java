package org.apache.accumulo.core.security.tokens;

import javax.security.auth.Destroyable;

/**
 * Any SecurityTokens created need to have an empty constructor as well
 */
public interface SecurityToken extends Destroyable {
  public String getPrincipal();
  public SecuritySerDe<? extends SecurityToken> getSerDe();
}

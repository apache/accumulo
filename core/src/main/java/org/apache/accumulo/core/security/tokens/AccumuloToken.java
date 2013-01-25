package org.apache.accumulo.core.security.tokens;

import javax.security.auth.Destroyable;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

/**
 * Any AccumuloTokens created need to have an empty constructor as well
 */
public interface AccumuloToken<T extends TBase<?,?>, F extends TFieldIdEnum> extends TBase<T, F>, Destroyable {
  public String getPrincipal();
  public boolean equals(AccumuloToken<?,?> token);
}

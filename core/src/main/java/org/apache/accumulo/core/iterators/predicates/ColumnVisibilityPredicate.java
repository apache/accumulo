package org.apache.accumulo.core.iterators.predicates;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Predicate;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

public final class ColumnVisibilityPredicate implements Predicate<Key,Value> {
  
  private final Authorizations auths;
  
  public Authorizations getAuthorizations() {
    return auths;
  }
  
  public ColumnVisibilityPredicate(Authorizations auths) {
    this.auths = auths;
  }
  
  @Override
  public boolean evaluate(Key k, Value v) {
    return new ColumnVisibility(k.getColumnVisibility()).evaluate(auths);
  }
  
  @Override
  public String toString() {
    return "{" + auths + "}";
  }
}

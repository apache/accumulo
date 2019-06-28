package org.apache.accumulo.server.security.handler;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

public class PermImpl implements Perm {

  @Override
  public boolean hasSystem(String user, SystemPermission perm) {
    return false;
  }

  @Override
  public boolean hasTable(String user, String table, TablePermission perm)
      throws TableNotFoundException {
    return false;
  }

  @Override
  public boolean hasNamespace(String user, String namespace, NamespacePermission perm)
      throws NamespaceNotFoundException {
    return false;
  }

  @Override
  public void grantSystem(String user, SystemPermission perm) throws AccumuloSecurityException {

  }

  @Override
  public void revokeSystem(String user, SystemPermission perm) throws AccumuloSecurityException {

  }

  @Override
  public void grantTable(String user, String table, TablePermission perm)
      throws AccumuloSecurityException, TableNotFoundException {

  }

  @Override
  public void revokeTable(String user, String table, TablePermission perm)
      throws AccumuloSecurityException, TableNotFoundException {

  }

  @Override
  public void grantNamespace(String user, String namespace, NamespacePermission perm)
      throws AccumuloSecurityException, NamespaceNotFoundException {

  }

  @Override
  public void revokeNamespace(String user, String namespace, NamespacePermission perm)
      throws AccumuloSecurityException, NamespaceNotFoundException {

  }
}

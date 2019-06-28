package org.apache.accumulo.server.security.handler;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

/**
 * Pluggable permissions module returned by {@link SecurityModule#perm()} ()}.
 */
public interface Perm {
  // TODO lot of duplication here with org.apache.accumulo.core.client.admin.SecurityOpertaions need
  // to figure out if we have to have both

  boolean hasSystem(String user, SystemPermission perm);

  boolean hasTable(String user, String table, TablePermission perm) throws TableNotFoundException;

  boolean hasNamespace(String user, String namespace, NamespacePermission perm)
      throws NamespaceNotFoundException;

  void grantSystem(String user, SystemPermission perm) throws AccumuloSecurityException;

  void revokeSystem(String user, SystemPermission perm) throws AccumuloSecurityException;

  void grantTable(String user, String table, TablePermission perm)
      throws AccumuloSecurityException, TableNotFoundException;

  void revokeTable(String user, String table, TablePermission perm)
      throws AccumuloSecurityException, TableNotFoundException;

  void grantNamespace(String user, String namespace, NamespacePermission perm)
      throws AccumuloSecurityException, NamespaceNotFoundException;

  void revokeNamespace(String user, String namespace, NamespacePermission perm)
      throws AccumuloSecurityException, NamespaceNotFoundException;

}

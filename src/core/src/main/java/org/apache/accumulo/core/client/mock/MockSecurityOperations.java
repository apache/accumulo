package org.apache.accumulo.core.client.mock;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;


public class MockSecurityOperations implements SecurityOperations {
    
    final private MockAccumulo acu;

    MockSecurityOperations(MockAccumulo acu) {
        this.acu = acu;
    }
    
    @Override
    public void createUser(String user, byte[] password, Authorizations authorizations)
            throws AccumuloException, AccumuloSecurityException {
        this.acu.users.put(user, new MockUser(user, password, authorizations));
    }

    @Override
    public void dropUser(String user) throws AccumuloException, AccumuloSecurityException {
        this.acu.users.remove(user);
    }

    @Override
    public boolean authenticateUser(String name, byte[] password)
            throws AccumuloException, AccumuloSecurityException {
        MockUser user = acu.users.get(name);
        if (user == null)
            return false;
        return Arrays.equals(user.password, password);
    }

    @Override
    public void changeUserPassword(String name, byte[] password)
            throws AccumuloException, AccumuloSecurityException {
        MockUser user = acu.users.get(name);
        if (user != null)
            user.password = Arrays.copyOf(password, password.length);
    }

    @Override
    public void changeUserAuthorizations(String name, Authorizations authorizations)
            throws AccumuloException, AccumuloSecurityException {
        MockUser user = acu.users.get(name);
        if (user != null)
            user.authorizations = authorizations;
    }

    @Override
    public Authorizations getUserAuthorizations(String name)
            throws AccumuloException, AccumuloSecurityException {
        MockUser user = acu.users.get(name);
        if (user != null)
            return user.authorizations;
        return new Authorizations();
    }

    @Override
    public boolean hasSystemPermission(String name, SystemPermission perm)
            throws AccumuloException, AccumuloSecurityException {
        MockUser user = acu.users.get(name);
        if (user != null)
            return user.permissions.contains(perm);
        return false;
    }

    @Override
    public boolean hasTablePermission(String name, String tableName, TablePermission perm)
            throws AccumuloException, AccumuloSecurityException {
        MockTable table = acu.tables.get(tableName);
        if (table == null)
            return false;
        EnumSet<TablePermission> perms = table.userPermissions.get(name);
        if (perms == null)
            return false;
        return perms.contains(perm);
    }

    @Override
    public void grantSystemPermission(String name, SystemPermission permission)
            throws AccumuloException, AccumuloSecurityException {
        MockUser user = acu.users.get(name);
        if (user != null)
            user.permissions.add(permission);
    }

    @Override
    public void grantTablePermission(String name, String tableName, TablePermission permission)
            throws AccumuloException, AccumuloSecurityException {
        if (acu.users.get(name) == null)
            return;
        MockTable table = acu.tables.get(tableName);
        if (table == null)
            return;
        EnumSet<TablePermission> perms = table.userPermissions.get(name);
        if (perms == null)
            table.userPermissions.put(name, EnumSet.of(permission));
        else
            perms.add(permission);
    }

    @Override
    public void revokeSystemPermission(String name, SystemPermission permission)
            throws AccumuloException, AccumuloSecurityException {
        MockUser user = acu.users.get(name);
        if (user != null)
            user.permissions.remove(permission);
    }

    @Override
    public void revokeTablePermission(String name, String tableName, TablePermission permission)
            throws AccumuloException, AccumuloSecurityException {
        if (acu.users.get(name) == null)
            return;
        MockTable table = acu.tables.get(tableName);
        if (table == null)
            return;
        EnumSet<TablePermission> perms = table.userPermissions.get(name);
        if (perms != null)
            perms.remove(permission);
        
    }

    @Override
    public Set<String> listUsers() throws AccumuloException, AccumuloSecurityException {
        return acu.users.keySet();
    }

}

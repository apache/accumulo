package org.apache.accumulo.server.security;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;


public interface Authenticator
{
	public void initializeSecurity(AuthInfo credentials, String rootuser, byte[] rootpass) throws AccumuloSecurityException;
	public String getRootUsername();
	
	public boolean authenticateUser(AuthInfo credentials, String user, ByteBuffer pass) throws AccumuloSecurityException;

	public Set<String> listUsers(AuthInfo credentials) throws AccumuloSecurityException;

	public void createUser(AuthInfo credentials, String user, byte[] pass, Authorizations authorizations) throws AccumuloSecurityException;
	public void dropUser(AuthInfo credentials, String user) throws AccumuloSecurityException;
	public void changePassword(AuthInfo credentials, String user, byte[] pass) throws AccumuloSecurityException;
	public void changeAuthorizations(AuthInfo credentials, String user, Authorizations authorizations) throws AccumuloSecurityException;
    public Authorizations getUserAuthorizations(AuthInfo credentials, String user) throws AccumuloSecurityException;

    public boolean hasSystemPermission(AuthInfo credentials, String user, SystemPermission permission) throws AccumuloSecurityException;
    public boolean hasTablePermission(AuthInfo credentials, String user, String table, TablePermission permission) throws AccumuloSecurityException;

    public void grantSystemPermission(AuthInfo credentials, String user, SystemPermission permission) throws AccumuloSecurityException;
    public void revokeSystemPermission(AuthInfo credentials, String user, SystemPermission permission) throws AccumuloSecurityException;
    public void grantTablePermission(AuthInfo credentials, String user, String table, TablePermission permission) throws AccumuloSecurityException;
    public void revokeTablePermission(AuthInfo credentials, String user, String table, TablePermission permission) throws AccumuloSecurityException;
	public void deleteTable(AuthInfo credentials, String table) throws AccumuloSecurityException;
}

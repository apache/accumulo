package org.apache.accumulo.server.security;

import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.ByteArraySet;
import org.apache.accumulo.server.security.ZKAuthenticator;

import junit.framework.TestCase;

public class ZKAuthenticatorTest extends TestCase
{
	public void testPermissionIdConversions()
	{
		for (SystemPermission s : SystemPermission.values())
			assertTrue(s.equals(SystemPermission.getPermissionById(s.getId())));
		
		for (TablePermission s : TablePermission.values())
			assertTrue(s.equals(TablePermission.getPermissionById(s.getId())));
	}
	
	public void testAuthorizationConversion()
	{
		ByteArraySet auths = new ByteArraySet();
		for (int i = 0; i < 300; i+=3)
			auths.add(Integer.toString(i).getBytes());
		
		Authorizations converted = new Authorizations(auths);
		byte[] test = ZKAuthenticator.Tool.convertAuthorizations(converted);
		Authorizations test2 = ZKAuthenticator.Tool.convertAuthorizations(test);
		assertTrue(auths.size() == test2.size());
		for (byte[] s : auths) {
			assertTrue(test2.contains(s));
		}
	}
	
	public void testSystemConversion()
	{
		Set<SystemPermission> perms = new TreeSet<SystemPermission>();
		for (SystemPermission s : SystemPermission.values())
			perms.add(s);
		
		Set<SystemPermission> converted = ZKAuthenticator.Tool.convertSystemPermissions(ZKAuthenticator.Tool.convertSystemPermissions(perms));
		assertTrue(perms.size() == converted.size());
		for (SystemPermission s : perms)
			assertTrue(converted.contains(s));
	}
	
	public void testTableConversion()
	{
		Set<TablePermission> perms = new TreeSet<TablePermission>();
		for (TablePermission s : TablePermission.values())
			perms.add(s);
		
		Set<TablePermission> converted = ZKAuthenticator.Tool.convertTablePermissions(ZKAuthenticator.Tool.convertTablePermissions(perms));
		assertTrue(perms.size() == converted.size());
		for (TablePermission s : perms)
			assertTrue(converted.contains(s));
	}
	
	public void testEncryption()
	{
		byte[] rawPass = "myPassword".getBytes();
		byte[] storedBytes;
		try {
			storedBytes = ZKAuthenticator.Tool.createPass(rawPass);
			assertTrue(ZKAuthenticator.Tool.checkPass(rawPass, storedBytes));
		} catch (AccumuloException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}
}

package org.apache.accumulo.server.test.randomwalk.security;

import java.util.Arrays;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public class Authenticate extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		Connector conn = SecurityHelper.getSystemConnector(state);

		authenticate(conn, state, props);
	}

	public static void authenticate(Connector conn, State state, Properties props) throws Exception {
		String targetProp = props.getProperty("target");
		boolean success = Boolean.parseBoolean(props.getProperty("valid"));
		
		String target;
		boolean exists = true;
		boolean hasPermission = true;
		byte[] password;
		if (targetProp.equals("table"))
		{
			exists = SecurityHelper.getTabUserExists(state); 
			target = SecurityHelper.getTabUserName(state);
			if (!conn.whoami().equals(state.getConnector().whoami()) && !SecurityHelper.getSysPerm(state, SecurityHelper.getSysUserName(state), SystemPermission.SYSTEM))
				hasPermission = false;
			password = Arrays.copyOf(SecurityHelper.getTabUserPass(state), SecurityHelper.getTabUserPass(state).length);
		}
		else
		{
			target = SecurityHelper.getSysUserName(state);
			password = Arrays.copyOf(SecurityHelper.getSysUserPass(state), SecurityHelper.getSysUserPass(state).length);
		}

		if (!success)
			for (int i = 0; i < password.length; i++)
				password[i]++;
		
		boolean result;
		
		try
		{
			result = conn.securityOperations().authenticateUser(target, password);
		} catch (AccumuloSecurityException ae)
		{
			switch (ae.getErrorCode())
			{
			case PERMISSION_DENIED:
				if (exists && hasPermission)
					throw new AccumuloException("Got a security exception when I should have had permission.",ae);
				else
					return;
			default:
				throw new AccumuloException("Unexpected exception!", ae);
			}
		}
		if (!hasPermission)
			throw new AccumuloException("Didn't get Security Exception when we should have");
		if (result != (success && exists))
			throw new AccumuloException("Got " + result + " as the result when it should be " + success);
	}
}

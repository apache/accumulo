package org.apache.accumulo.server.test.randomwalk.security;

import java.math.BigInteger;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public class ChangePass extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		Connector conn;

		String target = props.getProperty("target");
		String source = props.getProperty("source");

		String sourceUser;
		if (source.equals("system"))
		{
			conn = SecurityHelper.getSystemConnector(state);
			sourceUser = SecurityHelper.getSysUserName(state);
		} else
		{
			sourceUser = SecurityHelper.getTabUserName(state);
			try {
				conn = state.getInstance().getConnector(sourceUser, (SecurityHelper.getTabUserPass(state)));
			} catch (AccumuloSecurityException ae)
			{
				if (ae.getErrorCode().equals(SecurityErrorCode.BAD_CREDENTIALS))
				{
					if (SecurityHelper.getTabUserExists(state))
						throw new AccumuloException("Got a security exception when the user should have existed", ae);
					else
						return;
				}
				throw new AccumuloException("Unexpected exception!", ae);
			}
		}

		boolean hasPerm = true;
		if (!source.equals(target))
			hasPerm = SecurityHelper.getSysPerm(state, sourceUser, SystemPermission.ALTER_USER);

		boolean targetExists = true;
		boolean targetSystem = true;
		if (target.equals("table"))
		{
			targetSystem = false;
			if (!SecurityHelper.getTabUserExists(state))
				targetExists = false;
			target = SecurityHelper.getTabUserName(state);
		} else
			target = SecurityHelper.getSysUserName(state);

		Random r = new Random();

		byte[] newPass = new byte[r.nextInt(50)+1];
		r.nextBytes(newPass);
		BigInteger bi = new BigInteger(newPass);
		newPass = bi.toString(36).getBytes();

		try
		{
			conn.securityOperations().changeUserPassword(target, newPass);
		} catch (AccumuloSecurityException ae)
		{
			switch (ae.getErrorCode())
			{
			case PERMISSION_DENIED:
				if (hasPerm)
					throw new AccumuloException("Change failed when it should have succeeded to change " + target + "'s password",ae);
				return;
			case USER_DOESNT_EXIST:
				if (targetExists)
					throw new AccumuloException("User "+target+" doesn't exist and they SHOULD.", ae);
				return;
			default:
				throw new AccumuloException("Got unexpected exception",ae);
			}
		}
		if (targetSystem)
		{
			SecurityHelper.setSysUserPass(state, newPass);
		}
		else
			SecurityHelper.setTabUserPass(state, newPass);
		if (!hasPerm)
			throw new AccumuloException("Password change succeeded when it should have failed.");
	}
}

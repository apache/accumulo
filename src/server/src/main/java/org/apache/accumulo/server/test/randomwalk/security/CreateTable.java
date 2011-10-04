package org.apache.accumulo.server.test.randomwalk.security;

import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;


public class CreateTable extends Test {

	@Override
	public void visit(State state, Properties props) throws Exception {
		Connector conn = SecurityHelper.getSystemConnector(state);

		String tableName = SecurityHelper.getTableName(state);

		boolean exists = SecurityHelper.getTableExists(state);
		boolean hasPermission = false;
		if (SecurityHelper.getSysPerm(state, SecurityHelper.getSysUserName(state), SystemPermission.CREATE_TABLE))
			hasPermission = true;

		try
		{
			conn.tableOperations().create(tableName);
		} catch (AccumuloSecurityException ae)
		{
			if (ae.getErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED))
			{
				if (hasPermission)
					throw new AccumuloException("Got a security exception when I should have had permission.",ae);
				else
					// create table anyway for sake of state
				{
					try {
						state.getConnector().tableOperations().create(tableName);
						SecurityHelper.setTableExists(state, true);
					} catch (TableExistsException tee)
					{
						if (exists)
							return;
						else throw new AccumuloException("Test and Accumulo are out of sync");
					}
					return;
				}
			} else
				throw new AccumuloException("Got unexpected error", ae);
		} catch (TableExistsException tee)
		{
			if (!exists)
				throw new TableExistsException(null, tableName, "Got a TableExistsException but it shouldn't have existed", tee);
			else
				return;
		}
		SecurityHelper.setTableExists(state, true);
		for (TablePermission tp : TablePermission.values())
			SecurityHelper.setTabPerm(state, conn.whoami(), tp, true);
		if (!hasPermission)
			throw new AccumuloException("Didn't get Security Exception when we should have");
	}
}

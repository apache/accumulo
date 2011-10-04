package org.apache.accumulo.server.tabletserver.mastermessage;

import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.thrift.TException;


public interface MasterMessage {

	void send(AuthInfo info, String serverName, MasterClientService.Iface client) throws TException, ThriftSecurityException;

}

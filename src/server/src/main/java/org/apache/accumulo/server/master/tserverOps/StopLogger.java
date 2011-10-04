package org.apache.accumulo.server.master.tserverOps;

import java.net.InetSocketAddress;

import org.apache.accumulo.core.tabletserver.thrift.MutationLogger;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fate.Repo;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.master.tableOps.MasterRepo;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.log4j.Logger;


public class StopLogger extends MasterRepo {

    private static final long serialVersionUID = 1L;
    private static final Logger log = Logger.getLogger(StopLogger.class);
    private String logger;

    public StopLogger(String logger) {
        this.logger = logger;
    }

	@Override
	public long isReady(long tid, Master environment) throws Exception {
		return 0;
	}
    
    @Override
    public Repo<Master> call(long tid, Master m) throws Exception {
        InetSocketAddress addr = AddressUtil.parseAddress(logger, 0);
        MutationLogger.Iface client = ThriftUtil.getClient(new MutationLogger.Client.Factory(), addr, ServerConfiguration.getSystemConfiguration());
        try {
            client.halt(null, SecurityConstants.getSystemCredentials());
            log.info("logger asked to halt " + logger);
        } catch (Exception ex) {
            log.error("Unable to talk to logger at " + addr);
        } finally {
            ThriftUtil.returnClient(client);
        }
        return null;
    }

    @Override
    public void undo(long tid, Master m) throws Exception {
    }

}

package org.apache.accumulo.manager;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.lock.ServiceLockSupport;
import org.apache.accumulo.manager.fate.FateWorker;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

public class ManagerWorker extends AbstractServer {

    private static final Logger LOG = LoggerFactory.getLogger(ManagerWorker.class);
    private ServiceLock scanServerLock;

    protected ManagerWorker(ConfigOpts opts, String[] args) {
        super(ServerId.Type.MANAGER_WORKER, opts, ServerContext::new, args);
    }

    protected void startScanServerClientService() throws UnknownHostException {

        var fateWorker = new FateWorker(getContext());

        // This class implements TabletClientService.Iface and then delegates calls. Be sure
        // to set up the ThriftProcessor using this class, not the delegate.
        ClientServiceHandler clientHandler = new ClientServiceHandler(getContext());
        TProcessor processor =
                ThriftProcessorTypes.getManagerWorkerTProcessor(this, clientHandler, fateWorker, getContext());

        // TODO using scan server props
        updateThriftServer(() -> {
            return TServerUtils.createThriftServer(getContext(), getBindAddress(),
                    Property.SSERV_CLIENTPORT, processor, this.getClass().getSimpleName(),
                    Property.SSERV_PORTSEARCH, Property.SSERV_MINTHREADS, Property.SSERV_MINTHREADS_TIMEOUT,
                    Property.SSERV_THREADCHECK);
        }, true);
    }

    private ServiceLock announceExistence() {
        final ZooReaderWriter zoo = getContext().getZooSession().asReaderWriter();
        try {

            final ServiceLockPaths.ServiceLockPath zLockPath =
                    getContext().getServerPaths().createScanServerPath(getResourceGroup(), getAdvertiseAddress());
            ServiceLockSupport.createNonHaServiceLockPath(ServerId.Type.MANAGER_WORKER, zoo, zLockPath);
            var serverLockUUID = UUID.randomUUID();
            scanServerLock = new ServiceLock(getContext().getZooSession(), zLockPath, serverLockUUID);
            ServiceLock.LockWatcher lw = new ServiceLockSupport.ServiceLockWatcher(ServerId.Type.MANAGER_WORKER, () -> getShutdownComplete().get(),
                    (type) -> getContext().getLowMemoryDetector().logGCInfo(getConfiguration()));

            for (int i = 0; i < 120 / 5; i++) {
                zoo.putPersistentData(zLockPath.toString(), new byte[0], ZooUtil.NodeExistsPolicy.SKIP);

                ServiceLockData.ServiceDescriptors descriptors = new ServiceLockData.ServiceDescriptors();
                for (ServiceLockData.ThriftService svc : new ServiceLockData.ThriftService[] {ServiceLockData.ThriftService.CLIENT,
                        ServiceLockData.ThriftService.FATE_WORKER}) {
                    descriptors.addService(new ServiceLockData.ServiceDescriptor(serverLockUUID, svc,
                            getAdvertiseAddress().toString(), this.getResourceGroup()));
                }

                if (scanServerLock.tryLock(lw, new ServiceLockData(descriptors))) {
                    LOG.debug("Obtained scan server lock {}", scanServerLock.getLockPath());
                    return scanServerLock;
                }
                LOG.info("Waiting for scan server lock");
                sleepUninterruptibly(5, TimeUnit.SECONDS);
            }
            String msg = "Too many retries, exiting.";
            LOG.info(msg);
            throw new RuntimeException(msg);
        } catch (Exception e) {
            LOG.info("Could not obtain scan server lock, exiting.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ServiceLock getLock() {
        return scanServerLock;
    }

    @Override
    public void run() {
        try {
            waitForUpgrade();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for upgrade to complete, exiting...");
            System.exit(1);
        }

        SecurityUtil.serverLogin(getConfiguration());

        // TODO metrics

        try {
            startScanServerClientService();
        } catch (UnknownHostException e1) {
            throw new RuntimeException("Failed to start the scan server client service", e1);
        }

        ServiceLock lock = announceExistence();
        this.getContext().setServiceLock(lock);

        while (!isShutdownRequested()) {
            if (Thread.currentThread().isInterrupted()) {
                LOG.info("Server process thread has been interrupted, shutting down");
                break;
            }
            try {
                Thread.sleep(1000);
                // TODO update idle status
            } catch (InterruptedException e) {
                LOG.info("Interrupt Exception received, shutting down");
                gracefulShutdown(getContext().rpcCreds());
            }
        }

        LOG.debug("Stopping Thrift Servers");
        getThriftServer().stop();
    }
}

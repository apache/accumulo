package cloudtrace.instrument.receivers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

/**
 * Find a Span collector via zookeeper and push spans there via Thrift RPC
 *
 */
public class ZooSpanClient extends SendSpansViaThrift implements Watcher {
    
    private static final Logger log = Logger.getLogger(ZooSpanClient.class);
    private static final int TOTAL_TIME_WAIT_CONNECT_MS = 10 * 1000;
    private static final int TIME_WAIT_CONNECT_CHECK_MS = 100;
    
    final ZooKeeper zoo;
    final String path;
    final Random random = new Random();
    final List<String> hosts = new ArrayList<String>();
    
    public ZooSpanClient(String keepers, String path, String host, String service, long millis) throws IOException, KeeperException, InterruptedException {
        super(host, service, millis);
        this.path = path;
        zoo = new ZooKeeper(keepers, 30*1000, this);
        for (int i = 0; i < TOTAL_TIME_WAIT_CONNECT_MS; i += TIME_WAIT_CONNECT_CHECK_MS) {
            if (zoo.getState().equals(States.CONNECTED))
                break;
            try {
                Thread.sleep(TIME_WAIT_CONNECT_CHECK_MS);
            } catch (InterruptedException ex) {
                break;
            }
        }
        zoo.getChildren(path, true);
    }
    
    synchronized private void updateHosts(String path, List<String> children) {
        log.debug("Scanning trace hosts in zookeeper: " + path);
        try {
            List<String> hosts = new ArrayList<String>();
            for (String child : children) {
                byte[] data = zoo.getData(path + "/" + child, null, null);
                hosts.add(new String(data));
            }
            this.hosts.clear();
            this.hosts.addAll(hosts);
            log.debug("Trace hosts: " + this.hosts);
        } catch (Exception ex) {
            log.error("unable to get destination hosts in zookeeper", ex);
        }
    }

    @Override
    synchronized protected String getSpanKey(Map<String, String> data) {
        if (hosts.size() > 0) {
            String host = hosts.get(random.nextInt(hosts.size()));
            log.debug("sending data to " + host);
            return host;
        }
        return null;
    }


    @Override
    public void process(WatchedEvent event) {
        try {
            updateHosts(path, zoo.getChildren(path, null));
        } catch (Exception ex) {
            log.error("unable to get destination hosts in zookeeper", ex);
        }
    }
}

package org.apache.accumulo.core.trace;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.zookeeper.ZooReader;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.zookeeper.KeeperException;

import cloudtrace.instrument.Tracer;

public class DistributedTrace {
    public static void enable(Instance instance, ZooReader zoo, String application, String address) throws IOException, KeeperException, InterruptedException {
        String path = ZooUtil.getRoot(instance) + Constants.ZTRACERS;
        if (address == null) {
            try {
                address = InetAddress.getLocalHost().getHostAddress().toString();
            } catch (UnknownHostException e) {
                address = "unknown";
            }
        }
        Tracer.getInstance().addReceiver(new ZooTraceClient(zoo, path, address, application, 1000));
    }
}

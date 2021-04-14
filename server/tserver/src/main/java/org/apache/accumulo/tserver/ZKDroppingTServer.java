/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A fully functioning tabletserver that will look for a ZK path to signal when to start dropping
 * ZK connections. When the signal node is seen, the BadZooReaderWriter will be used that will
 * always throw errors anytime ZK is accessed from ServerContext. This is an
 * attempt to simulate a tserver having problems communication with ZK but manager doesn't.
 *
 * Class lives in tserver package to access package private tserver constructor.
 */
public class ZKDroppingTServer extends TabletServer {
    public static final String ZPATH_START_DROPPING = "/test/startDropping";
    private static final Logger log = LoggerFactory.getLogger(ZKDroppingTServer.class);
    ServerContext testContext;

    public static void main(String[] args) throws Exception {
        try (TabletServer tserver = new ZKDroppingTServer(new ServerOpts(), args)) {
            tserver.runServer();
        }
    }

    public ZKDroppingTServer(ServerOpts opts, String[] args) {
        super(opts, args);
        log.info("Constructed ZKDroppingTServer");
    }

    @Override
    public ServerContext getContext() {
        if (testContext == null)
            this.testContext = new TestServerContext(SiteConfiguration.auto());
        return this.testContext;
    }
}

class TestServerContext extends ServerContext {
    static Logger log = LoggerFactory.getLogger(ZKDroppingTServer.class);
    BadZooReaderWriter badZooReaderWriter;
    boolean getBad = false;

    public TestServerContext(SiteConfiguration siteConfig) {
        super(siteConfig);
        this.badZooReaderWriter = new BadZooReaderWriter(siteConfig);
        setupCrypto();
        try {
            // set watcher for signal in ZK to start dropping
            super.getZooReaderWriter().exists(ZKDroppingTServer.ZPATH_START_DROPPING, event -> {
                log.info("Start dropping ZK signal node created");
                getBad = true;
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ZooReaderWriter getZooReaderWriter() {
        if (getBad) {
            log.debug("Return BadZooReaderWriter");
            return this.badZooReaderWriter;
        } else {
            return super.getZooReaderWriter();
        }
    }
}

class BadZooReaderWriter extends ZooReaderWriter {
    static Logger log = LoggerFactory.getLogger(BadZooReaderWriter.class);

    public BadZooReaderWriter(AccumuloConfiguration conf) {
        super(conf);
    }

    private void simulateError() throws KeeperException {
        KeeperException e = new KeeperException.ConnectionLossException();
        log.warn("Saw (possibly) transient exception communicating with ZooKeeper", e);
        throw e;
    }

    @Override
    public void sync(String path) throws KeeperException, InterruptedException {
        simulateError();
        super.sync(path);
    }

    @Override
    public boolean putPersistentData(String zPath, byte[] data, ZooUtil.NodeExistsPolicy policy) throws KeeperException, InterruptedException {
        simulateError();
        return super.putPersistentData(zPath, data, policy);
    }

    @Override
    public boolean exists(String zPath) throws KeeperException, InterruptedException {
        simulateError();
        return super.exists(zPath);
    }

    @Override
    public boolean exists(String zPath, Watcher watcher) throws KeeperException, InterruptedException {
        simulateError();
        return super.exists(zPath, watcher);
    }

    @Override
    public byte[] getData(String zPath) throws KeeperException, InterruptedException {
        simulateError();
        return super.getData(zPath);
    }

    @Override
    public byte[] getData(String zPath, Stat stat) throws KeeperException, InterruptedException {
        simulateError();
        return super.getData(zPath, stat);
    }

    @Override
    public byte[] getData(String zPath, Watcher watcher) throws KeeperException, InterruptedException {
        simulateError();
        return super.getData(zPath, watcher);
    }

    @Override
    public Stat getStatus(String zPath) throws KeeperException, InterruptedException {
        simulateError();
        return super.getStatus(zPath);
    }

    @Override
    public Stat getStatus(String zPath, Watcher watcher) throws KeeperException, InterruptedException {
        simulateError();
        return super.getStatus(zPath, watcher);
    }

    @Override
    public List<String> getChildren(String zPath) throws KeeperException, InterruptedException {
        simulateError();
        return super.getChildren(zPath);
    }

    @Override
    public List<String> getChildren(String zPath, Watcher watcher) throws KeeperException, InterruptedException {
        simulateError();
        return super.getChildren(zPath, watcher);
    }

    @Override
    public List<ACL> getACL(String zPath) throws KeeperException, InterruptedException {
        simulateError();
        return super.getACL(zPath);
    }

    @Override
    public boolean putPrivatePersistentData(String zPath, byte[] data, ZooUtil.NodeExistsPolicy policy) throws KeeperException, InterruptedException {
        simulateError();
        return super.putPrivatePersistentData(zPath, data, policy);
    }

    @Override
    public boolean putPersistentData(String zPath, byte[] data, ZooUtil.NodeExistsPolicy policy, List<ACL> acls) throws KeeperException, InterruptedException {
        simulateError();
        return super.putPersistentData(zPath, data, policy, acls);
    }

    @Override
    public String putPersistentSequential(String zPath, byte[] data) throws KeeperException, InterruptedException {
        simulateError();
        return super.putPersistentSequential(zPath, data);
    }

    @Override
    public void putEphemeralData(String zPath, byte[] data) throws KeeperException, InterruptedException {
        simulateError();
        super.putEphemeralData(zPath, data);
    }

    @Override
    public String putEphemeralSequential(String zPath, byte[] data) throws KeeperException, InterruptedException {
        simulateError();
        return super.putEphemeralSequential(zPath, data);
    }

    @Override
    public void recursiveCopyPersistentOverwrite(String source, String destination) throws KeeperException, InterruptedException {
        simulateError();
        super.recursiveCopyPersistentOverwrite(source, destination);
    }

    @Override
    public byte[] mutateExisting(String zPath, Mutator mutator) throws KeeperException, InterruptedException, AcceptableThriftTableOperationException {
        simulateError();
        return super.mutateExisting(zPath, mutator);
    }

    @Override
    public byte[] mutateOrCreate(String zPath, byte[] createValue, Mutator mutator) throws KeeperException, InterruptedException, AcceptableThriftTableOperationException {
        simulateError();
        return super.mutateOrCreate(zPath, createValue, mutator);
    }

    @Override
    public void mkdirs(String path) throws KeeperException, InterruptedException {
        simulateError();
        super.mkdirs(path);
    }

    @Override
    public void delete(String path) throws KeeperException, InterruptedException {
        simulateError();
        super.delete(path);
    }

    @Override
    public void recursiveDelete(String zPath, ZooUtil.NodeMissingPolicy policy) throws KeeperException, InterruptedException {
        simulateError();
        super.recursiveDelete(zPath, policy);
    }
}
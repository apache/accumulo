/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.management.*;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.metrics.MetricsSystemHelper;
import org.apache.accumulo.server.zookeeper.ZooReaderWriterFactory;
import org.apache.accumulo.test.functional.util.FateUtilBase;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IT Tests that create / run a "slow" FATE transaction to test FATE metrics, based on
 * FateConcurrentIT.
 * <p>
 * TODO - this may be combined with FATE Concurrency tests - or common methods refactored out.
 */
public class FateMetricsIT extends FateUtilBase {

    private static final Logger log = LoggerFactory.getLogger(FateMetricsIT.class);


    /**
     * Validate the the AdminUtil.getStatus works correctly after refactor and validate that
     * getTransactionStatus can be called without lock map(s). The test starts a long running fate
     * transaction (slow compaction) and the calls AdminUtil functions to get the FATE.
     *
     * @throws Exception any exception is a test failure
     */
    @Test
    public void getFateMetrics() throws Exception {

        try {
            log.info("*** Starting sleep 1");
            Thread.sleep(500);
        }catch(InterruptedException ex){
            Thread.currentThread().interrupt();
            return;
        }       // TODO
        //
        // connect to jxm server - like this?

        MetricsSystem metricsSystem = MetricsSystemHelper.getInstance();

        log.info("MS: {}", metricsSystem.currentConfig());

        // ms.startMetricsMBeans();


        // find values by attribute name?
        // server.getMBeanInfo();
        // server.getAttributes();

        // initFates = jmx.get...
        // initZxid = jmx.get...


        assertEquals("verify table online after created", TableState.ONLINE, getTableState(tableName));

        Future<?> compactTask = startCompactTask();

        assertTrue("compaction fate transaction exits", findFate(tableName));

        Instance instance = connector.getInstance();
        AdminUtil<String> admin = new AdminUtil<>(false);

        try {

            String tableId = Tables.getTableId(instance, tableName);

            log.trace("tid: {}", tableId);

            IZooReaderWriter zk = new ZooReaderWriterFactory().getZooReaderWriter(
                    instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut(), secret);
            ZooStore<String> zs = new ZooStore<>(ZooUtil.getRoot(instance) + Constants.ZFATE, zk);

            AdminUtil.FateStatus withLocks = admin.getStatus(zs, zk,
                    ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS + "/" + tableId, null, null);

            // call method that does not use locks.
            List<AdminUtil.TransactionStatus> noLocks = admin.getTransactionStatus(zs, null, null);

            for(int i = 0; i < 10; i++) {

                debugMetrics(metricsSystem);

                try {
                    log.info("*** Starting sleep {}", i);

                    Thread.sleep(30_000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            // TODO
            // call jmx and get # fate operations;
            // long #fate = jmx.get()...
            long numFate = 0;

            // fast check - count number of transactions
            assertEquals(numFate, noLocks.size());

        } catch (KeeperException | TableNotFoundException | InterruptedException ex) {
            throw new IllegalStateException(ex);
        }

        // test complete, cancel compaction and move on.
        connector.tableOperations().cancelCompaction(tableName);

        // block if compaction still running
        compactTask.get();

    }

    private void debugMetrics(MetricsSystem metricsSystem) {

        try {

            metricsSystem.publishMetricsNow();

            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

            Set<ObjectName> mbeans = mBeanServer.queryNames(null, null);

            for (ObjectName name : mbeans) {


                MBeanInfo info = mBeanServer.getMBeanInfo(name);
                MBeanAttributeInfo[] attrInfo = info.getAttributes();

                if (name.getDomain().startsWith("Hadoop")) {
                    log.error("HHHH {}", name);

                    log.info("Attributes for object: {}", name);
                    for (MBeanAttributeInfo attr : attrInfo) {
                        log.info("  {}", attr);
                    }
                } else {
                    log.info("A:{}", name);
                }
            }
        } catch (InstanceNotFoundException | IntrospectionException | ReflectionException ex) {
            log.error("Could not retrieve metrics info", ex);
        }
    }
}
